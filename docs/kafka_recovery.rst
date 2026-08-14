Kafka consumer recovery
=======================

HAMSTRING's pipeline workers use Kafka transactions to commit their output
records and their consumed input offsets atomically. This prevents a committed
Kafka output from being separated from the input offset that produced it.

Consumer membership and transactions
------------------------------------

A Kafka consumer owns an assignment of topic partitions only while it remains
a valid member of its consumer group. Kafka can change that assignment when a
worker starts or stops, a service is scaled, a broker or coordinator restarts,
or a worker exceeds ``environment.kafka_consumer.max_poll_interval_ms``.

If a worker finishes an old batch after losing membership, Kafka rejects
``send_offsets_to_transaction`` with an error such as
``UNKNOWN_MEMBER_ID``, ``ILLEGAL_GENERATION``, or
``REBALANCE_IN_PROGRESS``. The transaction must then be aborted. Delivery
callbacks can report ``_PURGE_QUEUE`` while records from that aborted
transaction are discarded; this is a consequence of the abort, not Kafka
topic retention deleting input records.

Automatic recovery
------------------

Consumer membership errors do not use HAMSTRING's producer-only retry path.
HAMSTRING instead performs the following recovery:

#. abort the current Kafka transaction;
#. discard output records belonging to that transaction;
#. close the consumer with stale group metadata;
#. create and subscribe a fresh consumer;
#. consume again from the group's last committed offsets.

The failed input offsets were not committed, so Kafka can redeliver the batch.
The replacement transaction uses current group metadata. This recovery is
shared by the logserver, collector, prefilter, inspector, detector, and alerter
stages.

HAMSTRING also handles librdkafka's local ``_MAX_POLL_EXCEEDED`` consumer error
through the same consumer-reset path.

Broker and Docker-network outages
---------------------------------

Kafka transport failures such as ``_TIMED_OUT``, ``_MSG_TIMED_OUT``,
``_TRANSPORT``, and ``_RESOLVE`` use coordinated recovery. Native transaction
initialization, commit, and abort calls have an explicit API timeout. When a
transactional stage loses Kafka connectivity it:

#. aborts the transaction within the configured API timeout;
#. closes the consumer, thereby leaving its current group generation;
#. recreates and initializes the transactional producer;
#. subscribes a fresh consumer after Kafka is reachable;
#. reconsumes the uncommitted input batch.

The service may remain in a visible reconnect loop while Kafka or Docker DNS
is unavailable, but it no longer holds stale consumer membership while doing
so. ``_PURGE_QUEUE`` delivery callbacks during the abort describe records
discarded from the old local producer. They do not mean that the corresponding
input offsets were committed.

``environment.kafka_producer.transaction_api_timeout_seconds`` bounds each
native transactional API call. ``message_timeout_ms`` bounds how long produced
records may remain undelivered. The environment variables
``KAFKA_TRANSACTION_API_TIMEOUT_SECONDS`` and
``KAFKA_PRODUCER_MESSAGE_TIMEOUT_MS`` are final deployment overrides.

Rebalance tracking
------------------

Every HAMSTRING consumer registers Kafka's assignment callbacks:

``on_assign``
   Records the newly owned partitions and starts a new local assignment epoch.

``on_revoke``
   Invalidates the current epoch when Kafka begins taking partitions away.

``on_lost``
   Invalidates the current epoch when ownership has already been lost and an
   offset commit is no longer safe.

Each consumed record carries the local assignment epoch under which it was
fetched. Before opening a transaction, HAMSTRING verifies that every record in
the batch still belongs to the current epoch and currently assigned
partitions. A stale batch is discarded before any output transaction begins;
the consumer is reset so that its uncommitted records are fetched again.

These callbacks are delivered when the application serves Kafka events through
``consume`` or ``poll``. They therefore improve correctness and diagnostics
around observed rebalances, but they do not make unbounded processing safe. A
CPU-bound detector that does not return to Kafka before
``max_poll_interval_ms`` can still lose membership. The transaction error
recovery remains the final safety net for that case.

Preventing repeated membership loss
-----------------------------------

Choose ``max_poll_interval_ms`` with enough margin for the slowest expected
batch, and keep the transaction batch small enough that normal processing stays
well below that interval. The supplied ``config.yaml`` currently selects five
minutes explicitly; the Python configuration fallback is 30 minutes when the
setting is omitted. Increasing the interval reduces false membership loss for
long-running work, but also lets a live yet stuck worker retain its assignment
for longer.

Remember that ``kafka_transaction_batch.size`` counts Kafka records, not the
number of application events nested inside each record. Upstream application
batches may need their own bound when detector work is expensive.

Per-stage and per-topic batch settings
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Transaction batch size and collection timeout can be configured globally, by
the consuming pipeline stage, or by the complete consumed topic name:

.. code-block:: yaml

   environment:
     kafka_transaction_batch:
       size: 50
       timeout_ms: 50
       stages:
         detector:
           size: 10
         alerter:
           size: 25
       topics:
         pipeline-inspector_to_detector-domainator:
           size: 3
           timeout_ms: 25

The resolution order, from highest to lowest priority, is:

#. ``KAFKA_TRANSACTION_BATCH_SIZE`` and
   ``KAFKA_TRANSACTION_BATCH_TIMEOUT_MS`` environment variables;
#. an exact entry under ``topics``;
#. an entry under ``stages``;
#. the global ``size`` and ``timeout_ms`` values.

Stage keys can use the short component name, such as ``detector``, or the full
internal name, such as ``data_analysis.detector``. A full name overrides the
corresponding short name. Exact topic keys must contain the full topic name
consumed by the worker. If one consumer subscribes to multiple topics, it uses
the smallest effective size and timeout across those topics.

The supplied configuration sets the global value to 50 records, the detector
stage to 10 records, and the alerter stage to 25 records. Other stages inherit
the global value. A caller that explicitly supplies ``max_messages`` or
``timeout_ms`` to ``consume_batch`` still overrides the resolved defaults; the
collector uses this for its application-level batching configuration.

Operationally, alert on:

* ``_MAX_POLL_EXCEEDED`` and ``UNKNOWN_MEMBER_ID``;
* assignment revocation or loss followed by repeated recovery;
* transaction aborts while consumer lag increases;
* no successful output transaction while input offsets continue to arrive.

Monitoring sink recovery
------------------------

The monitoring agent is not part of a Kafka transaction: it inserts a consumed
batch into ClickHouse and then commits its Kafka offsets. Its recovery guarantee
is therefore **at least once**, not exactly once.

The supplied configuration consumes at most 500 monitoring records per Kafka
batch while ClickHouse continues to flush each table every 50 rows. Increasing
the Kafka value does not increase the ClickHouse insert size; it only delays the
offset commit and enlarges the batch that must be replayed after a failure.

ClickHouse connection and request calls are bounded by
``pipeline.monitoring.clickhouse_connector.connect_timeout_seconds`` and
``operation_timeout_seconds``. If an insert fails, the agent closes its Kafka
consumer before waiting for ClickHouse to return. It discards the local batch,
reconnects ClickHouse, subscribes a fresh Kafka consumer, and replays records
from committed offsets.

Normal offset commits retry transient coordinator errors only for
``environment.kafka_consumer.commit_retry_timeout_seconds``. Membership errors
such as ``UNKNOWN_MEMBER_ID`` skip that retry window and immediately cause a
fresh subscription. Retrying a commit with the same expired generation can
never succeed.

An outage can occur after ClickHouse accepted an insert but before Kafka
accepted its offset commit. Replaying the Kafka record can consequently insert
a duplicate monitoring row. This is preferable to committing an offset for a
row that may not have reached ClickHouse. Strict duplicate prevention requires
an idempotent ClickHouse schema keyed by the source Kafka topic, partition, and
offset; the current raw monitoring tables do not provide that guarantee.

Delivery guarantees outside Kafka
---------------------------------

The consume-process-produce transaction provides exactly-once behavior for
Kafka records and Kafka offsets. File writes, ClickHouse inserts, webhooks, and
other external actions are not part of that transaction. A recovered batch is
processed again and can repeat those external effects. Integrations requiring
deduplication should use a stable identifier such as ``server_message_id`` or
``logline_id``.
