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
       size: 250
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

The supplied configuration sets the detector stage to 10 records, the alerter
stage to 25 records, and the initial Domainator topic to 5 records. Other stages
inherit the global value. A caller that explicitly supplies ``max_messages`` or
``timeout_ms`` to ``consume_batch`` still overrides the resolved defaults; the
collector uses this for its application-level batching configuration.

Operationally, alert on:

* ``_MAX_POLL_EXCEEDED`` and ``UNKNOWN_MEMBER_ID``;
* assignment revocation or loss followed by repeated recovery;
* transaction aborts while consumer lag increases;
* no successful output transaction while input offsets continue to arrive.

Delivery guarantees outside Kafka
---------------------------------

The consume-process-produce transaction provides exactly-once behavior for
Kafka records and Kafka offsets. File writes, ClickHouse inserts, webhooks, and
other external actions are not part of that transaction. A recovered batch is
processed again and can repeat those external effects. Integrations requiring
deduplication should use a stable identifier such as ``server_message_id`` or
``logline_id``.
