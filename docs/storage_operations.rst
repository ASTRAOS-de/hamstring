Storage-constrained Swarm deployments
=====================================

This guide describes the storage policy for a three-node HAMSTRING Swarm in
which each node has approximately 50 GB of local storage.  It is intentionally
conservative: Kafka, ClickHouse, container images, container logs, and the
operating system must all fit on the same small hosts unless separate
filesystems are provided.

Why a node can fill even with Kafka retention
----------------------------------------------

Kafka's ``retention.bytes`` is a rolling limit per partition replica, not a
broker-wide disk quota.  Kafka also needs active segments, indexes, internal
topics, and temporary deletion headroom.  With replication factor three and
three brokers, each broker normally stores one replica of every partition, so
each node carries approximately one complete logical copy of the retained
pipeline.

Other host storage is independent of Kafka retention.  In particular:

* Docker's unbounded ``json-file`` log driver can create multi-gigabyte
  ``*-json.log`` files under ``/var/lib/docker/containers``.
* Container image layers and snapshots may be stored under
  ``/var/lib/containerd`` and are not included by a size check limited to
  ``/var/lib/docker``.
* ClickHouse TTL deletion happens during background merges and can temporarily
  use more space than the steady-state retained data.
* Swarm's default local volumes remain on the node where they were created.
  Rescheduling a stateful service can leave old data behind and create a new
  local volume elsewhere.

For a 48 GB usable root filesystem, use the following operating budget:

.. list-table:: Per-node storage budget
   :header-rows: 1
   :widths: 55 20

   * - Consumer
     - Target maximum
   * - Operating system, container runtime, and current images
     - 10 GB
   * - Kafka
     - 12 GB
   * - ClickHouse on its assigned node
     - 10 GB
   * - Container and system logs
     - 1 GB
   * - Other writable data and temporary files
     - 5 GB
   * - Free emergency headroom
     - 10 GB

The targets are operational thresholds, not guarantees imposed by a single
Kafka or ClickHouse setting.  Separate filesystems, LVM logical volumes, or
filesystem quotas are required for hard isolation.

Stateful service placement
--------------------------

Kafka and ClickHouse default to ``node.platform.os == linux`` and can therefore
be scheduled on any Linux node.  On a three-node production cluster, verify
after every deployment that the three brokers run on separate physical nodes
and that ClickHouse has not moved away from its intended local data.

If deterministic placement is desired, override the constraints with the
existing Swarm node hostnames during deployment:

.. code-block:: console

   export KAFKA1_PLACEMENT_CONSTRAINT='node.hostname == hamstring-1'
   export KAFKA2_PLACEMENT_CONSTRAINT='node.hostname == hamstring-2'
   export KAFKA3_PLACEMENT_CONSTRAINT='node.hostname == hamstring-3'
   export CLICKHOUSE_PLACEMENT_CONSTRAINT='node.hostname == hamstring-1'

Use the actual Swarm node hostnames.  These overrides are optional.  Moving a
Kafka node ID onto another node with an unrelated local volume remains unsafe.

Hard storage isolation
----------------------

By default, the stack still uses Docker named volumes under Docker's data
root.  On a fresh host, prefer dedicated, quota-controlled mount points and
provide them as bind-mount sources:

.. code-block:: console

   export KAFKA1_DATA_SOURCE=/srv/hamstring/kafka
   export KAFKA2_DATA_SOURCE=/srv/hamstring/kafka
   export KAFKA3_DATA_SOURCE=/srv/hamstring/kafka
   export CLICKHOUSE_DATA_SOURCE=/srv/hamstring/clickhouse
   export CLICKHOUSE_LOG_SOURCE=/srv/hamstring/clickhouse-logs

Only the broker assigned to a node uses that node's
``/srv/hamstring/kafka`` directory.  Create the directories with ownership and
permissions appropriate for the container images before deploying.  On the
ClickHouse node, a workable 48 GB layout is an 18 GB root/runtime filesystem,
a 15 GB Kafka filesystem, and a 15 GB ClickHouse filesystem.  Keep Kafka below
12 GB and ClickHouse below 10 GB in normal operation; their remaining space is
emergency headroom.  Nodes without ClickHouse can leave more space for the
root/runtime filesystem.  This layout requires the active container image
footprint to remain within the documented runtime budget.

Container logging
-----------------

Every service in ``docker/docker_swarm/docker-compose.swarm.yml`` uses Docker's
``local`` logging driver with these defaults:

.. code-block:: yaml

   logging:
     driver: local
     options:
       max-size: 10m
       max-file: "3"

This limits ordinary stdout/stderr history to approximately 30 MB per
container before compression.  Override the values with
``DOCKER_LOG_DRIVER``, ``DOCKER_LOG_MAX_SIZE``, and ``DOCKER_LOG_MAX_FILE``.
Do not select ``json-file`` without also configuring rotation.

The stack-level setting protects HAMSTRING services.  Set the host-wide Docker
default as well so unrelated containers cannot fill the same filesystem.  Add
or merge the following keys in ``/etc/docker/daemon.json`` on every node:

.. code-block:: json

   {
     "log-driver": "local",
     "log-opts": {
       "max-size": "10m",
       "max-file": "3"
     }
   }

Restarting Docker is disruptive.  Plan the restart one node at a time, and
recreate existing containers afterward because changing the daemon default
does not retrofit their logging configuration.  Docker documents the local
driver and its rotation behavior at
https://docs.docker.com/engine/logging/drivers/local/.

Verify a deployed service's logging policy with:

.. code-block:: console

   docker service inspect hamstring_zeek \
     --format '{{json .Spec.TaskTemplate.ContainerSpec.LogDriver}}'

Kafka topic and disk policy
---------------------------

``config.yaml`` now supplies Kafka-native topic creation options under each
``config`` mapping.  New topics use the following policy:

.. list-table:: Kafka topic defaults
   :header-rows: 1
   :widths: 35 15 20 20

   * - Topic class
     - Partitions
     - Byte limit per partition
     - Time limit
   * - Default monitoring/telemetry topic
     - 1
     - 64 MiB
     - 15 minutes
   * - Zeek input
     - 3
     - 512 MiB
     - 2 hours
   * - Ordinary intermediate stage
     - 3
     - 128 MiB
     - 30 minutes
   * - Detector fan-out/alerter stage
     - 3
     - 256 MiB
     - 1 hour
   * - External alert topic
     - 3
     - 512 MiB
     - 24 hours

Both limits apply; Kafka can delete a closed segment when either limit is
exceeded.  The recoverable outage is therefore bounded by
``retention bytes / actual topic byte rate`` and may be shorter than the time
limit.  If a consumer falls behind the retained window, the missing records
cannot be recovered from Kafka.

The broker defaults use RF=3, minimum ISR=2, unclean leader election disabled,
16 MiB segments, five-minute segment rolling, and a one-minute retention scan.
This keeps the cluster writable with one broker unavailable while avoiding an
unsafe leader election.

HAMSTRING's Python producers use Zstandard compression by default to reduce
broker disk and replication traffic for JSON pipeline and monitoring records.
This does not automatically configure the separately packaged Zeek producer;
enable a supported Kafka compression option in that image independently.

Topic creation settings apply when HAMSTRING creates a topic.  They do not
shrink or reconfigure an already existing topic, and Kafka cannot reduce an
existing partition count.  Use a fresh cluster or an explicitly planned topic
migration when changing from the old 12-partition layout.

ClickHouse retention and limits
-------------------------------

High-cardinality raw monitoring tables retain six hours instead of one day and
use daily partitions so expired data can be reclaimed more efficiently.  The
``alerts`` table retains 30 days.  Lower-cardinality rollups preserve longer
dashboard history.  Eligible TTL merges are checked every five minutes.

ClickHouse is also configured with ``keep_free_space_bytes=5368709120``.  This
causes ClickHouse to refuse new allocations before it consumes the final 5 GiB
visible on its data filesystem.  It is not a 10 GB ClickHouse quota, and it
does not reserve that space against Kafka, containerd, or other root processes.
Use the dedicated ClickHouse mount described above for a hard boundary.

TTL deletion is asynchronous and may briefly require extra merge space.  Check
the live table footprint with:

.. code-block:: sql

   SELECT
       database,
       table,
       formatReadableSize(sum(bytes_on_disk)) AS disk_size
   FROM system.parts
   WHERE active
   GROUP BY database, table
   ORDER BY sum(bytes_on_disk) DESC;

Image and host storage maintenance
----------------------------------

Measure the whole runtime footprint, not only Docker volumes:

.. code-block:: console

   df -h /
   du -xhd1 /var/lib | sort -h
   du -xhd1 /var/lib/docker | sort -h
   du -xhd1 /var/lib/containerd | sort -h
   docker system df -v
   journalctl --disk-usage
   lsof +L1

``lsof +L1`` reveals deleted files that are still open and therefore counted
by ``df`` but not by ``du``.  Review ``docker system df -v`` after deployments
and remove superseded, unused image versions through a controlled maintenance
procedure.  Do not run broad pruning automatically on production nodes without
checking what is reclaimable.

Alert at 70%, 80%, and 90% filesystem use.  At 90%, stop or shed ingress before
the root filesystem reaches 100%; a small local disk cannot guarantee both
indefinite live capture and lossless recovery during an extended downstream
outage.
