Monitoring
~~~~~~~~~~

.. note::

   This page is under active development.

Overview
========

The software includes a monitoring functionality that stores relevant information in a database (`ClickHouse`). The
collected data is then visualized using multiple `Grafana` dashboard views.

.. image:: ../assets/readme_assets/overview.png


Setup
=====

Normal mode
-----------

Both, `ClickHouse` and `Grafana` can be executed as their own Docker container. All needed containers are started
when executing:

.. code-block:: console

   $ HOST_IP=127.0.0.1 docker compose -f docker/docker-compose.yml up


All modules send their monitoring-relevant information to Kafka, from which it is then collected by the
`Monitoring Agent` module. This module checks their validity and resumes by storing the values in `ClickHouse`. By the
default configuration defined in ``docker-compose.yml``, `Grafana` automatically loads the dashboard views and fills
them with the data in `ClickHouse`. The dashboard views can then be observed on ``localhost:3000`` (by default).

Storage retention
-----------------

HAMSTRING treats Kafka as transient transport. By default each Kafka partition replica has a four-hour / 16 MiB
retention target, whichever limit is reached first. Kafka can delete only *closed* log segments, so the Compose files
also roll a segment every 15 minutes (or after 4 MiB); this makes time-based retention work for low-volume topics.
All Kafka values can be overridden at startup, for example:

.. code-block:: console

   KAFKA_LOG_RETENTION_HOURS=12 KAFKA_LOG_RETENTION_BYTES=67108864 \\
     HOST_IP=127.0.0.1 docker compose -f docker/docker-compose.yml --profile prod up

``KAFKA_LOG_RETENTION_BYTES`` is a limit **per partition replica**, rather than a per-broker or per-volume limit.
Increasing the partition count, replication factor, or this value increases the possible disk usage accordingly.

ClickHouse monitoring tables have a one-day TTL (alerts have a 60-day TTL). TTL deletion happens in background
merges; the supplied ClickHouse configuration checks for TTL work every 15 minutes. Its server logs are also rotated
at 100 MiB with three retained files.

The new retention settings apply after restarting Kafka and ClickHouse. They reclaim data as the brokers close new
segments and ClickHouse performs TTL merges; they do not remove a stopped stack's volume files immediately. If this is
a disposable local environment and its historical data is not needed, stop the stack and remove its volumes with
``docker compose -f docker/docker-compose.yml down -v`` before starting it again. Do not remove production volumes as
a retention operation.

`Datatest` mode
---------------

For users interested in testing their own machine learning models used by the detection algorithm in the `Data Analysis`
stage, the monitoring functionality can be started in the `datatest` mode:

.. code-block:: console

   $ HOST_IP=127.0.0.1 docker compose --profile datatest -f docker-compose.yml -f ./docker-compose/prod/docker-compose.datatest.yml up

`Grafana` then shows one more dashboard view, `Datatests`, that shows the confusion matrix for a testing dataset.
Make sure that you set the profile to `datatest` and use the additional docker-compose file
``docker-compose/prod/docker-compose.datatest.yml``.

.. warning::

   This feature is in an early development stage!
