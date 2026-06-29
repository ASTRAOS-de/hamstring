Logline format configuration
............................

If a wants to add a new inspector or detector, it might be necessary to adapt the logline formats, if the preexisting ones do not contain the needed information.
To do so, one can adapt or define logcollector formats in the main configuration file (``config.yaml``) under ``pipeline.log_collection.collectors.[collector_name].required_log_information``.
Adding a new logcollector enables prefilters (and later on onspectors and detectors) to consume from a new Kafka topic.

Currently, we support timestamps, IP addresses, regular expressions, and list-based validation for data fields in a logline.
For example, a logline for the DNS protocol might look like this:

.. code-block:: console

   2025-04-04T14:45:32.458123Z NXDOMAIN 192.168.3.152 10.10.0.3 test.com AAAA 192.168.15.34 196b

Field Definition Structure
^^^^^^^^^^^^^^^^^^^^^^^^^^

Each list entry of the parameter defines one field of the input logline, and the order of the entries corresponds to the
order of the values in each logline. Each list entry itself consists of a list with
two to four entries depending on the field type. For example, a field definition might look like this:

.. code-block:: console

   [ "status_code", ListItem, [ "NOERROR", "NXDOMAIN" ], [ "NXDOMAIN" ] ]

Field Names and Requirements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The first entry of each field definition always corresponds to the name of the field. Certain field names are required
for proper pipeline operation, while others are forbidden as they are reserved for internal use.

.. list-table:: Required and forbidden field names
   :header-rows: 1
   :widths: 15 50

   * - Required
     - ``ts``, ``src_ip``
   * - Forbidden
     - ``logline_id``, ``batch_id``

**Required fields** must be present in the configuration as they are essential for pipeline processing.
**Forbidden fields** are reserved for internal communication and cannot be used as custom field names.

Field Types and Validation
^^^^^^^^^^^^^^^^^^^^^^^^^^

The second entry specifies the type of the field. Depending on the type defined, the method for defining
validation parameters varies. The third and fourth entries change depending on the type.

There are four field types available:

.. list-table:: Field types
   :header-rows: 1
   :widths: 20 25 20 35

   * - Field type
     - Format of 3rd entry
     - Format of 4th entry
     - Description
   * - ``Timestamp``
     - Timestamp format string
     - *(not used)*
     - Validates timestamp fields using Python's strptime format. Automatically converts to ISO format for internal processing.
       Example: ``"%Y-%m-%dT%H:%M:%S.%fZ"``
   * - ``IpAddress``
     - *(not used)*
     - *(not used)*
     - Validates IPv4 and IPv6 addresses. No additional parameters required.
   * - ``RegEx`` (Regular Expression)
     - RegEx pattern as string
     - *(not used)*
     - Validates field content against a regular expression pattern. If the pattern matches, the field is valid.
   * - ``ListItem``
     - List of allowed values
     - List of relevant values *(optional)*
     - Validates field values against an allowed list. Optionally defines relevant values for filtering in later pipeline stages.
       All relevant values must also be in the allowed list. If not specified, all allowed values are deemed relevant.

Configuration Examples
^^^^^^^^^^^^^^^^^^^^^^

Here are examples for each field type:

.. code-block:: yaml

   logline_format:
     - [ "timestamp", Timestamp, "%Y-%m-%dT%H:%M:%S.%fZ" ]
     - [ "status_code", ListItem, [ "NOERROR", "NXDOMAIN" ], [ "NXDOMAIN" ] ]
     - [ "client_ip", IpAddress ]
     - [ "domain_name", RegEx, '^(?=.{1,253}$)((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)' ]
     - [ "record_type", ListItem, [ "A", "AAAA" ] ]


Logging Configuration
.....................

The following parameters control the logging behavior.

.. list-table:: ``logging`` Parameters
   :header-rows: 1
   :widths: 15 50

   * - Parameter
     - Description
   * - base
     - The ``debug`` field enables debug-level logging if set to ``true`` for all files, that do not contain the main modules.
   * - modules
     - For each module, the ``debug`` field can be set to show debug-level logging messages.

If a ``debug`` field is set to ``false``, only info-level logging is shown. By default, all the fields are set to ``false``.


Pipeline Configuration
......................

The following parameters control the behavior of each stage of the HAMSTRING pipeline, including the
functionality of the modules.

``pipeline.scaling``
^^^^^^^^^^^^^^^^^^^^

Controls how many independent workers each pipeline module starts. Each worker owns its own Kafka
consumer and producer, so workers consuming the same topic join the same Kafka consumer group and can
process different partitions in parallel. Values from ``defaults`` apply to every module and can be
overridden per module under ``modules``. Modules that run several configured instances can also override
an individual instance under ``instances``.

Scaling is resolved in this order:

#. ``pipeline.scaling.defaults``
#. ``pipeline.scaling.modules.<module-name>``
#. ``pipeline.scaling.modules.<module-name>.instances.<instance-name>``

The instance names are the configured pipeline object names, not Docker service names. For example,
``log_collection.collector.instances.dga_collector`` applies only to the collector whose
``pipeline.log_collection.collectors[].name`` is ``dga_collector``.

.. code-block:: yaml

   pipeline:
     scaling:
       defaults:
         executor: thread
         max_workers: 1
       modules:
         log_collection.collector:
           executor: thread
           max_workers: 2
           instances:
             dga_collector:
               threads: 4
         data_analysis.detector:
           executor: process
           processes: 2
         pipeline.alerter:
           executor: hybrid
           processes: 2
           threads_per_process: 4

.. list-table:: Scaling options
   :header-rows: 1
   :widths: 25 20 55

   * - Parameter
     - Default
     - Description
   * - ``executor``
     - ``thread``
     - Worker model. Valid values are ``thread``, ``process``, and ``hybrid``.
   * - ``threads``
     - ``1``
     - Number of thread workers for ``executor: thread``. In ``executor: hybrid``, this is accepted as an alias for ``threads_per_process``.
   * - ``threads_per_process``
     - ``1``
     - Number of thread workers inside each process for ``executor: hybrid``.
   * - ``processes``
     - ``1``
     - Number of worker processes for ``executor: process`` or ``executor: hybrid``.
   * - ``max_workers``
     - ``1``
     - Backwards-compatible worker-count alias. For ``thread`` it maps to ``threads``; for pure ``process`` it maps to ``processes``.
   * - ``workers``
     - ``1``
     - Alias for ``max_workers``.
   * - ``instances``
     - none
     - Per-configured-instance overrides. The nested keys must match the instance names listed below.

``thread`` mode starts ``threads`` independent workers in the service process. ``process`` mode starts
``processes`` worker processes with one worker each. ``hybrid`` mode starts ``processes`` processes with
``threads_per_process`` worker threads inside each process.

If ``executor`` is omitted, HAMSTRING infers it from the worker-count keys:

* ``threads`` only: ``thread``
* ``processes`` only: ``process``
* ``processes`` and ``threads`` or ``threads_per_process``: ``hybrid``

For example, this starts two processes with four Kafka-consuming workers in each process:

.. code-block:: yaml

   pipeline:
     scaling:
       modules:
         data_analysis.detector:
           executor: hybrid
           processes: 2
           threads_per_process: 4

This is equivalent, because ``threads`` is an alias for ``threads_per_process`` in hybrid mode:

.. code-block:: yaml

   pipeline:
     scaling:
       modules:
         data_analysis.detector:
           processes: 2
           threads: 4

Per-instance overrides are useful when one configured stage is more expensive than another. This example
uses hybrid mode for all log collectors, but gives the ``dga_collector`` fewer workers and the
``domainator_collector`` pure process workers:

.. code-block:: yaml

   pipeline:
     scaling:
       modules:
         log_collection.collector:
           executor: hybrid
           processes: 2
           threads_per_process: 4
           instances:
             dga_collector:
               processes: 1
               threads_per_process: 2
             domainator_collector:
               executor: process
               processes: 3

The effective number of Kafka consumers for one configured pipeline instance is:

.. code-block:: text

   Docker service replicas * processes * threads_per_process

For ``thread`` mode, ``processes`` is ``1``. For pure ``process`` mode, ``threads_per_process`` is ``1``.
The consumed Kafka topic needs at least that many partitions to keep every worker busy. HAMSTRING requests
at least the local worker count when creating or expanding topics; set ``NUMBER_OF_INSTANCES`` on the
service when Docker Compose replicas are used so topic creation can account for the replica count as well.

.. list-table:: Module and instance keys
   :header-rows: 1
   :widths: 30 30 40

   * - Module key
     - Instance keys
     - Example
   * - ``log_storage.logserver``
     - Full consumed input topic name. Without an instance override, the module setting applies to every logserver protocol topic.
     - ``pipeline-logserver_in-dns`` when the ``logserver_in`` topic prefix is ``pipeline-logserver_in`` and the protocol is ``dns``.
   * - ``log_collection.collector``
     - ``pipeline.log_collection.collectors[].name``
     - ``dga_collector``, ``domainator_collector``
   * - ``log_filtering.prefilter``
     - ``pipeline.log_filtering[].name``
     - ``dga_filter``, ``domainator_filter``
   * - ``data_inspection.inspector``
     - ``pipeline.data_inspection[].name``
     - ``dga_inspector``, ``domainator_inspector``
   * - ``data_analysis.detector``
     - ``pipeline.data_analysis[].name``
     - ``RF-dga_detector``, ``domainator``
   * - ``pipeline.alerter``
     - ``generic`` and ``pipeline.alerting.plugins[].name``
     - ``generic``, ``attributor``
   * - ``monitoring.agent``
     - No per-instance key by default.
     - Configure the module key directly.

Docker Compose service replicas are configured separately from ``pipeline.scaling``. Compose replicas add
more containers; ``pipeline.scaling`` adds more workers inside each container. Both forms of scaling use the
same Kafka consumer group for the same stage/topic.

For local Docker Compose runs, scale services with ``docker compose up --scale``:

.. code-block:: console

   $ HOST_IP=127.0.0.1 docker compose -f docker/docker-compose.yml --profile prod up --scale logcollector=3 --scale detector=2

For the development profile, use the ``-dev`` service names from ``docker/docker-compose.yml``:

.. code-block:: console

   $ HOST_IP=127.0.0.1 docker compose -f docker/docker-compose.yml --profile dev up --scale logcollector-dev=3 --scale detector-dev=2

The compose fragments under ``docker/docker-compose/dev`` and ``docker/docker-compose/prod`` also contain
``deploy.replicas`` fields. Those fields document the intended replica count and are used by orchestrators
that honor Compose ``deploy`` settings. For portable local Compose usage, prefer the explicit ``--scale``
flag and keep ``NUMBER_OF_INSTANCES`` aligned with the replica count:

.. code-block:: yaml

   services:
     detector:
       environment:
         - GROUP_ID=data_analysis
         - NUMBER_OF_INSTANCES=2

.. code-block:: console

   $ HOST_IP=127.0.0.1 docker compose -f docker/docker-compose.yml --profile prod up --scale detector=2

With this example and the hybrid detector config shown above, the detector starts
``2 Docker replicas * 2 processes * 4 threads_per_process = 16`` Kafka consumers.

``pipeline.log_storage``
^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: ``logserver`` Parameters
   :header-rows: 1
   :widths: 30 20 50

   * - Parameter
     - Default Value
     - Description
   * - input_file
     - ``"/opt/file.txt"``
     - Path of the input file, to which data is appended during usage.

       Keep this setting unchanged when using Docker; modify the ``MOUNT_PATH`` in ``docker/.env`` instead.

``pipeline.log_collection``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: ``collectors`` Parameters
   :header-rows: 1
   :widths: 30 70

   * - Parameter
     - Description
   * - name
     - A unique name amongst the ``collectors`` configurations top identify the collector instance.
   * - protocol_base
     - The lowercase protocol name to ingest data from. Currently supported: ``dns`` and ``http``.
   * - required_log_information
     - Defines the expected format for incoming log lines. See the :doc:`configuration` page for more
       details.

Each log_collector has a BatchHandler instance. Default confgurations for all Batch handlers are defined in ``pipeline.log_collection.default_batch_handler_config``.
You can override these values for each logcollector instance by adjusting the values inside the ``pipeline.log_collection.collectors.[collector_instance].batch_handler_config_override``.
The following list shows the available configuration options.

.. list-table:: ``batch_handler`` Parameters
   :header-rows: 1
   :widths: 30 20 50

   * - Parameter
     - Default Value
     - Description
   * - batch_size
     - ``10000``
     - Number of entries in a Batch, at which it is sent due to reaching the maximum fill state.
   * - batch_timeout
     - ``30.0``
     - Time after which a Batch is sent. Mainly relevant for Batches that only contain a small number of entries, and
       do not reach the size limit for a longer time period.
   * - subnet_id.ipv4_prefix_length
     - ``24``
     - The number of bits to trim from the client's IPv4 address for use as `Subnet ID`.
   * - subnet_id.ipv6_prefix_length
     - ``64``
     - The number of bits to trim from the client's IPv6 address for use as `Subnet ID`.




``pipeline.log_filtering``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: ``prefilter`` Parameters
   :header-rows: 1
   :widths: 30 70

   * - Parameter
     - Description
   * - name
     - A unique name amongst the prefilter configurations top identify the prefitler instance.
   * - relevance_method
     - The name of the method used to to check if a given logline is relevant for further inspection.
       This check can be skipped by choosing ``"no_relevance_check"``.
       Avalable configurations are: ``"no_relevance_check"``, ``"check_dga_relevance"``
   * - collector_name
     - The name of the collector configuration the prefilter consumes data from. The same collector name can be referenced in multiple prefilter configurations.

``pipeline.data_inspection``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: ``inspector`` Parameters
   :header-rows: 1
   :widths: 30 70

   * - Parameter
     - Description
   * - name
     - A unique name amongst the inspector configurations top identify the inspector instance.
   * - prefilter_name
     - The name of the prefitler configuration the inspector consumes data from. The same prefilter name can be referenced in multiple inspector configurations.
   * - inspector_module_name
     - Name of the python file in ``"src/inspector/plugins/"`` the inspector should use.
   * - inspector_class_name
     - Name of the class inside the ``inspector_module`` to use.



Inspectors can be added easily by implementing the base class for an inspector. More information is available at :ref:`inspection_stage`.
Each inspector might be needing additional configurations. These are also documented at :ref:`inspection_stage`.

To entirely skip the anomaly detection phase, you can set ``inspector_module_name: "no_inspector"`` and ``inspector_class_name: "NoInspector"``.

``pipeline.data_analysis``
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: ``detector`` Parameters
   :header-rows: 1
   :widths: 30 20 50

   * - Parameter
     - Default Value
     - Description
   * - name
     -
     - A unique name amongst the detector configurations top identify the detector instance.
   * - inspector_name
     -
     - The name of the inspector configuration the detector consumes data from. The same inspector name can be referenced in multiple detector configurations. Omit this or set ``consume_from: detector`` when the detector consumes from another detector.
   * - consume_from
     - ``inspector``
     - Set to ``detector`` for detector instances that consume from the detector-to-detector topic instead of an inspector.
   * - detector_module_name
     -
     - Name of the python file in ``"src/detector/plugins/"`` the detector should use.
   * - detector_class_name
     -
     - Name of the class inside the ``detector_module`` to use.
   * - model
     - ``rf`` option: ``XGBoost``
     - Model to use for the detector
   * - checksum
     - Not given here
     - Checksum for the model file to ensure integrity
   * - base_url
     - https://heibox.uni-heidelberg.de/d/0d5cbcbe16cd46a58021/
     - Base URL for downloading the model if not present locally
   * - threshold
     - ``0.5``
     - Threshold for the detector's classification.
   * - produce_topics
     - ``(empty)``
     - (Optional) Comma-separated list of alerter topic suffixes to produce alerts to. If left empty, defaults to the ``generic`` alerter topic. Use ``send_to_alerter: false`` or ``produce_topics: []`` for intermediary detectors that should not produce to an alerter.
   * - next_detectors
     - ``(empty)``
     - (Optional) Comma-separated list of detector instance names that should receive this detector's suspicious output on detector-to-detector topics.
   * - send_to_alerter
     - ``true``
     - Set to ``false`` to disable the detector-to-alerter Kafka output while still allowing detector-to-detector forwarding.


``pipeline.alerting``
^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: ``alerting`` Parameters
   :header-rows: 1
   :widths: 30 20 50

   * - Parameter
     - Default Value
     - Description
   * - log_to_file
     - ``true``
     - Boolean flag to enable/disable logging of alerts to a local file.
   * - log_to_kafka
     - ``true``
     - Boolean flag to enable/disable forwarding of alerts to an external Kafka topic.
   * - log_file_path
     - ``"/opt/logs/alerts.txt"``
     - Local file path where alerts will be appended if ``log_to_file`` is enabled.
   * - external_kafka_topic
     - ``"hamstring_alerts"``
     - Name of the external Kafka topic where alerts will be sent if ``log_to_kafka`` is enabled.
   * - plugins
     - ``[]``
     - List of custom alerter plugins to execute. Each plugin must specify ``name``, ``alerter_module_name``, and ``alerter_class_name``.

``pipeline.zeek``
^^^^^^^^^^^^^^^^^

To configure the Zeek sensors to ingest data, an entry in ther ``pipeline.zeek.sensors`` must be adapted or added.
Each of the configured sensores is meant to run on a different machine or network interface to collect data.
Each instance configured needs to be setup using the ``docker-compose.yaml``. The dictionary name needs to exactly correspond with the
name of the instance configured there.
Each sensore has the following configuration parameters:

.. list-table:: ``zeek`` Parameters
   :header-rows: 1
   :widths: 30 70

   * - Parameter
     - Description
   * - static_analysis
     - A bool to indicate whether or not a static analysis should be executed. If ``true``, the PCAPs from ``"data/test_pcaps"`` which are mounted to
       each Zeek instance are analyzed. If set to ``false``, a network analysis is executed on the configured network interfaces.
   * - protocols
     - List of lowercase names of protocols the Zeek sensor should be monitoring and sending in the Kafka Queues. Currently supported: ``"dns"`` and ``http``.
   * - interfaces
     - List of network interface names for a network analysis to monitor. As the Zeek containers run in ``host`` mode, all network interfaces of the node are automatically mounted and ready to be scraped.

Environment Configuration
.........................

The following parameters control the infrastructure of the software.

.. list-table:: ``environment`` Parameters
   :header-rows: 1
   :widths: 15 15 50

   * - Parameter
     - Default Value
     - Description
   * - kafka_brokers
     - ``hostname: kafka1, port: 8097, node_ip: 0.0.0.0``, ``hostname: kafka2, port: 8098, node_ip: 0.0.0.0``, ``hostname: kafka3, port: 8099, node_ip: 0.0.0.0``
     - Hostnames and ports of the Kafka brokers, given as list. The node ip is crucial and needs to be set to the actual IP of the system where the Kafka broker will be running on.
   * - kafka_topics_prefix
     - Not given here
     - Kafka topic name prefixes given as strings. These prefix name are used to construct the actual topic names based on the instance name (e.g. a collector instance name) that produces for the given stage.
       (e.g. a prefilter instance name is added as suffix to the prefilter_to_inspector prefix for the inspector to know where to consume.)
   * - kafka_consumer.max_poll_interval_ms
     - ``1800000``
     - Maximum time in milliseconds between Kafka consumer polls before Kafka removes the consumer from its group. Increase this for long-running detector batches.
   * - kafka_topics.replication_factor
     - ``3``
     - Replication factor used when creating new Kafka topics. At runtime this is capped to the number of configured Kafka brokers.
   * - kafka_topics.auto_expand_partitions
     - ``true``
     - If enabled, existing HAMSTRING topics with fewer than the desired partition count are automatically expanded on consumer startup. Kafka does not support shrinking partition counts, so topics that are already larger are left unchanged.
   * - kafka_topics.stages
     - See ``config.yaml``
     - Per-pipeline-stage topic settings. Keys match ``environment.kafka_topics_prefix.pipeline`` keys. Each stage can set ``partitions`` and ``replication_factor`` for topics whose names use that stage prefix.
   * - kafka_topics.topics
     - See ``config.yaml``
     - Exact per-topic settings for topics that are not represented by a pipeline prefix, for example external alert topics. Topics without a stage or exact entry use 12 partitions and the default replication factor.
   * - monitoring.clickhouse_server.hostname
     - ``clickhouse-server``
     - Hostname of the ClickHouse server. Used by Grafana.
