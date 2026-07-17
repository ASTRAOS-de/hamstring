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

``pipeline.acceleration``
^^^^^^^^^^^^^^^^^^^^^^^^^

Controls optional hardware acceleration for modules that support accelerated execution. Values from
``default`` are used unless a configured inspector or detector overrides them with its own
``acceleration`` block.

.. list-table:: Acceleration options
   :header-rows: 1
   :widths: 30 20 50

   * - Parameter
     - Default Value
     - Description
   * - ``enabled``
     - ``true``
     - Enables accelerated execution where a module supports it.
   * - ``fallback_to_cpu``
     - ``true``
     - Allows a module to continue on CPU if the requested accelerator is unavailable.
   * - ``log_device``
     - ``true``
     - Logs the selected acceleration device during startup.
   * - ``default.device``
     - ``auto``
     - Default device selection. Use ``auto`` for automatic detection, or a module-supported device name.
   * - ``default.backend``
     - ``auto``
     - Default acceleration backend. Use ``auto`` for module-specific automatic backend selection.
   * - ``default.batch_size``
     - ``auto``
     - Default accelerated inference batch size.

``pipeline.resilience``
^^^^^^^^^^^^^^^^^^^^^^^

Controls retry behavior around transient startup and infrastructure errors.

.. list-table:: Retry options
   :header-rows: 1
   :widths: 30 20 50

   * - Parameter
     - Default Value
     - Description
   * - ``retry.initial_delay_seconds``
     - ``1.0``
     - Delay before the first retry.
   * - ``retry.max_delay_seconds``
     - ``30.0``
     - Maximum delay between retries.
   * - ``retry.backoff_multiplier``
     - ``2.0``
     - Multiplier used for exponential backoff.
   * - ``retry.jitter_seconds``
     - ``0.25``
     - Random jitter added to retry delays.
   * - ``retry.log_every_attempts``
     - ``5``
     - Log every nth retry attempt while a dependency is still unavailable.

Kafka routing and exactly-once processing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

HAMSTRING uses the canonical ``src_ip`` as the Kafka record key from logserver
onward. This keeps one IP on one partition within each topic. Different topics
may use different partition counts; a key does not need the same numeric
partition in every stage. The original server message ID is transported in a
Kafka header and is retained in the collector payload for monitoring
correlation.

``environment.kafka_pipeline_mode`` selects the delivery implementation used
by every pipeline stage. ``exactly_once`` atomically commits output records and
consumed Kafka offsets in one transaction. ``simple`` publishes outputs before
synchronously committing the input offsets and therefore provides
at-least-once delivery. This guarantee covers Kafka records and offsets only;
ClickHouse monitoring writes and alerter side effects are separate systems.

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
         threads_per_process: 1
       modules:
         log_collection.collector:
           executor: thread
           threads_per_process: 2
           instances:
             dga_collector:
               threads_per_process: 4
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
   * - ``threads_per_process``
     - ``1``
     - Number of thread workers in the service process for ``thread`` or inside each process for ``hybrid``.
   * - ``processes``
     - ``1``
     - Number of worker processes for ``executor: process`` or ``executor: hybrid``.
   * - ``instances``
     - none
     - Per-configured-instance overrides. The nested keys must match the instance names listed below.

``thread`` mode starts ``threads_per_process`` independent workers in the service process. ``process`` mode starts
``processes`` worker processes with one worker each. ``hybrid`` mode starts ``processes`` processes with
``threads_per_process`` worker threads inside each process.

If ``executor`` is omitted, HAMSTRING uses ``thread``. The only supported scaling
keys are ``executor``, ``processes``, and ``threads_per_process``; unknown keys
are rejected as configuration errors.

For example, this starts two processes with four Kafka-consuming workers in each process:

.. code-block:: yaml

   pipeline:
     scaling:
       modules:
         data_analysis.detector:
           executor: hybrid
           processes: 2
           threads_per_process: 4

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
The consumed Kafka topic needs at least that many partitions to keep every worker busy. Configure the
desired partition count for each newly created topic under ``environment.kafka_topics.stages`` or
``environment.kafka_topics.topics``. HAMSTRING does not resize existing topics at application startup;
change those topics explicitly with Kafka administration tooling when scaling requires more partitions.

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
flag. Ensure that the consumed topic was created with enough partitions for the resulting worker count.

.. code-block:: console

   $ HOST_IP=127.0.0.1 docker compose -f docker/docker-compose.yml --profile prod up --scale detector=2

With this example and the hybrid detector config shown above, the detector starts
``2 Docker replicas * 2 processes * 4 threads_per_process = 16`` Kafka consumers.

For Docker Swarm, deploy the dedicated stack file instead of the local Compose
profiles:

.. code-block:: console

   $ docker swarm init --advertise-addr <manager-ip>
   $ HAMSTRING_ROOT="$PWD" docker stack deploy -c docker/docker_swarm/docker-compose.swarm.yml hamstring

Set ``--advertise-addr`` to the manager IP address that worker nodes and
published services should use.

The Swarm file reads replica counts such as ``LOGSERVER_REPLICAS``,
``LOGCOLLECTOR_REPLICAS``, ``PREFILTER_REPLICAS``, ``INSPECTOR_REPLICAS``,
``DETECTOR_REPLICAS``, ``ALERTER_REPLICAS``, and ``ZEEK_REPLICAS``. It also
accepts image tags, published ports, and placement constraints through
environment variables, for example ``HAMSTRING_DETECTOR_IMAGE_TAG``,
``GRAFANA_PORT``, and ``DETECTOR_PLACEMENT_CONSTRAINT``.

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

       Keep this setting unchanged when using Docker. Mount a different host file with
       ``LOGSERVER_INPUT_PATH`` instead.

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
   * - poll_timeout_ms
     - ``250``
     - Maximum Kafka poll wait while a partial batch is pending. It determines how quickly the collector notices the configured ``batch_timeout``; it does not shorten that timeout.
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
     - The name of the inspector configuration the detector consumes data from. The same inspector name can be referenced in multiple detector configurations. Omit it when ``consume_from`` is ``detector``.
   * - consume_from
     - ``inspector``
     - Accepts only ``inspector`` or ``detector``. The latter consumes from the detector-to-detector topic instead of an inspector.
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
     - Optional YAML list of alerter topic suffixes. An omitted or empty list uses the ``generic`` alerter topic. Set ``send_to_alerter: false`` for intermediary detectors that should not produce to an alerter.
   * - next_detectors
     - ``(empty)``
     - Optional YAML list of detector instance names that should receive this detector's suspicious output on detector-to-detector topics.
   * - send_to_alerter
     - ``true``
     - Set to ``false`` to disable the detector-to-alerter Kafka output while still allowing detector-to-detector forwarding.
   * - acceleration
     - See ``pipeline.acceleration``
     - Optional per-detector acceleration override.


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
   * - log_rotation.enabled
     - ``true``
     - Enables rotation for the alert log file.
   * - log_rotation.retention_days
     - ``7``
     - Number of days to retain rotated alert log files.
   * - external_kafka_topic
     - ``"hamstring_alerts"``
     - Name of the external Kafka topic where alerts will be sent if ``log_to_kafka`` is enabled.
   * - plugins
     - ``[]``
     - List of custom alerter plugins to execute. Each plugin must specify ``name``, ``alerter_module_name``, and ``alerter_class_name``.

``pipeline.monitoring``
^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: ``monitoring`` Parameters
   :header-rows: 1
   :widths: 30 20 50

   * - Parameter
     - Default Value
     - Description
   * - kafka_consumer.batch_size
     - ``5000``
     - Maximum number of monitoring Kafka records fetched before one offset commit. This is independent of the per-table ClickHouse insert batch size.
   * - kafka_consumer.timeout_ms
     - ``250``
     - Maximum wait in milliseconds for a partial monitoring Kafka fetch.
   * - clickhouse_connector.batch_size
     - ``50``
     - Number of monitoring rows written to ClickHouse in one batch.

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
     - ``hostname: kafka1, internal_port: 19092, external_port: 8097, node_ip: 127.0.0.1``, ``hostname: kafka2, internal_port: 19093, external_port: 8098, node_ip: 127.0.0.1``, ``hostname: kafka3, internal_port: 19094, external_port: 8099, node_ip: 127.0.0.1``
     - Kafka broker endpoints. Containers use ``hostname`` and ``internal_port`` on the Docker network;
       host-network clients use ``node_ip`` and ``external_port``.
   * - kafka_topics_prefix
     - Not given here
     - Kafka topic name prefixes given as strings. These prefix name are used to construct the actual topic names based on the instance name (e.g. a collector instance name) that produces for the given stage.
       (e.g. a prefilter instance name is added as suffix to the prefilter_to_inspector prefix for the inspector to know where to consume.)
   * - kafka_consumer.max_poll_interval_ms
     - ``1800000``
     - Maximum time in milliseconds between Kafka consumer polls before Kafka removes the consumer from its group. Increase this for long-running detector batches.
   * - kafka_pipeline_mode
     - ``exactly_once``
     - Delivery implementation used consistently by all pipeline stages. Supported values are
       ``exactly_once`` and ``simple`` (synchronous at-least-once).
   * - kafka_max_record_bytes
     - ``900000``
     - Maximum Kafka application record size. Collector batches are split below this boundary so
       records remain below the broker's configured maximum.
   * - kafka_transaction_batch.size
     - ``100``
     - Maximum number of source records committed in one transactional Kafka batch by the logserver.
       Other stages use the same commit and transaction timeout settings for their EOS producers.
   * - kafka_transaction_batch.timeout_ms
     - ``50``
     - Maximum wait for the next transactional Kafka batch, in milliseconds.
   * - kafka_transaction_batch.commit_timeout_ms
     - ``15000``
     - Maximum time an EOS producer waits for a transaction commit or abort before recovering its producer.
   * - kafka_transaction_batch.transaction_timeout_ms
     - ``30000``
     - Kafka transaction lifetime advertised by every EOS producer; must not exceed the broker maximum.
   * - kafka_topics.replication_factor
     - ``3``
     - Replication factor used when creating new Kafka topics. At runtime this is capped to the number of configured Kafka brokers.
   * - kafka_topics.stages
     - See ``config.yaml``
     - Per-pipeline-stage settings used when creating missing topics. Keys match
       ``environment.kafka_topics_prefix.pipeline`` keys. Each stage independently sets
       ``partitions`` and ``replication_factor`` for topics using that prefix.
   * - kafka_topics.topics
     - See ``config.yaml``
     - Exact per-topic settings for topics that are not represented by a pipeline prefix, for example external alert topics. Topics without a stage or exact entry use 12 partitions and the default replication factor.
   * - monitoring.clickhouse_server.hostname
     - ``clickhouse-server``
     - Hostname of the ClickHouse server. Used by Grafana.
   * - monitoring.clickhouse_server.http_port
     - ``8123``
     - Optional HTTP port used by the container entrypoint when waiting for ClickHouse. If omitted,
       HAMSTRING uses ``8123``.

Deployment Environment Variables
................................

The Docker Compose and Docker Swarm files expose the following environment variables. Values shown here
are the defaults used when no override is provided.

Set overrides in the deployment shell or its ``.env`` file. If an application
override is unset, the service uses the corresponding value from the mounted
``config.yaml`` instead.

Service readiness
^^^^^^^^^^^^^^^^^

HAMSTRING application images use ``src.base.service_entrypoint`` as their container entrypoint. The
entrypoint waits for selected infrastructure before starting the module script.

.. list-table:: Readiness variables
   :header-rows: 1
   :widths: 30 25 45

   * - Variable
     - Default
     - Description
   * - ``HAMSTRING_WAIT_FOR``
     - unset
     - Comma-separated dependencies to wait for. Supported values are ``kafka`` and ``clickhouse``.
       Compose and Swarm set ``kafka`` for pipeline modules and ``kafka,clickhouse`` for the
       monitoring agent.
   * - ``HAMSTRING_WAIT_INITIAL_DELAY_SECONDS``
     - ``30``
     - Delay before dependency checks start.
   * - ``HAMSTRING_WAIT_TIMEOUT_SECONDS``
     - ``180``
     - Maximum wait time per dependency endpoint.
   * - ``HAMSTRING_WAIT_INTERVAL_SECONDS``
     - ``2``
     - Delay between readiness attempts.
   * - ``HAMSTRING_KAFKA_WAIT_ENDPOINTS``
     - from ``environment.kafka_brokers``
     - Optional comma-separated ``host:port`` list overriding the Kafka endpoints used by the
       readiness check.
   * - ``HAMSTRING_CLICKHOUSE_WAIT_ENDPOINT``
     - from ``environment.monitoring.clickhouse_server``
     - Optional ``host:port`` endpoint overriding the ClickHouse readiness check.

Application service variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: Application service variables
   :header-rows: 1
   :widths: 30 25 45

   * - Variable
     - Default
     - Description
   * - ``GROUP_ID``
     - service-specific
     - Kafka consumer group for the service. Compose and Swarm set service defaults such as
       ``log_storage``, ``log_collection``, ``log_filtering``, ``data_inspection``,
       ``data_analysis``, ``data_alerting``, and ``monitoring_agent``.
   * - ``MONITORING_AGENT_GROUP_ID``
     - ``monitoring_agent``
     - Swarm override for the monitoring agent ``GROUP_ID``.
   * - ``LOGSERVER_GROUP_ID``
     - ``log_storage``
     - Swarm override for the logserver ``GROUP_ID``.
   * - ``LOGCOLLECTOR_GROUP_ID``
     - ``log_collection``
     - Swarm override for the logcollector ``GROUP_ID``.
   * - ``PREFILTER_GROUP_ID``
     - ``log_filtering``
     - Swarm override for the prefilter ``GROUP_ID``.
   * - ``INSPECTOR_GROUP_ID``
     - ``data_inspection``
     - Swarm override for the inspector ``GROUP_ID``.
   * - ``DETECTOR_GROUP_ID``
     - ``data_analysis``
     - Swarm override for the detector ``GROUP_ID``.
   * - ``ALERTER_GROUP_ID``
     - ``data_alerting``
     - Swarm override for the alerter ``GROUP_ID``.
   * - ``KAFKA_TOPIC_PARTITIONS``
     - ``12``
     - Default partition count requested for new HAMSTRING Kafka topics.
   * - ``KAFKA_TOPIC_REPLICATION_FACTOR``
     - from ``environment.kafka_topics.replication_factor``
     - Replication factor requested for new HAMSTRING Kafka topics. At runtime this is capped to the
       configured broker count.
   * - ``KAFKA_PIPELINE_MODE``
     - from ``environment.kafka_pipeline_mode``
     - Selects ``exactly_once`` or ``simple`` delivery for all pipeline consumers and producers.
   * - ``KAFKA_MAX_RECORD_BYTES``
     - from ``environment.kafka_max_record_bytes``
     - Overrides the application record-size ceiling used by producers and collector packet splitting.
   * - ``KAFKA_BROKER_MAX_RECORD_BYTES``
     - ``2097152``
     - Broker record-batch ceiling for Compose and Swarm. Keep this above
       ``KAFKA_MAX_RECORD_BYTES`` because a producer batch includes framing and may close after
       adding one final record beyond its target batch size.
   * - ``KAFKA_TRANSACTION_BATCH_SIZE``
     - from ``environment.kafka_transaction_batch.size``
     - Maximum number of source records processed in one logserver Kafka transaction.
   * - ``KAFKA_TRANSACTION_BATCH_TIMEOUT_MS``
     - from ``environment.kafka_transaction_batch.timeout_ms``
     - Maximum time the logserver waits to fill a Kafka transaction batch.
   * - ``KAFKA_TRANSACTION_COMMIT_TIMEOUT_MS``
     - from ``environment.kafka_transaction_batch.commit_timeout_ms``
     - Maximum time an EOS producer waits for each commit or abort call.
   * - ``KAFKA_TRANSACTION_TIMEOUT_MS``
     - from ``environment.kafka_transaction_batch.transaction_timeout_ms``
     - Transaction lifetime configured on every EOS producer.
   * - ``KAFKA_TRANSACTIONAL_ID_PREFIX``
     - ``HOSTNAME``
     - Optional transactional-id namespace for one deployment replica. HAMSTRING combines it with
       the stage, configured instance, input topic, and worker ID. It must differ for concurrently
       running Compose containers or Swarm replicas (for example, include a Docker Swarm task slot).
   * - ``HAMSTRING_CONFIG_CHECKSUM``
     - current compose value
     - Optional deployment marker used to force Swarm service updates when ``config.yaml`` changes.
   * - ``NVIDIA_VISIBLE_DEVICES``
     - ``all``
     - GPU device selection for detector services that use GPU acceleration.
   * - ``NVIDIA_DRIVER_CAPABILITIES``
     - ``compute,utility``
     - NVIDIA runtime capabilities for GPU detector services.
   * - ``ZEEK_CONTAINER_NAME``
     - ``zeek``
     - Container name passed to the Zeek image.

Image variables
^^^^^^^^^^^^^^^

.. list-table:: Image variables
   :header-rows: 1
   :widths: 35 25 40

   * - Variable
     - Default
     - Description
   * - ``HAMSTRING_IMAGE_REGISTRY``
     - ``ghcr.io/astraos-de``
     - Registry used for HAMSTRING application images.
   * - ``HAMSTRING_MONITORING_IMAGE_TAG``
     - ``v2.2.0``
     - Tag for ``hamstring-monitoring``.
   * - ``HAMSTRING_LOGSERVER_IMAGE_TAG``
     - ``v2.2.0``
     - Tag for ``hamstring-logserver``.
   * - ``HAMSTRING_LOGCOLLECTOR_IMAGE_TAG``
     - ``v2.2.0``
     - Tag for ``hamstring-logcollector``.
   * - ``HAMSTRING_PREFILTER_IMAGE_TAG``
     - ``v2.2.0``
     - Tag for ``hamstring-prefilter``.
   * - ``HAMSTRING_INSPECTOR_IMAGE_TAG``
     - ``v2.2.0``
     - Tag for ``hamstring-inspector``.
   * - ``HAMSTRING_DETECTOR_IMAGE_TAG``
     - ``v2.2.0``
     - Tag for ``hamstring-detector``.
   * - ``HAMSTRING_ALERTER_IMAGE_TAG``
     - ``v2.2.0``
     - Tag for ``hamstring-alerter``.
   * - ``HAMSTRING_ZEEK_IMAGE_TAG``
     - ``2.0.1``
     - Tag for ``hamstring-zeek``.
   * - ``KAFKA_IMAGE_REPOSITORY``
     - ``confluentinc/cp-kafka``
     - Kafka image repository.
   * - ``KAFKA_IMAGE_TAG``
     - ``8.2.2``
     - Kafka image tag.
   * - ``CLICKHOUSE_IMAGE_REPOSITORY``
     - ``clickhouse/clickhouse-server``
     - ClickHouse image repository.
   * - ``CLICKHOUSE_IMAGE_TAG``
     - ``26.5-alpine``
     - ClickHouse image tag.
   * - ``GRAFANA_IMAGE_REPOSITORY``
     - ``grafana/grafana``
     - Grafana image repository.
   * - ``GRAFANA_IMAGE_TAG``
     - ``13.0.3-slim``
     - Grafana image tag.
   * - ``PROMETHEUS_IMAGE_REPOSITORY``
     - ``prom/prometheus``
     - Prometheus image repository.
   * - ``PROMETHEUS_IMAGE_TAG``
     - ``latest``
     - Prometheus image tag.
   * - ``KAFKA_EXPORTER_IMAGE_REPOSITORY``
     - ``danielqsj/kafka-exporter``
     - Kafka exporter image repository.
   * - ``KAFKA_EXPORTER_IMAGE_TAG``
     - ``latest``
     - Kafka exporter image tag.

Infrastructure variables
^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: Infrastructure variables
   :header-rows: 1
   :widths: 35 25 40

   * - Variable
     - Default
     - Description
   * - ``HOST_IP``
     - ``localhost``
     - Host advertised by Kafka for external listeners in Docker Compose.
   * - ``KAFKA_EXTERNAL_HOST``
     - ``localhost``
     - Host advertised by Kafka for external listeners in Docker Swarm.
   * - ``KAFKA1_EXTERNAL_PORT``
     - ``8097``
     - Published external port for Kafka broker 1.
   * - ``KAFKA2_EXTERNAL_PORT``
     - ``8098``
     - Published external port for Kafka broker 2.
   * - ``KAFKA3_EXTERNAL_PORT``
     - ``8099``
     - Published external port for Kafka broker 3.
   * - ``KAFKA_CLUSTER_ID``
     - ``MkU3OEVBNTcwNTJENDM2Qk``
     - Kafka KRaft cluster id.
   * - ``KAFKA_CONTROLLER_QUORUM_VOTERS``
     - ``1@kafka1:29093,2@kafka2:29093,3@kafka3:29093``
     - Kafka KRaft controller voter list.
   * - ``KAFKA_AUTO_CREATE_TOPICS_ENABLE``
     - ``false``
     - Kafka broker auto-topic-creation setting.
   * - ``KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR``
     - ``3``
     - Kafka transaction state topic replication factor.
   * - ``KAFKA_TRANSACTION_STATE_LOG_MIN_ISR``
     - ``2``
     - Kafka transaction state topic minimum in-sync replicas.
   * - ``KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR``
     - ``3``
     - Kafka offsets topic replication factor.
   * - ``KAFKA_LOG_RETENTION_HOURS``
     - ``4``
     - Kafka log retention in hours.
   * - ``KAFKA_LOG_RETENTION_BYTES``
     - ``10737418240``
     - Kafka log retention size.
   * - ``KAFKA_LOG_SEGMENT_BYTES``
     - ``1073741824``
     - Kafka log segment size.
   * - ``KAFKA_LOG_CLEANUP_POLICY``
     - ``delete``
     - Kafka log cleanup policy.
   * - ``KAFKA_LOG4J_LOGGERS``
     - Kafka controller defaults
     - Optional Kafka log category configuration for brokers 2 and 3.
   * - ``CLICKHOUSE_USER``
     - ``default``
     - ClickHouse user used by ClickHouse, Grafana, and the monitoring agent.
   * - ``CLICKHOUSE_PASSWORD``
     - ``hamstring``
     - ClickHouse password used by ClickHouse, Grafana, and the monitoring agent.
   * - ``CLICKHOUSE_HTTP_PORT``
     - ``8123``
     - Published ClickHouse HTTP port in Swarm.
   * - ``CLICKHOUSE_NATIVE_PORT``
     - ``9000``
     - Published ClickHouse native client port in Swarm.
   * - ``GRAFANA_PORT``
     - ``3000``
     - Published Grafana port in Swarm.
   * - ``GRAFANA_ADMIN_USER``
     - ``admin``
     - Grafana admin username.
   * - ``GRAFANA_ADMIN_PASSWORD``
     - ``admin``
     - Grafana admin password.
   * - ``GRAFANA_INSTALL_PLUGINS``
     - ``grafana-clickhouse-datasource``
     - Grafana plugins installed at startup.
   * - ``PROMETHEUS_PORT``
     - ``9088``
     - Published Prometheus port in Swarm.
   * - ``PROMETHEUS_CONFIG_FILE``
     - ``../../docker/prometheus/prometheus.yml``
     - Prometheus config file used for the Swarm config object.
   * - ``KAFKA_EXPORTER_PORT``
     - ``9308``
     - Published Kafka exporter port in Swarm.

Path and mount variables
^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table:: Path variables
   :header-rows: 1
   :widths: 35 25 40

   * - Variable
     - Default
     - Description
   * - ``HAMSTRING_ROOT``
     - ``../..``
     - Base path for Swarm bind mounts to ``config.yaml`` and provisioning files.
   * - ``LOGSERVER_INPUT_PATH``
     - ``docker/default_input``
     - Host file mounted to ``/opt/file.txt`` for the logserver. The exact relative default differs
       between Compose fragments and the Swarm file but resolves to ``docker/default_input`` from the
       repository.
   * - ``ALERTER_LOGS_PATH``
     - ``/opt/logs`` in prod, ``../../../logs`` in dev
     - Host directory mounted to ``/opt/logs`` by Docker Compose. The Swarm stack uses a named volume
       instead.

Swarm scheduling variables
^^^^^^^^^^^^^^^^^^^^^^^^^^

Docker Swarm placement constraints default to ``node.platform.os == linux``. Set the corresponding
variable to pin a service to a labeled node, for example
``KAFKA1_PLACEMENT_CONSTRAINT='node.labels.kafka1 == true'``.

.. list-table:: Swarm placement and replica variables
   :header-rows: 1
   :widths: 35 25 40

   * - Variable
     - Default
     - Description
   * - ``KAFKA1_PLACEMENT_CONSTRAINT``
     - ``node.platform.os == linux``
     - Placement constraint for Kafka broker 1.
   * - ``KAFKA2_PLACEMENT_CONSTRAINT``
     - ``node.platform.os == linux``
     - Placement constraint for Kafka broker 2.
   * - ``KAFKA3_PLACEMENT_CONSTRAINT``
     - ``node.platform.os == linux``
     - Placement constraint for Kafka broker 3.
   * - ``CLICKHOUSE_PLACEMENT_CONSTRAINT``
     - ``node.platform.os == linux``
     - Placement constraint for ClickHouse.
   * - ``GRAFANA_PLACEMENT_CONSTRAINT``
     - ``node.platform.os == linux``
     - Placement constraint for Grafana.
   * - ``PROMETHEUS_PLACEMENT_CONSTRAINT``
     - ``node.platform.os == linux``
     - Placement constraint for Prometheus.
   * - ``KAFKA_EXPORTER_PLACEMENT_CONSTRAINT``
     - ``node.platform.os == linux``
     - Placement constraint for Kafka exporter.
   * - ``MONITORING_AGENT_PLACEMENT_CONSTRAINT``
     - ``node.platform.os == linux``
     - Placement constraint for the monitoring agent.
   * - ``LOGSERVER_PLACEMENT_CONSTRAINT``
     - ``node.platform.os == linux``
     - Placement constraint for the logserver.
   * - ``LOGCOLLECTOR_PLACEMENT_CONSTRAINT``
     - ``node.platform.os == linux``
     - Placement constraint for the logcollector.
   * - ``PREFILTER_PLACEMENT_CONSTRAINT``
     - ``node.platform.os == linux``
     - Placement constraint for the prefilter.
   * - ``INSPECTOR_PLACEMENT_CONSTRAINT``
     - ``node.platform.os == linux``
     - Placement constraint for the inspector.
   * - ``DETECTOR_PLACEMENT_CONSTRAINT``
     - ``node.platform.os == linux``
     - Placement constraint for the detector.
   * - ``ALERTER_PLACEMENT_CONSTRAINT``
     - ``node.platform.os == linux``
     - Placement constraint for the alerter.
   * - ``ZEEK_PLACEMENT_CONSTRAINT``
     - ``node.platform.os == linux``
     - Placement constraint for Zeek.
   * - ``PREFILTER_REPLICAS``
     - ``1``
     - Swarm replica count for the prefilter service.
   * - ``INSPECTOR_REPLICAS``
     - ``1``
     - Swarm replica count for the inspector service.
   * - ``DETECTOR_REPLICAS``
     - ``1``
     - Swarm replica count for the detector service.
   * - ``ALERTER_REPLICAS``
     - ``1``
     - Swarm replica count for the alerter service.
   * - ``ZEEK_REPLICAS``
     - ``1``
     - Swarm replica count for the Zeek service.
   * - ``INSPECTOR_CPU_LIMIT``
     - ``2``
     - Swarm CPU limit for the inspector.
   * - ``INSPECTOR_MEMORY_LIMIT``
     - ``512M``
     - Swarm memory limit for the inspector.
   * - ``INSPECTOR_CPU_RESERVATION``
     - ``1``
     - Swarm CPU reservation for the inspector.
   * - ``INSPECTOR_MEMORY_RESERVATION``
     - ``256M``
     - Swarm memory reservation for the inspector.
