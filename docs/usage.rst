Usage
=====

.. note::

   This page is under active development.

.. _installation:
.. _configuration:


Getting Started
---------------

To use HAMSTRING, use the provided ``docker-compose.yml`` with either the production or development profile:

.. code-block:: console

   $ HOST_IP=127.0.0.1 docker compose -f docker/docker-compose.yml --profile prod up

For local development builds, use the development profile:

.. code-block:: console

   $ HOST_IP=127.0.0.1 docker compose -f docker/docker-compose.yml --profile dev up

Set ``HOST_IP`` to the host address that external Kafka clients should use. The default
``localhost`` works for single-host local runs.

Docker Swarm Deployment
-----------------------

Use ``docker/docker_swarm/docker-compose.swarm.yml`` when deploying HAMSTRING as
a Swarm stack. Swarm honors the ``deploy`` sections for replicas, restart
policies, resource limits, and placement constraints:

.. code-block:: console

   $ docker swarm init --advertise-addr <manager-ip>
   $ HAMSTRING_ROOT="$PWD" docker stack deploy -c docker/docker_swarm/docker-compose.swarm.yml hamstring

Set ``--advertise-addr`` to the manager IP address that worker nodes and
published services should use.

Configure the stack with environment variables before deployment. Common options
include ``HAMSTRING_IMAGE_REGISTRY`` and per-service image tags, replica counts
such as ``LOGCOLLECTOR_REPLICAS`` and ``DETECTOR_REPLICAS``, published ports such
as ``GRAFANA_PORT`` and ``PROMETHEUS_PORT``, and placement constraints such as
``DETECTOR_PLACEMENT_CONSTRAINT``. Remove the stack with:

.. code-block:: console

   $ docker stack rm hamstring

Scaling With Docker Compose
---------------------------

HAMSTRING has two scaling axes:

* Docker Compose replicas start more containers for a service.
* ``pipeline.scaling`` in ``config.yaml`` starts more workers inside each service container.

Use Docker Compose replicas when you want horizontal service scaling across containers. For the production
profile, scale the production service names:

.. code-block:: console

   $ HOST_IP=127.0.0.1 docker compose -f docker/docker-compose.yml --profile prod up --scale logcollector=3 --scale detector=2

For the development profile, scale the ``-dev`` service names:

.. code-block:: console

   $ HOST_IP=127.0.0.1 docker compose -f docker/docker-compose.yml --profile dev up --scale logcollector-dev=3 --scale detector-dev=2

Before scaling, configure the consumed topics with enough partitions for the
whole consumer group. Partition counts for newly created topics live under
``environment.kafka_topics`` in ``config.yaml``. Existing topics must be
resized explicitly with Kafka administration tooling.

The compose fragments also contain ``deploy.replicas`` fields. Use them for orchestrators that honor Compose
``deploy`` settings; for local ``docker compose up`` runs, the explicit ``--scale`` flag is the clearest option.

For worker scaling inside a container, configure ``pipeline.scaling``. For example, this starts two detector
processes with four worker threads each in every detector container:

.. code-block:: yaml

   pipeline:
     scaling:
       modules:
         data_analysis.detector:
           executor: hybrid
           processes: 2
           threads_per_process: 4

With ``--scale detector=2``, that configuration creates ``2 Docker replicas * 2 processes * 4 threads``:
16 Kafka consumers for the detector stage. See :ref:`configuration` for the full scaling option reference and
per-instance override examples.

Installation
------------

Install all Python requirements.

.. code-block:: console

   $ python -m venv .venv

.. code-block:: console

   $ source .venv/bin/activate

.. code-block:: console

   (.venv) $ sh install_requirements.sh

Now, you can start each module, e.g. the `Inspector`:

.. code-block:: console

   (.venv) $ python -m src.inspector.inspector

Configuration
-------------

.. _logline-format-configuration:

.. include:: configuration.rst
