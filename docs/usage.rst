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

When using Compose replicas, set ``NUMBER_OF_INSTANCES`` for the scaled service to the same replica count so
Kafka topic creation can request enough partitions for the whole consumer group:

.. code-block:: yaml

   services:
     detector:
       environment:
         - GROUP_ID=data_analysis
         - NUMBER_OF_INSTANCES=2

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

   (.venv) $ python src/inspector/main.py

Configuration
-------------

.. _logline-format-configuration:

.. include:: configuration.rst
