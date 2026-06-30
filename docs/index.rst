Welcome to HAMSTRING's documentation!
======================================

**HAMSTRING** is a CIDS framework to run signature and machine learning-based IDS classifiers. It employs several string and anomaly-based filtering techniques
to maximize detection efficiency. Currently the tool focueses on DNS attacks, as it incorporates heiDGAF (DGA detection) & Domainator (DNS Tunneling Detector)
:cite:p:`petrov_domainator_2025` :cite:p:`machmeier_heidgaf`.

Check out the :doc:`usage` section for further information on how to use the software, including how to
:ref:`install <installation>` and :ref:`configure <configuration>` the project. For more details on the implementation
and structure, take a look at the :doc:`pipeline` section. The :doc:`monitoring` section describes how to set up the
monitoring environment for observing the software's functionality in real-time.

.. note::

   This project is under active development.


Contents
--------

.. toctree::
   :maxdepth: 2

   usage
   pipeline
   monitoring
   training
   developer_guide
   api/index
   sources
   references
