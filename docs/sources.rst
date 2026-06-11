Sources and Attribution
~~~~~~~~~~~~~~~~~~~~~~~

This page tracks scientific sources, dataset origins, and model attribution notes for HAMSTRING detector models.

Domainator Model Sources
========================

The Domainator detector and attributor models use the same subdomain-level feature family and differ by their label space. The current model family combines malware, tunneling-tool, and benign DNS traffic sources:

.. list-table:: Domainator training sources
   :header-rows: 1
   :widths: 25 45 30

   * - Source
     - Contribution
     - Citation
   * - Domainator malware samples
     - Real DNS-tunneling malware samples and the feature processing procedure used for subdomain sequence metadata.
     - Petrov et al. :cite:p:`petrov_domainator_2025`
   * - LSTM DNS covert-channel dataset
     - DNS tunneling/covert-channel tool traffic used as malicious tunneling examples.
     - Chen et al. :cite:p:`chen_dns_lstm_2021`
   * - GraphTunnel dataset
     - DNS tunneling samples used to broaden tunneling-tool coverage.
     - Gao et al. :cite:p:`gao_graphtunnel_2024`
   * - Benign DNS traffic
     - Real DNS traffic used as legitimate traffic for training and evaluation.
     - Žiža et al. :cite:p:`ziza_dns_exfiltration_2023`

Attribution Notes
=================

- Keep new detector or attributor model descriptions in :ref:`detection_stage`.
- Add scientific publications to ``docs/refs.bib`` and cite them from the relevant model documentation.
- Record dataset provenance here whenever a model release changes training sources, label definitions, or intended attribution semantics.
