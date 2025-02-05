# Changelog

All notable changes to the CDC Aggregator OAI-PMH Repo Handler will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).


## 0.10.0 - 2025-01-17

### Added

- Support more DDI-C elements in `oai_ddi25`
  metadataprefix. (Implements
  [#77](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/77))

  - `/codeBook/stdyDscr/dataAccs/useStmt/conditions/@elementVersion`
  - `/codeBook/stdyDscr/citation/distStmt/distDate` CDATA
  - `/codeBook/stdyDscr/method/dataColl/resInstru`,
    `/codeBook/stdyDscr/method/dataColl/resInstru@xml:lang`,
    `/codeBook/stdyDscr/method/dataColl/resInstru/concept`,
    `/codeBook/stdyDscr/method/dataColl/resInstru/concept/@vocab`,
    `/codeBook/stdyDscr/method/dataColl/resInstru/concept/@vocabURI`
  - `/codeBook/stdyDscr/stdyInfo/sumDscr/collDate` CDATA

### Changed

- Update dependencies in requirements.txt:

  - Tornado 6.4.2
  - CESSDA Aggregator Shared Library 0.9.0
  - Kuha Common 2.6.0
  - Kuha OAI-PMH Repo Handler 1.9.0


## 0.9.0 - 2024-09-06

### Added

- Support Python 3.11 & Python 3.12.
- New test environment for tox 'warnings-as-errors' to treat warnings
  as errors in tests. Run this environment in CI with latest python.

### Changed

- Update dependencies in requirements.txt to support Python 3.12:

  - Genshi 0.7.9
  - Tornado 6.4.1
  - PyYAML 6.0.2
  - Py12fLogging 0.7.0
  - Kuha Common 2.5.0
  - Kuha OAI-PMH Repo Handler 1.8.0
  - CESSDA Aggregator Shared Library 0.8.1

- Update dependencies to latest in requirements.txt:

  - ConfigArgParse 1.7
  - prometheus-client 0.20.0

- Don’t render empty IDNo & holdings elements inside relPubl in
  metadataprefix `oai_ddi25`. CESSDA validator considers IDNo elements
  without agency-attribute as invalid according to CDC Profile
  2.5. This is implemented in Kuha OAI-PMH Repo Handler 1.7.0.
- Migrate away from `kuha_common.testcases` in tests, since stdlib
  unittest now offers `AsyncMock` & `IsolatedAsyncioTestCase`.

### Fixed

- The submitted resumptionToken value is validated and
  BadResumptionToken OAI-PMH error is rendered if it is
  invalid. Invalid resumptionToken -message is also logged with
  WARNING severity. This is implemented in Kuha OAI-PMH Repo Handler
  1.8.0.
- Fix random `ResourceWarning: unclosed event loop` when running
  tests. `KuhaUnitTestCase` does not properly close the event
  loop on tearDown. Migrate away from `KuhaUnitTestCase` and use
  `unittest.IsolatedAsyncioTestCase` when running coroutines is
  required.


## 0.8.0 - 2024-04-30

### Added

- Support Study.principal_investigator attributes ``external_link``,
  ``external_link_uri``, ``external_link_role`` and
  ``external_link_title``.
- Render /CodeBook/stdyDscr/citation/rspStmt/AuthEnty/ExtLink to DDI-C
  (metadataprefixes `ddi_c` and `oai_ddi25`).

### Changed

- Require CESSDA CDC Aggregator Shared Library 0.7.0 in
  requirements.txt and setup.py. (Implements
  [#72](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/72))
- Require Kuha OAI-PMH Repo Handler 1.5.0 in requirements.txt and
  setup.py. (Implements
  [#72](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/72))
- Require Kuha Common 2.4.0 in requirements.txt and
  setup.py. (Implements
  [#72](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/72))


## 0.7.0 - 2024-01-24

### Fixed

- Empty resumptionToken is now intepreted correctly by XSLT
  processing. It means the list response is complete. (Fixes
  [#67](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/67))
- Use new study schema introduced by Shared Library 0.6.0 to count
  metrics exposed via /metrics endpoint. This improves performance by
  decreasing response times of the endpoint.

### Changed

- Require Kuha OAI-PMH Repo Handler 1.4.1 in requirements.txt.
- Require CESSDA CDC Aggegator Shared Library 0.6.0 in requirements.txt and setup.py.


## 0.6.0 - 2023-08-29
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8277095.svg)](https://doi.org/10.5281/zenodo.8277095)

### Added

- Add /metrics endpoint to serve prometheus metrics (Implements
  [#43](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/43))

### Fixed

- Read configuration option `--server-process-count`. (Fixes
  [#45](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/45))


## 0.5.0 - 2023-03-17

### Added

- Include `study.study_uris` to element <dc:identifier> in `oai_dc`
  serialization. (Implements
  [#40](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/40))


## 0.4.0 - 2022-12-21

### Added

- Add hard-coded `resourceType` to `oai_datacite` serialization which
  always has the value `Dataset`. (Implements
  [#33](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/33))
- Add hard-coded `dc:type` element to `oai_dc` serialization which
  always has the value `Dataset`. (Implements
  [#36](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/36))
- Add XML Stylesheet to make OAI responses more
  human-friendly. (Implements
  [#22](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/22))
- Configuration option to control XML Stylesheets
  (``--oai-pmh-stylesheet-url``):
    - Set to empty string to disable stylesheets completely.
    - Set to a full URL to serve the stylesheet from some external file server.
    - Start with a slash ('/') to serve via Kuha OAI-PMH Repo Handler server.
    - Defaults to '/v0/oai/static/oai2.xsl', which works with other
      default configuration values and uses Kuha OAI-PMH Repo Handler server
      to actually serve the file.

### Changed

- Require Kuha OAI-PMH Repo Handler 1.2.0 in requirements.txt and setup.py.

### Fixed

- Make sure `oai_datacite` serialization yields valid Datacite
  v3. (Fixes
  [#35](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/35/))
    - Remove invalid xml:lang attributes.
    - Wrap geoLocationPlace inside geoLocation element.


## 0.3.0 - 2022-11-22

### Added

- Grant & funding information to `oai_datacite` and `oai_ddi25`
  metadata. (Implements
  [#34](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/34))
- Related publication identifiers and agencies to `oai_datacite` and
  `oai_ddi25` metadata. (Implements
  [#34](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/34))

### Changed

- Add primary lookup to `oai_datacite` Publisher from
  Study.distributors. The current lookup from Study.publishers will
  remain as a secondary source. (Fixes [#31](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/31))
- Update dependencies:
    - Require Aggregator Shared Library 0.5.0 in requirements.txt and setup.py.
    - Require Kuha OAI-PMH Repo Handler 1.1.0 in requirements.txt and setup.py.
    - Require Kuha Common 2.0.1 in requirements.txt and 2.0.0 or newer in setup.py.
    - Require Tornado 6.2.0 in requirements.txt.
    - Require Genshi 0.7.7 in requirements.txt.

### Fixed

- Change lookup order of preferred PublicationYear value for
  `oai_datacite`. (Fixes [#30](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/30))
- Format PublicationYear value for `oai_datecite` so that it is a
  year, instead of full datestamp. (Fixes [#30](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/30))
- Include mandatory Date property to `oai_datacite`. (Fixes [#29](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/29))


## 0.2.1 - 2022-06-29

### Changed

- Require Aggregator Shared Library 0.3.0 in requirements.txt.
- Require Kuha Common 1.0.0 in requirements.txt.
- Require Kuha OAI-PMH Repo Handler 1.0.1 in requirements.txt.

### Fixed

- Include missing fields to oai_ddi25 metadata: (Fixes
  [#26](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/26))
    - /codeBook/docDscr/citation/titlStmt/titl
    - /codeBook/stdyDscr/citation/holdings/@URI
    - /codeBook/stdyDscr/stdyInfo/sumDscr/dataKind

- Render subject in oai_dc metadata regardless if the study.keyword
  does not have a value. (Fixes
  [#27](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/27))



## 0.2.0 - 2021-12-17
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.5779937.svg)](https://doi.org/10.5281/zenodo.5779937)

### Added

- Mapping file syntax for source-sets (SourceAggMDSet-class) now
  supports setname and description key-value pairs. The setname is
  mandatory while description is optional.
- Mapping file for configurable-sets now supports external mapping
  files via `path` key. (Implements
  [#18](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/18))
- Mapping file for configurable-sets is validated upon server startup.

### Changed

- Mapping file configuration option for SourceAggMDSet
  `--oai-set-sources-path` defaults to None, which implies that the
  set is discarded (not loaded) on server startup. The operator is in
  charge of creating and configuring the mapping file.
- Update dependencies in requirements.txt
    - PyYAML 6.0.0
    - ConfigArgParse 1.5.3
    - Kuha Common to Git commit 8e7de1f16530decc356fee660255b60fcacaea23
    - Kuha OAI-PMH Repo Handler to Git commit cbe6d16bbe00369ccddc8a0ae5bcd64f8476755e
    - CDC Aggregator Shared Library 0.2.0

### Fixed

- Value for the altered attribute in Provenance containers is now
  either 'true' or 'false. (Fixes
  [#14](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/14))
- Empty setName elements for language-sets are populated with
  generated values. Key-value pairs for setname are expected to be
  defined for source-sets in configured mapping file. (Fixes
  [#15](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/15))
- Source set no longer falls back to automatically generating sets
  based on source archive's baseUrl. (Fixes
  [#15](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/15))
- deletedRecord declaration is now configurable. (Fixes
  [#16](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/16))
- Provenance container's baseUrl element name should be baseURL. (Fixes
  [#19](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/19))


## 0.1.0 - 2021-09-21

### Added

- New codebase for CDC Aggregator OAI-PMH Repo Handler.
- HTTP server providing an OAI-PMH aggregator endpoint serving
  DocStore records.
