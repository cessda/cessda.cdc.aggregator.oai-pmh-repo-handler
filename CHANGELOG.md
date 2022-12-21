# Changelog

All notable changes to the CDC Aggregator OAI-PMH Repo Handler will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).


## 0.4.0 - 2022-12-21

### Added

- Add hard-coded `resourceType` to `oai_datacite` serialization which
  always has the value `Dataset`. (Implements
  [#33](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/33))
- Add hard-coded `dc:type` element to `oai_dc` serialization which
  always has the value `Dataset`. (Implements
  [#36](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/36))
- Add XML Stylesheet to make OAI responses more
  human-friendly. (Implements
  [#22](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/22))
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
  [#35](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/35/))

  - Remove invalid xml:lang attributes.
  - Wrap geoLocationPlace inside geoLocation element.


## 0.3.0 - 2022-11-22

### Added

- Grant & funding information to `oai_datacite` and `oai_ddi25`
  metadata. (Implements
  [#34](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/34))
- Related publication identifiers and agencies to `oai_datacite` and
  `oai_ddi25` metadata. (Implements
  [#34](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/34))

### Changed

- Add primary lookup to `oai_datacite` Publisher from
  Study.distributors. The current lookup from Study.publishers will
  remain as a secondary source. (Fixes [#31](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/31))
- Update dependencies:

  - Require Aggregator Shared Library 0.5.0 in requirements.txt and setup.py.
  - Require Kuha OAI-PMH Repo Handler 1.1.0 in requirements.txt and setup.py.
  - Require Kuha Common 2.0.1 in requirements.txt and 2.0.0 or newer in setup.py.
  - Require Tornado 6.2.0 in requirements.txt.
  - Require Genshi 0.7.7 in requirements.txt.

### Fixed

- Change lookup order of preferred PublicationYear value for
  `oai_datacite`. (Fixes [#30](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/30))
- Format PublicationYear value for `oai_datecite` so that it is a
  year, instead of full datestamp. (Fixes [#30](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/30))
- Include mandatory Date property to `oai_datacite`. (Fixes [#29](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/29))


## 0.2.1 - 2022-06-29

### Changed

- Require Aggregator Shared Library 0.3.0 in requirements.txt.
- Require Kuha Common 1.0.0 in requirements.txt.
- Require Kuha OAI-PMH Repo Handler 1.0.1 in requirements.txt.

### Fixed

- Include missing fields to oai_ddi25 metadata: (Fixes
  [#26](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/26))
    - /codeBook/docDscr/citation/titlStmt/titl
    - /codeBook/stdyDscr/citation/holdings/@URI
    - /codeBook/stdyDscr/stdyInfo/sumDscr/dataKind

- Render subject in oai_dc metadata regardless if the study.keyword
  does not have a value. (Fixes
  [#27](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/27))



## 0.2.0 - 2021-12-17
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.5779937.svg)](https://doi.org/10.5281/zenodo.5779937)

### Added

- Mapping file syntax for source-sets (SourceAggMDSet-class) now
  supports setname and description key-value pairs. The setname is
  mandatory while description is optional.
- Mapping file for configurable-sets now supports external mapping
  files via `path` key. (Implements
  [#18](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/18))
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
  [#14](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/14))
- Empty setName elements for language-sets are populated with
  generated values. Key-value pairs for setname are expected to be
  defined for source-sets in configured mapping file. (Fixes
  [#15](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/15))
- Source set no longer falls back to automatically generating sets
  based on source archive's baseUrl. (Fixes
  [#15](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/15))
- deletedRecord declaration is now configurable. (Fixes
  [#16](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/16))
- Provenance container's baseUrl element name should be baseURL. (Fixes
  [#19](https://bitbucket.org/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler/issues/19))


## 0.1.0 - 2021-09-21

### Added

- New codebase for CDC Aggregator OAI-PMH Repo Handler.
- HTTP server providing an OAI-PMH aggregator endpoint serving
  DocStore records.
