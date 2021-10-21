# Changelog

All notable changes to the CDC Aggregator OAI-PMH Repo Handler will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security


## 0.2.0 - unreleased

### Added

- Mapping file syntax for source-sets (SourceAggMDSet-class) now
  supports setname and description key-value pairs. The setname is
  mandatory while description is optional.

### Changed

- Mapping file configuration option for SourceAggMDSet
  `--oai-set-sources-path` defaults to None, which implies that the
  set is discarded (not loaded) on server startup. The operator is in
  charge of creating and configuring the mapping file.

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


## 0.1.0 - 2021-09-21

### Added

- New codebase for CDC Aggregator OAI-PMH Repo Handler.
- HTTP server providing an OAI-PMH aggregator endpoint serving
  DocStore records.
