# CESSDA Metadata Aggregator - OAI-PMH Repo Handler #

[![SQAaaS badge](https://github.com/EOSC-synergy/SQAaaS/raw/master/badges/badges_150x116/badge_software_silver.png)](https://api.eu.badgr.io/public/assertions/l65LikWrSn2hUiZuE7BVhQ "SQAaaS silver badge achieved")

[![SQAaaS badge shields.io](https://img.shields.io/badge/sqaaas%20software-silver-lightgrey)](https://api.eu.badgr.io/public/assertions/l65LikWrSn2hUiZuE7BVhQ "SQAaaS silver badge achieved")


[![Build Status](https://jenkins.cessda.eu/buildStatus/icon?job=cessda.cdc.aggregator.oai-pmh-repo-handler%2Fmaster)](https://jenkins.cessda.eu/job/cessda.cdc.aggregator.oai-pmh-repo-handler/job/master/)
[![Bugs](https://sonarqube.cessda.eu/api/project_badges/measure?project=cessda.cdc.aggregator.oai-pmh-repo-handler&metric=bugs)](https://sonarqube.cessda.eu/dashboard?id=cessda.cdc.aggregator.oai-pmh-repo-handler)
[![Code Smells](https://sonarqube.cessda.eu/api/project_badges/measure?project=cessda.cdc.aggregator.oai-pmh-repo-handler&metric=code_smells)](https://sonarqube.cessda.eu/dashboard?id=cessda.cdc.aggregator.oai-pmh-repo-handler)
[![Coverage](https://sonarqube.cessda.eu/api/project_badges/measure?project=cessda.cdc.aggregator.oai-pmh-repo-handler&metric=coverage)](https://sonarqube.cessda.eu/dashboard?id=cessda.cdc.aggregator.oai-pmh-repo-handler)
[![Duplicated Lines (%)](https://sonarqube.cessda.eu/api/project_badges/measure?project=cessda.cdc.aggregator.oai-pmh-repo-handler&metric=duplicated_lines_density)](https://sonarqube.cessda.eu/dashboard?id=cessda.cdc.aggregator.oai-pmh-repo-handler)
[![Lines of Code](https://sonarqube.cessda.eu/api/project_badges/measure?project=cessda.cdc.aggregator.oai-pmh-repo-handler&metric=ncloc)](https://sonarqube.cessda.eu/dashboard?id=cessda.cdc.aggregator.oai-pmh-repo-handler)
[![Maintainability Rating](https://sonarqube.cessda.eu/api/project_badges/measure?project=cessda.cdc.aggregator.oai-pmh-repo-handler&metric=sqale_rating)](https://sonarqube.cessda.eu/dashboard?id=cessda.cdc.aggregator.oai-pmh-repo-handler)
[![Quality Gate Status](https://sonarqube.cessda.eu/api/project_badges/measure?project=cessda.cdc.aggregator.oai-pmh-repo-handler&metric=alert_status)](https://sonarqube.cessda.eu/dashboard?id=cessda.cdc.aggregator.oai-pmh-repo-handler)
[![Reliability Rating](https://sonarqube.cessda.eu/api/project_badges/measure?project=cessda.cdc.aggregator.oai-pmh-repo-handler&metric=reliability_rating)](https://sonarqube.cessda.eu/dashboard?id=cessda.cdc.aggregator.oai-pmh-repo-handler)
[![Security Rating](https://sonarqube.cessda.eu/api/project_badges/measure?project=cessda.cdc.aggregator.oai-pmh-repo-handler&metric=security_rating)](https://sonarqube.cessda.eu/dashboard?id=cessda.cdc.aggregator.oai-pmh-repo-handler)
[![Technical Debt](https://sonarqube.cessda.eu/api/project_badges/measure?project=cessda.cdc.aggregator.oai-pmh-repo-handler&metric=sqale_index)](https://sonarqube.cessda.eu/dashboard?id=cessda.cdc.aggregator.oai-pmh-repo-handler)
[![Vulnerabilities](https://sonarqube.cessda.eu/api/project_badges/measure?project=cessda.cdc.aggregator.oai-pmh-repo-handler&metric=vulnerabilities)](https://sonarqube.cessda.eu/dashboard?id=cessda.cdc.aggregator.oai-pmh-repo-handler)

HTTP server providing an OAI-PMH aggregator endpoint serving DocStore
records. This program is part of CESSDA Metadata Aggregator.

Source code is hosted at Github [https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler](https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler).

## Features ##

The OAI-PMH Repo Handler implements an OAI-PMH Aggregator service. The
aggregator provides an OAI-PMH endpoint which enables tracing of
record origin using OAI-PMH provenance containers. The OAI-PMH
specification is publicly available at
[http://www.openarchives.org/OAI/2.0/openarchivesprotocol.htm](http://www.openarchives.org/OAI/2.0/openarchivesprotocol.htm)
and provenance containers are described at
[http://www.openarchives.org/OAI/2.0/guidelines-provenance.htm](http://www.openarchives.org/OAI/2.0/guidelines-provenance.htm).
The aggregator adheres to the implementation Guidelines for Aggregators,
Caches and Proxies, which is available at
[http://www.openarchives.org/OAI/2.0/guidelines-aggregator.htm](http://www.openarchives.org/OAI/2.0/guidelines-aggregator.htm).

The aggregator implements the following OAI-PMH features:

  - All six OAI-PMH verbs of OAI-PMH protocol 2.0.
  - ResumptionTokens to partition large list responses.
  - Selective harvesting via OAI-sets and datestamps.
  - Configurable support for deleted records.
  - Configurable support for OAI-identifiers.
  - Configurable support for arbitrary OAI-sets.
  - Built-in OAI set for grouping by study language.
  - Built-in OAI set for grouping by OpenAIRE data.

The following metadata formats are supported:

  - OAI-DC using metadataprefix ``oai_dc``.
  - DDI 2.5 using metadataprefix ``oai_ddi25``.
  - OpenAIRE Datacite using metadataprefix ``oai_datacite``.

The application exposes a /metrics endpoint, which provides certain
statistics about the running instance of the application. This
endpoint is provided by
[prometheus-client](https://github.com/prometheus/client_python). The following
metrics are exposed:

| Metric                              | Type    | Explanation                                                                                 |
| ----------------------------------- | ------- | ------------------------------------------------------------------------------------------- |
| `requests_total`                    | Counter | Total number of requests received                                                           |
| `requests_per_user_agent_total`     | Counter | Number of requests received per user-agent                                                  |
| `requests_succeeded_total`          | Counter | Number of successful requests                                                               |
| `requests_failed_total`             | Counter | Number of failed requests                                                                   |
| `requests_duration`                 | Summary | Response time in milliseconds                                                               |
| `records_total`                     | Gauge   | Total number of OAI-PMH records (includes records marked as deleted)                        |
| `records_total_without_deleted`     | Gauge   | Total number of OAI-PMH records (excludes records marked as deleted)                        |
| `publishers_total`                  | Gauge   | Total number of distinct publishers (defined by the repository's declared OAI-PMH base URL) |
| `publishers_counts`                 | Gauge   | Number of OAI-PMH records per publisher (includes records marked as deleted)                |
| `publishers_counts_without_deleted` | Gauge   | Number of OAI-PMH records per publisher (excludes records marked as deleted)                |


## Requirements ##

  - Python 3.8 or newer.
  - Running CESSDA Metadata Aggregator DocStore instance.


## Installation ##

On Ubuntu 20.04


### Get Package ###

Clone the repository using Git.

```sh
git clone https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler.git
```

Or fetch a specific release using a tag. For example to get 0.2.0 release.

```sh
git clone --branch 0.2.0 https://github.com/cessda/cessda.cdc.aggregator.oai-pmh-repo-handler.git
```


### Install OAI-PMH Repo Handler ###

It is recommended to install packages inside Python virtual
environment to isolate the install. This package also provides a
Dockerfile to help setup a containerized environment.

Create the Python virtual environment and activate it.

```sh
python3 -m venv cdcagg-env
source cdcagg-env/bin/activate
```

Install Python packages.

```sh
cd cessda.cdc.aggregator.oai-pmh-repo-handler
pip install -r requirements.txt
pip install .
```

To upgrade existing install, use ``--upgrade`` flag in pip commands. Pip
uses ``only-if-needed`` upgrade strategy by default since version
10.0.0, but for backwards compatibility the option is also included in
the example.

```sh
pip install --upgrade -r requirements.txt --upgrade-strategy=only-if-needed
pip install . --upgrade --upgrade-strategy=only-if-needed
```


## Run ##

Replace ``<docstore-url>`` with an URL pointing to a DocStore
server. Replace ``<base-url>`` with your endpoint OAI-PMH Base
URL. Replace ``<admin-email>`` with administrator email address.

```sh
python -m cdcagg_oai --document-store-url <docstore-url> --oai-pmh-base-url <base-url> --oai-pmh-admin-email <admin-email>
```


## Configuration reference ##

To list all available configuration options, use ``--help``.

```sh
python -m cdcagg_oai --help
```

Note that most configuration options can be specified via command line
arguments, configuration file options and environment variables.

[Prometheus client](https://github.com/prometheus/client_python)
provides additional configuration options that can be set using environment variables:

- `PROMETHEUS_DISABLE_CREATED_SERIES` for disabling series suffixed by `_created`.
- `PROMETHEUS_MULTIPROC_DIR` for storing metrics when running in multiprocess mode.

Refer to Prometheus client documentation for more information.


## Build OAI sets based on source endpoint ##

The aggregator provides a way to define OAI sets which group records by
the source OAI-PMH endpoint. This functionality relies on a mapping
file which maps the source OAI-PMH endpoint base-url value to a
OAI-PMH setspec value. In order to use the mapping file, its filepath
must given to the program via configuration and the program must be
able to read the file.

See [example of a mapping file](sources_set.yaml.example) for syntax
reference. The mapping file is expected to be valid YAML.

The value that corresponds with the ``url`` in the mapping file is
used to query the Document Store. Results are grouped using a setspec
value ``source:<source-key-value>``, where <source-key-value>
corresponds to the value of ``source`` in the mapping file.

For example, if the mapping file has the following definition

    -
      url: 'archive.org'
      source: 'archive'
      setname: 'Some archive'
      description: 'Describe some archive'


then all records that are harvested from archive.org are grouped in setspec
``source:archive``.

Values for ``setname`` and ``description`` are used in
ListSets-response to describe the set contents.

When the mapping file is defined, the OAI-PMH Repo Handler must be
configured using configuration option
``--oai-set-source-path <mapping-file-path>``.


## Build arbitrary OAI sets ##

Arbitrary sets can be built using configurable sets -functionality.

Records can be grouped into arbitrary sets by creating a mapping file
which defines OAI set properties and record identifiers belonging to
the defined set. The record identifiers correspond to Study records
``_aggregator_identifier`` values, which are the same values that are
used as default OAI-identifiers.

The set builder supports a single top-level ``spec`` value with
multiple second-level ``spec`` values. Second-level ``spec`` values are always
prepended with the top-level ``spec`` value
``<setSpec>top-level:second-level</setSpec>``. The top-level setspec
contains records matching all ``identifiers`` defined in second-level set
definitions.

See [example of a mapping file](configurable_set.yaml.example) for
syntax reference. The mapping file is expected to be valid YAML.

A single ``spec`` must be found from top-level. The ``spec`` value is used
as a top-level OAI setspec value and identifies that this
setspec-value gets intepreted as a configurable OAI-set. The ``nodes``
contain a list of second-level set definitions. The second-level
``spec`` values must be unique and the list item must contain list of
``identifiers`` that belong to that particular OAI set.

For example, if the mapping file has the following definition

    spec: 'thematic'
    name: 'Thematic'
    description: 'Thematic grouping of records'
    nodes:
      - spec: 'social_sciences'
        name: 'Social sciences'
        description: 'Studies in social sciences'
        identifiers:
        - id_1
        - id_2
      - spec: 'humanities'
        name: 'Humanities'
        description: 'Studies in humanities'
        identifiers:
        - id_2
        - id_3
        - id_4

then ``thematic`` is the top-level setspec node. It contains two child
nodes: ``social_sciences`` and ``humanities``. ListRecords-request
with ``spec=thematic`` will return records from all its second-level
nodes. ListRecords-request with ``spec=thematic:social_sciences`` will
return records with ``_aggregator_identifiers`` values ``id_1`` and
``id_2``. The record with identifier ``id_2`` belongs to both
second-level setspec nodes.

Only a single top-level node is supported. It must contain at least
one second-level child node.

Instead of specifying set definitions directly, the second level node
may alternatively specify a ``path`` which points to an absolute path
of an external mapping file that contains second-level set
definitions.

The external configuration must specify ``spec``, ``name`` and
``identifiers`` keys and may have an optional ``description`` key. The
external configuration file can specify a single node or multiple
nodes in a list.

Main configuration file with path

    spec: 'thematic'
    name: 'Thematic'
    description: 'Thematic grouping of records'
    nodes:
      - path: '/absolute/path/to/ext/conf.yaml'

External configuration file with a single node

    spec: 'history'
    name: 'History'
    description: 'Studies in history'
    identifiers:
    - id_5
    - id_6

External configuration file with a list of nodes

    - spec: 'history'
      name: 'History'
      description: 'Studies in history'
      identifiers:
      - id_5
      - id_6
    - spec: 'literature'
      name: 'Literature'
      description: 'Literature Studies'
      identifiers:
      - id_7
      - id_8

The external configuration cannot further refer to an external
configuration file.

The mapping file syntax is validated on server startup. The file is
not loaded in-memory, but always read on-demand. Exceptions may occur
after server startup, if the file is changed after initial syntax
check.

When the mapping file is defined, the OAI-PMH Repo Handler must be
configured using configuration option
``--oai-set-configurable-path <mapping-file-path>``


## License ##

See the [LICENSE](LICENSE.txt) file.
