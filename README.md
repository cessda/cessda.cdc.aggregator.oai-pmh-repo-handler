# CESSDA CDC Aggregator - OAI-PMH Repo Handler #

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
records. This program is part of CESSDA CDC Aggregator.


## Installation ##

```sh
python3 -m venv cdcagg-env
source cdcagg-env/bin/activate
cd cessda.cdc.aggregator.oai-pmh-repo-handler
pip install -r requirements.txt
pip install .
```


## Run ##

Replace <docstore-url> with an URL pointing to a DocStore
server. Replace <base-url> with your endpoint OAI-PMH Base
URL. Replace <admin-email> with administrator email address.

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
