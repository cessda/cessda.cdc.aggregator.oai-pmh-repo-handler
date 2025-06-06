# Copyright CESSDA ERIC 2021-2025
#
# Licensed under the EUPL, Version 1.2 (the "License"); you may not
# use this file except in compliance with the License.
# You may obtain a copy of the License at
# https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Define metadataformats and sets of the OAI-PMH Repo Handler."""
# Stdlib
import os
# PyPI
from yaml import safe_load
# Kuha Common
from kuha_common.query import QueryController
# Kuha OAI-PMH
from kuha_oai_pmh_repo_handler.metadataformats import (
    MDFormat,
    DDICMetadataFormat,
    OAIDataciteMetadataFormat
)
from kuha_oai_pmh_repo_handler.metadataformats.const import valid_openaire_id_types
from kuha_oai_pmh_repo_handler.genshi_loader import GenPlate
from kuha_oai_pmh_repo_handler.constants import TEMPLATE_FOLDER
from kuha_oai_pmh_repo_handler.oai.constants import OAI_RESPONSE_LIST_SIZE
# CDCAGG Common
from cdcagg_common.records import Study


class InvalidMappingConfig(Exception):
    """Raised on invalid config/mapping file contents"""


def _is_nonempty_str(value):
    return hasattr(value, 'encode') and len(value) > 0


def _is_nonempty_list(value):
    return hasattr(value, 'sort') and len(value) > 0


def _validate_keys_values(node, path, *keys_funcs):
    keys_funcs = [('spec', _is_nonempty_str),
                  ('name', _is_nonempty_str)] + list(keys_funcs)
    for key, func in keys_funcs:
        if key not in node:
            raise InvalidMappingConfig("Invalid mapping file '%s'. Did not find a '%s' key.'"
                                       % (path, key))
        value = node[key]
        if func(value) is False:
            raise InvalidMappingConfig("Invalid mapping file '%s'. Value '%s' for '%s' is invalid.'"
                                       % (path, value, key))


class ConfigurableAggMDSet(MDFormat.MDSet):
    """Configurable arbitrary OAI set

    Groups records to arbitrary sets.

    The grouping relies on a mapping file that maps OAI setspecs to
    record's aggregator_identifiers. The mapping file is read everytime this class is used
    to query(), get() or filter() records.

    The mapping file is expected to be valid YAML. A single
    spec-key must be found from top-level. The spec value is used as a
    top-level OAI setspec value and identifies this MDSet. The
    nodes-key contains a list of second-level set definitions. The
    second-level spec-values must be unique and contain list of
    identifiers that belong to that particular OAI set.

    Configuration file syntax example::

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

    The example is intepreted in following way:

      * ``thematic`` is the top-level setspec node.
      * The top-level node contains two child nodes: ``social_sciences`` and ``humanities``
      * The set ``thematic:social_sciences`` contains two records identied by ``id_1`` and ``id_2``.
      * The set ``thematic:humanities`` contains three records identified by ``id_2``, ``id_3`` and ``id_4``.
      * The identifier ``id_2`` belongs to two sets.

    Features & limitations:

      * Supports hierachical set of records with a single top-level node. Example setspec: ``top_level_node``
      * Only direct child nodes are supported after the top-level node. Example setspec: ``top_level_node:child_node``
      * The mapping file YAML syntax is checked on configure() and mandatory keys are validated. Since it is not loaded
        to memory, it is possible to modify the file contents while the server is operating. This may lead to
        errors during runtime.
      * The top-level spec node is used to identify this particular MDSet.
        For example if the configuration file declares spec: ``first`` a request with
        OAI setspec value ``first:second`` implies that the correct MDSet class to consult is this one.

    A single node may alternatively specify a ``path`` key that points
    to an absolute path of an external configuration of sets. The
    external configuration must specify ``spec``, ``name`` and
    ``identifiers`` keys and may have an optional ``description`` key. The
    external configuration file can specify a single node or multiple
    nodes in a list.

    Main configuration file with path::

      spec: 'thematic'
      name: 'Thematic'
      description: 'Thematic grouping of records'
      nodes:
        - path: '/absolute/path/to/ext/conf.yaml'

    External configuration file with a single node::

      spec: 'history'
      name: 'History'
      description: 'Studies in history'
      identifiers:
      - id_5
      - id_6

    External configuration file with a list of nodes::

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
    """
    _loaded_filepath = None

    @classmethod
    def add_cli_args(cls, parser):
        """Add command line arguments to argument parser.

        :param parser: Argument parser.
        :type parser: :obj:`configargparse.ArgumentParser`
        """
        parser.add('--oai-set-configurable-path',
                   help='Path to look for configurable OAI set definitions. '
                   'Leave unset to discard configurable set.',
                   env_var='OPRH_OS_CONFIGURABLE_PATH',
                   type=str)

    @classmethod
    def _validate_node(cls, node, path):
        _validate_keys_values(node, path, ('identifiers', _is_nonempty_list))

    @classmethod
    def _validate_config(cls, cnf_path):
        def _load_file(cnf_path):
            with open(cnf_path, 'r') as file_obj:
                return safe_load(file_obj)
        cnf = _load_file(cnf_path)
        _validate_keys_values(cnf, cnf_path, ('nodes', _is_nonempty_list))

        def _iter_node_and_path(cnf):
            for node_or_path in cnf['nodes']:
                if 'path' in node_or_path:
                    ext_node_path = node_or_path['path']
                    ext_node = _load_file(ext_node_path)
                    if hasattr(ext_node, 'items'):
                        ext_node = [ext_node]
                    for node in ext_node:
                        yield (node, ext_node_path)
                    continue
                yield (node_or_path, cnf_path)

        for node, path in _iter_node_and_path(cnf):
            cls._validate_node(node, path)
        return cnf

    @classmethod
    def configure(cls, settings):
        """Configure set with loaded settings. Validate mapping file.

        :param settings: Loaded settings.
        :type settings: :obj:`argparse.Namespace`
        """
        path = settings.oai_set_configurable_path
        if path is None:
            # Don't load this set.
            return False
        # Load to ensure correct yaml syntax.
        cnf = cls._validate_config(path)
        cls.spec = cnf['spec']
        cls._loaded_filepath = path
        return True

    @staticmethod
    async def _load_file(filepath):
        with open(filepath, 'r') as file_obj:
            return safe_load(file_obj)

    @classmethod
    async def _get_config(cls):
        cnf = await cls._load_file(cls._loaded_filepath)
        nodes = []
        for node in cnf.get('nodes', []):
            if 'path' in node:
                ext_cnf = await cls._load_file(node['path'])
                if hasattr(ext_cnf, 'items'):
                    # ext_cnf is a dict
                    ext_cnf = [ext_cnf]
                nodes.extend(ext_cnf)
            else:
                nodes.append(node)
        cnf['nodes'] = nodes
        return cnf

    async def fields(self):
        """Return list of fields to include when querying for record headers.

        This is used when gathering all docstore fields that are needed to
        construct oai headers.

        :returns: list of fields
        :rtype: list
        """
        return [self._mdformat.study_class._aggregator_identifier]

    async def query(self, on_set_cb):
        """Query and add distinct values for setspecs

        This is used when constructing ListSets OAI response.

        :param on_set_cb: Async callback with signature (spec, name=None, description=None)
        :returns: None
        """
        cnf = await self._get_config()
        await on_set_cb(self.spec, name=cnf.get('name'), description=cnf.get('description'))
        for node in cnf.get('nodes', []):
            await on_set_cb(':'.join((self.spec, node['spec'])),
                            name=node.get('name'),
                            description=node.get('description'))

    async def get(self, study):
        """Get values from record used in setspec: ':<value>'.
        A None value will leave out the <value> part: '<key>'

        This is used when constructing setspecs for a specific record.

        :param study: study record to get set values from
        :returns: List of values
        """
        identifier = study._aggregator_identifier.get_value()
        cnf = await self._get_config()
        values = []
        for node in cnf.get('nodes', []):
            if identifier in node.get('identifiers', []):
                values.append(node['spec'])
        return values

    async def filter(self, value):
        """Return a query filter that includes all studies matching 'value'.

        This is used when constructing docstore query that will include all records
        in this OAI-set group. In other words, in selective harvesting.

        :param str or None value: Requested setspec after colon.
        :returns: query filter
        :rtype: dict
        """
        cnf = await self._get_config()
        identifiers = []
        for node in cnf.get('nodes', []):
            if value is None or value == node.get('spec'):
                identifiers.extend(node.get('identifiers', []))
                if value is not None:
                    break
        return {self._mdformat.study_class._aggregator_identifier:
                {QueryController.fk_constants.in_: list(set(identifiers))}}


class SourceAggMDSet(MDFormat.MDSet):
    """OAI set grouping records by their originating source archive

    The grouping relies on a mapping file that maps a record source url (OAI base url)
    to a source value. This file is read once on configure() and kept in-memory for
    the rest of the application run time.

    Mapping file syntax::

      -
        url: 'http://archive_1.url/oai'
        source: 'Archive_1'
        setname: 'a short human-readable string naming the set source:archive_1'
        description: 'an optional long description for the set source:archive_1'
      -
        url: 'http://archive_2.url/oai'
        source: 'Archive_2'
        setname: 'a short human-readable string naming the set source:archive_2'
    """

    spec = 'source'
    # Contains source definitions. Populated once on configure and kept in-memory
    # for the rest of the application run time.
    _source_defs = None

    @classmethod
    def add_cli_args(cls, parser):
        """Add command line arguments to argument parser.

        :param parser: Argument parser.
        :type parser: :obj:`configargparse.ArgumentParser`
        """
        parser.add('--oai-set-sources-path',
                   help='Full path to sources definitions',
                   env_var='OPRH_OS_SOURCES_PATH',
                   type=str)

    @classmethod
    def configure(cls, settings):
        """Configure set with loaded settings. Load mapping file to memory.

        :param settings: Loaded settings.
        :type settings: :obj:`argparse.Namespace`
        """
        path = settings.oai_set_sources_path
        if path is None:
            # Don't load this set.
            return False
        with open(path, 'r') as file_obj:
            cls._source_defs = safe_load(file_obj) or []
        return True

    @classmethod
    async def _get_source_defs(cls):
        return cls._source_defs

    async def _get_definitions_by_url(self, url):
        source_defs = await self._get_source_defs()
        for source_def in source_defs:
            if source_def['url'] == url:
                return (source_def['source'], source_def['setname'], source_def.get('description'))
        return (None, None, None)

    async def _get_url_by_source(self, source):
        source_defs = await self._get_source_defs()
        for source_def in source_defs:
            if source_def['source'] == source:
                return source_def['url']

    async def fields(self):
        """Return list of fields to include when querying for record headers.

        This is used when gathering all docstore fields that are needed to
        construct oai headers.

        :returns: list of fields
        :rtype: list
        """
        return [self._mdformat.study_class._provenance]

    async def query(self, on_set_cb):
        """Query and add distinct values for setspecs

        This is used when constructing ListSets OAI response.

        :param on_set_cb: Async callback with signature (spec, name=None, description=None)
        :returns: None
        """
        result = await QueryController().query_distinct(
            self._mdformat.study_class,
            headers=self._mdformat.corr_id_header,
            fieldname=self._mdformat.study_class._provenance.attr_base_url,
            _filter={self._mdformat.study_class._provenance.attr_direct: True})
        await on_set_cb(self.spec, name='Source archive')
        for baseurl in result[self._mdformat.study_class._provenance.attr_base_url.path]:
            source, setname, description = await self._get_definitions_by_url(baseurl)
            if source is not None:
                await on_set_cb('%s:%s' % (self.spec, source), name=setname, description=description)

    async def get(self, study):
        """Get values from record used in setspec: '<key>:<value>'.
        A None value will leave out the <value> part: '<key>'

        This is used when constructing setspecs for a specific record.

        :param study: study record to get set values from
        :returns: List of values
        """
        sources = []
        for prov in study._provenance:
            if prov.attr_direct.get_value() is not True or prov.attr_base_url.get_value() is None:
                continue
            base_url = prov.attr_base_url.get_value()
            source, _, __ = await self._get_definitions_by_url(base_url)
            if source is not None and source not in sources:
                sources.append(source)
        return sources

    async def filter(self, value):
        """Return a query filter that includes all studies matching 'value'.

        This is used when constructing docstore query that will include all records
        in this OAI-set group. In other words, in selective harvesting.

        :param str or None value: Requested setspec value after colon.
        :returns: query filter
        :rtype: dict
        """
        value = await self._get_url_by_source(value) if value else self._exists_filter
        return {self._mdformat.study_class._provenance:
                {QueryController.fk_constants.elem_match:
                 {self._mdformat.study_class._provenance.attr_base_url: value,
                  self._mdformat.study_class._provenance.attr_direct: True}}}


class AggMetadataFormatBase(MDFormat):
    """Base class for Aggregator metadataformat definitions.

    Subclass of :class:`kuha_oai_pmh_repo_handler.metadataformats.MDFormat`

    Adds a new template folder, which is relative to this package install location.
    The folder is added to the list of lookup folders, so Genshi lookup may search
    for Kuha's templates and Aggregator's templates.

    Overrides parent's :attr:`study_class` with Aggregator's Study record class.

    Overrides parent's :attr:`sets` to keep Kuha's `language` and
    `openaire_data` OAI sets, and to include Aggregator's
    :class:`SourceAggMDSet` and :class:`ConfigurableAggMDSet` OAI
    sets.

    Overrides parent's :meth:`_header_fields` to include
    :attr:`cdcagg_common.records.Study._aggregator_identifier` and
    :attr:`cdcagg_common.records.Study._provenance`.

    Overrides parents :meth:`_get_identifier` to use
    :attr:`cdcagg_common.records.Study._aggregator_identifier` as
    local OAI-identifier.

    Overrides parents :meth:`_valid_record_filter` to use
    :attr:`cdcagg_common.records.Study._aggregator_identifier` for
    record lookup in DocStore query.
    """

    default_template_folders = MDFormat.default_template_folders + [
        os.path.join(os.path.dirname(os.path.realpath(__file__)), TEMPLATE_FOLDER)]
    study_class = Study
    sets = [MDFormat.get_set('language'),
            MDFormat.get_set('openaire_data'),
            SourceAggMDSet,
            ConfigurableAggMDSet]

    async def _header_fields(self):
        return await super()._header_fields() + [self.study_class._aggregator_identifier,
                                                 self.study_class._provenance]

    async def _get_identifier(self, study, **record_objs):
        return study._aggregator_identifier.get_value()

    async def _valid_record_filter(self):
        return {self.study_class._aggregator_identifier: self._oai.arguments.get_local_identifier()}


class AggDCMetadataFormat(AggMetadataFormatBase):
    """Define metadataformat for OAI-DC.

    Subclass of :class:`AggMetadataFormatBase`.

    This metadataformat is available using metadataprefix `oai_dc`
    """

    mdprefix = 'oai_dc'
    mdschema = 'http://www.openarchives.org/OAI/2.0/oai_dc.xsd'
    mdnamespace = 'http://www.openarchives.org/OAI/2.0/oai_dc/'

    @property
    def _record_fields(self):
        return [self.study_class.identifiers,
                self.study_class.principal_investigators,
                self.study_class.publishers,
                self.study_class.document_uris,
                self.study_class.study_uris,
                self.study_class.abstract,
                self.study_class.keywords,
                self.study_class.publication_years,
                self.study_class.study_area_countries,
                self.study_class.data_collection_copyrights]

    @classmethod
    def add_cli_args(cls, parser):
        """Add command line arguments to argument parser.

        :param parser: Argument parser.
        :type parser: :obj:`configargparse.ArgumentParser`
        """
        super().add_cli_args(parser)
        parser.add('--oai-pmh-list-size-oai-dc',
                   help='How many results should a list response contain for '
                   'OAI DC metadata',
                   default=OAI_RESPONSE_LIST_SIZE,
                   env_var='OPRH_OP_LIST_SIZE_OAI_DC',
                   type=int)

    @classmethod
    def configure(cls, settings):
        """Configure metadataformat with loaded settings.

        :param settings: Loaded settings.
        :type settings: :obj:`argparse.Namespace`
        """
        cls.list_size = settings.oai_pmh_list_size_oai_dc
        super().configure(settings)

    @GenPlate('agg_get_record.xml', subtemplate='agg_oai_dc.xml')
    async def get_record(self):
        """Get OAI-DC record and prepare metadata response.

        Defines a genshi template used in response via GenPlate-decorator.

        :returns: Context used in Genshi XML template.
        :rtype: dict
        """
        await self._get_record()
        return await self._metadata_response()

    @GenPlate('agg_list_records.xml', subtemplate='agg_oai_dc.xml')
    async def list_records(self):
        """Get OAI-DC records and prepare metadata response.

        Defines a genshi template used in response serialization via
        GenPlate-decorator.

        :returns: Context used in Genshi XML template.
        :rtype: dict
        """
        await self._list_records()
        return await self._metadata_response()


class AggOAIDDI25MetadataFormat(AggMetadataFormatBase):
    """Define metadataformat for OAI-DDI25.

    Subclass of :class:`AggMetadataFormatBase`.

    This metadataformat is available using metadataprefix `oai_ddi25`
    """

    mdprefix = 'oai_ddi25'
    mdschema = 'http://www.ddialliance.org/Specification/DDI-Codebook/2.5/XMLSchema/codebook.xsd'
    mdnamespace = 'ddi:codebook:2_5'

    @property
    def _record_fields(self):
        return [self.study_class.identifiers,
                self.study_class.document_titles,
                self.study_class.data_kinds,
                self.study_class.study_uris,
                self.study_class.publishers,
                self.study_class.document_uris,
                self.study_class.distributors,
                self.study_class.copyrights,
                self.study_class.parallel_titles,
                self.study_class.principal_investigators,
                self.study_class.publication_dates,
                self.study_class.distribution_dates,
                self.study_class.publication_years,
                self.study_class.keywords,
                self.study_class.time_methods,
                self.study_class.sampling_procedures,
                self.study_class.collection_modes,
                self.study_class.analysis_units,
                self.study_class.research_instruments,
                self.study_class.collection_periods,
                self.study_class.classifications,
                self.study_class.abstract,
                self.study_class.study_area_countries,
                self.study_class.universes,
                self.study_class.data_access,
                self.study_class.data_access_descriptions,
                self.study_class.file_names,
                self.study_class.data_collection_copyrights,
                self.study_class.citation_requirements,
                self.study_class.deposit_requirements,
                self.study_class.geographic_coverages,
                self.study_class.instruments,
                self.study_class.grant_numbers,
                self.study_class.related_publications,
                self.study_class.funding_agencies]

    @classmethod
    def add_cli_args(cls, parser):
        """Add command line arguments to argument parser.

        :param parser: Argument parser.
        :type parser: :obj:`configargparse.ArgumentParser`
        """
        super().add_cli_args(parser)
        parser.add('--oai-pmh-list-size-oai-ddi25',
                   help='How many results should a list response contain for '
                   'OAI DDI25 metadata',
                   default=OAI_RESPONSE_LIST_SIZE,
                   env_var='OPRH_OP_LIST_SIZE_OAI_DDI25',
                   type=int)

    @classmethod
    def configure(cls, settings):
        """Configure metadataformat with loaded settings.

        :param settings: Loaded settings.
        :type settings: :obj:`argparse.Namespace`
        """
        cls.list_size = settings.oai_pmh_list_size_oai_ddi25
        super().configure(settings)

    async def _on_record(self, study):
        """Override _on_record to include iter_relpubls helper
        function in template contexts for each record.

        :param study: Study record
        :type study: :obj:`cdcagg_common.records.Study`
        """
        await super()._on_record(study, iter_relpubls=DDICMetadataFormat.iter_relpubls)

    @GenPlate('agg_get_record.xml', subtemplate='oai_ddi25.xml')
    async def get_record(self):
        """Get OAI-DDI25 record and prepare metadata response.

        Defines a genshi template used in response via GenPlate-decorator.

        :returns: Context used in Genshi XML template.
        :rtype: dict
        """
        await super()._get_record()
        return await super()._metadata_response()

    @GenPlate('agg_list_records.xml', subtemplate='oai_ddi25.xml')
    async def list_records(self):
        """Get OAI-DDI25 records and prepare metadata response.

        Defines a genshi template used in response serialization via
        GenPlate-decorator.

        :returns: Context used in Genshi XML template.
        :rtype: dict
        """
        await super()._list_records()
        return await super()._metadata_response()


class AggOAIDataciteMetadataFormat(AggMetadataFormatBase):
    """Define metadataformat for OAI-Datacite.

    Subclass of :class:`AggMetadataFormatBase`.

    This metadataformat is available using metadataprefix `oai_datacite`
    """

    mdprefix = 'oai_datacite'
    mdschema = 'http://schema.datacite.org/meta/kernel-3/metadata.xsd'
    mdnamespace = 'http://datacite.org/schema/kernel-3'

    @property
    def _record_fields(self):
        return [self.study_class.identifiers,
                self.study_class.principal_investigators,
                self.study_class.distributors,
                self.study_class.publishers,
                self.study_class.publication_years,
                self.study_class.keywords,
                self.study_class.classifications,
                self.study_class.data_access,
                self.study_class.abstract,
                self.study_class.geographic_coverages,
                self.study_class.study_titles,
                self.study_class.related_publications,
                self.study_class.grant_numbers]

    @classmethod
    def add_cli_args(cls, parser):
        """Add command line arguments to argument parser.

        :param parser: Argument parser.
        :type parser: :obj:`configargparse.ArgumentParser`
        """
        super().add_cli_args(parser)
        parser.add('--oai-pmh-list-size-oai-datacite',
                   help='How many results should a list response contain for '
                   'OAI Datacite metadata',
                   default=OAI_RESPONSE_LIST_SIZE,
                   env_var='OPRH_OP_LIST_SIZE_OAI_DATACITE',
                   type=int)

    @classmethod
    def configure(cls, settings):
        """Configure metadataformat with loaded settings.

        :param settings: Loaded settings.
        :type settings: :obj:`argparse.Namespace`
        """
        cls.list_size = settings.oai_pmh_list_size_oai_datacite
        super().configure(settings)

    async def _on_record(self, study):
        """Override _on_record to make additional checks before adding
        the record to response.

        OAI-Datacite requires that a record has a certain type of identifier.
        This function will drop records that do not have such identifier type.

        Also makes sure that the publication year is actually a year
        with four digits.

        :param study: Study record
        :type study: :obj:`cdcagg_common.records.Study`
        """
        preferred_id = await OAIDataciteMetadataFormat.get_preferred_identifier(study)
        if preferred_id != ():
            # Only add records that have some valid id.
            # For GetRecord, this leads to idDoesNotExist
            # For ListRecords & ListIdentifiers this may lead to false record count,
            # however, ListRecords & ListIdentifiers should use _valid_records_filter() to
            # make sure this will never happen.
            publication_year = await OAIDataciteMetadataFormat.get_publication_year(study)
            publisher = await OAIDataciteMetadataFormat.get_publisher_lang_value_pair(study)
            related_identifier_types_ids = await OAIDataciteMetadataFormat.get_related_identifiers_types(study)
            funders = await OAIDataciteMetadataFormat.get_funders(study)
            await super()._on_record(study, preferred_identifier=preferred_id,
                                     publication_year=publication_year,
                                     publisher_lang_val=publisher,
                                     related_identifier_types_ids=related_identifier_types_ids,
                                     funders=funders)

    @GenPlate('agg_get_record.xml', subtemplate='agg_oai_datacite.xml')
    async def get_record(self):
        """Get OAI-Datacite record and prepare metadata response.

        Defines a genshi template used in response via GenPlate-decorator.

        :returns: Context used in Genshi XML template.
        :rtype: dict
        """
        await super()._get_record()
        return await super()._metadata_response()

    @GenPlate('agg_list_records.xml', subtemplate='agg_oai_datacite.xml')
    async def list_records(self):
        """Get OAI-Datacite records and prepare metadata response.

        Defines a genshi template used in response serialization via
        GenPlate-decorator.

        :returns: Context used in Genshi XML template.
        :rtype: dict
        """
        await super()._list_records()
        return await super()._metadata_response()

    async def _valid_records_filter(self):
        """Override _valid_records_filter to add filtering for certain types of identifiers.

        This makes sure that records that do not have a valid OpenAIRE
        ID type are not included in list responses.

        :returns: Filter for valid records.
        :rtype: dict
        """
        _filter = await super()._valid_records_filter()
        _filter.update({
            self.study_class.identifiers.attr_agency: {
                QueryController.fk_constants.in_: list(valid_openaire_id_types)}})
        return _filter
