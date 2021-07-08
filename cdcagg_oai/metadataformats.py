# Stdlib
import os.path
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


class SourceAggMDSet(MDFormat.MDSet):

    spec = 'source'
    _default_filepath = os.path.abspath(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            '..', 'sources_default.yaml'))
    # Contains source definitions. Populated once on configure and kept in-memory
    # for the rest of the application run time.
    _source_defs = None

    @classmethod
    def add_cli_args(cls, parser):
        parser.add('--oai-set-sources-path',
                   help='Full path to sources definitions',
                   env_var='OPRH_OS_SOURCES_PATH',
                   default=cls._default_filepath,
                   type=str)

    @classmethod
    def configure(cls, settings):
        path = settings.oai_set_sources_path
        if not os.path.isfile(path):
            raise ValueError("'%s' is not a valid file." % (path,))
        with open(path, 'r') as file_obj:
            cls._source_defs = safe_load(file_obj) or []

    @classmethod
    async def _get_source_defs(cls):
        return cls._source_defs

    async def _get_source_by_url(self, url):
        source_defs = await self._get_source_defs()
        for source_def in source_defs:
            if source_def['url'] == url:
                return source_def['source']

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

        :param on_set_cb: Async callback with signature (spec, name=None), where spec is the setSpec value
                          'language:en' or 'openaire_data'
        :returns: None
        """
        result = await QueryController().query_distinct(
            self._mdformat.study_class,
            headers=self._mdformat.corr_id_header,
            fieldname=self._mdformat.study_class._provenance.attr_base_url,
            _filter={self._mdformat.study_class._provenance.attr_direct: True})
        await on_set_cb(self.spec, name='Source archive')
        for baseurl in result[self._mdformat.study_class._provenance.attr_base_url.path]:
            source = await self._get_source_by_url(baseurl) or baseurl
            await on_set_cb('%s:%s' % (self.spec, source))

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
            source = await self._get_source_by_url(base_url) or base_url
            if source not in sources:
                sources.append(source)
        return sources

    async def filter(self, value):
        """Return a query filter that includes all studies matching 'value'.

        This is used when constructing docstore query that will include all records
        in this OAI-set group. In other words, in selective harvesting.

        :param dict value: filter value
        :returns: query filter
        :rtype: dict
        """
        value = await self._get_url_by_source(value) or value
        return {self._mdformat.study_class._provenance:
                {QueryController.fk_constants.elem_match:
                 {self._mdformat.study_class._provenance.attr_base_url: value,
                  self._mdformat.study_class._provenance.attr_direct: True}}}


class AggMetadataFormatBase(MDFormat):

    default_template_folders = MDFormat.default_template_folders + [
        os.path.join(os.path.dirname(os.path.realpath(__file__)), TEMPLATE_FOLDER)]
    study_class = Study
    sets = [MDFormat.get_set('language'),
            MDFormat.get_set('openaire_data'),
            SourceAggMDSet]

    async def _header_fields(self):
        return await super()._header_fields() + [self.study_class._aggregator_identifier,
                                                 self.study_class._provenance]

    async def _get_identifier(self, study, **record_objs):
        return study._aggregator_identifier.get_value()

    async def _valid_record_filter(self):
        return {self.study_class._aggregator_identifier: self._oai.arguments.get_local_identifier()}


class AggDCMetadataFormat(AggMetadataFormatBase):

    mdprefix = 'oai_dc'
    mdschema = 'http://www.openarchives.org/OAI/2.0/oai_dc.xsd'
    mdnamespace = 'http://www.openarchives.org/OAI/2.0/oai_dc/'

    @property
    def _record_fields(self):
        return [self.study_class.identifiers,
                self.study_class.principal_investigators,
                self.study_class.publishers,
                self.study_class.document_uris,
                self.study_class.abstract,
                self.study_class.keywords,
                self.study_class.publication_years,
                self.study_class.study_area_countries,
                self.study_class.data_collection_copyrights]

    @classmethod
    def add_cli_args(cls, parser):
        super().add_cli_args(parser)
        parser.add('--oai-pmh-list-size-oai-dc',
                   help='How many results should a list response contain for '
                   'OAI DC metadata',
                   default=OAI_RESPONSE_LIST_SIZE,
                   env_var='OPRH_OP_LIST_SIZE_OAI_DC',
                   type=int)

    @classmethod
    def configure(cls, settings):
        cls.list_size = settings.oai_pmh_list_size_oai_dc
        super().configure(settings)

    @GenPlate('agg_get_record.xml', subtemplate='oai_dc.xml')
    async def get_record(self):
        await self._get_record()
        return await self._metadata_response()

    @GenPlate('agg_list_records.xml', subtemplate='oai_dc.xml')
    async def list_records(self):
        await self._list_records()
        return await self._metadata_response()


class AggOAIDDI25MetadataFormat(AggMetadataFormatBase):

    mdprefix = 'oai_ddi25'
    mdschema = 'http://www.ddialliance.org/Specification/DDI-Codebook/2.5/XMLSchema/codebook.xsd'
    mdnamespace = 'ddi:codebook:2_5'

    @property
    def _record_fields(self):
        return [self.study_class.identifiers,
                self.study_class.publishers,
                self.study_class.document_uris,
                self.study_class.distributors,
                self.study_class.copyrights,
                self.study_class.parallel_titles,
                self.study_class.principal_investigators,
                self.study_class.publication_dates,
                self.study_class.publication_years,
                self.study_class.keywords,
                self.study_class.time_methods,
                self.study_class.sampling_procedures,
                self.study_class.collection_modes,
                self.study_class.analysis_units,
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
                self.study_class.related_publications]

    @classmethod
    def add_cli_args(cls, parser):
        super().add_cli_args(parser)
        parser.add('--oai-pmh-list-size-oai-ddi25',
                   help='How many results should a list response contain for '
                   'OAI DDI25 metadata',
                   default=OAI_RESPONSE_LIST_SIZE,
                   env_var='OPRH_OP_LIST_SIZE_OAI_DDI25',
                   type=int)

    @classmethod
    def configure(cls, settings):
        cls.list_size = settings.oai_pmh_list_size_oai_ddi25
        super().configure(settings)

    async def _on_record(self, study):
        await super()._on_record(study, iter_relpubls=DDICMetadataFormat.iter_relpubls)

    @GenPlate('agg_get_record.xml', subtemplate='oai_ddi25.xml')
    async def get_record(self):
        await super()._get_record()
        return await super()._metadata_response()

    @GenPlate('agg_list_records.xml', subtemplate='oai_ddi25.xml')
    async def list_records(self):
        await super()._list_records()
        return await super()._metadata_response()


class AggOAIDataciteMetadataFormat(AggMetadataFormatBase):

    mdprefix = 'oai_datacite'
    mdschema = 'http://schema.datacite.org/meta/kernel-3/metadata.xsd'
    mdnamespace = 'http://datacite.org/schema/kernel-3'

    @property
    def _record_fields(self):
        return [self.study_class.identifiers,
                self.study_class.principal_investigators,
                self.study_class.publishers,
                self.study_class.publication_years,
                self.study_class.keywords,
                self.study_class.classifications,
                self.study_class.data_access,
                self.study_class.abstract,
                self.study_class.geographic_coverages,
                self.study_class.study_titles]

    @classmethod
    def add_cli_args(cls, parser):
        super().add_cli_args(parser)
        parser.add('--oai-pmh-list-size-oai-datacite',
                   help='How many results should a list response contain for '
                   'OAI Datacite metadata',
                   default=OAI_RESPONSE_LIST_SIZE,
                   env_var='OPRH_OP_LIST_SIZE_OAI_DATACITE',
                   type=int)

    @classmethod
    def configure(cls, settings):
        cls.list_size = settings.oai_pmh_list_size_oai_datacite
        super().configure(settings)

    async def _on_record(self, study):
        preferred_id = await OAIDataciteMetadataFormat.get_preferred_identifier(study)
        if preferred_id != ():
            # Only add records that have some valid id.
            # For GetRecord, this leads to idDoesNotExist
            # For ListRecords & ListIdentifiers this may lead to false record count,
            # however, ListRecords & ListIdentifiers should use _valid_records_filter() to
            # make sure this will never happen.
            publication_year = await OAIDataciteMetadataFormat.get_publication_year(study)
            await super()._on_record(study, preferred_identifier=preferred_id,
                                     publication_year=publication_year)

    @GenPlate('agg_get_record.xml', subtemplate='oai_datacite.xml')
    async def get_record(self):
        await super()._get_record()
        return await super()._metadata_response()

    @GenPlate('agg_list_records.xml', subtemplate='oai_datacite.xml')
    async def list_records(self):
        await super()._list_records()
        return await super()._metadata_response()

    async def _valid_records_filter(self):
        _filter = await super()._valid_records_filter()
        _filter.update({
            self.study_class.identifiers.attr_agency: {
                QueryController.fk_constants.in_: list(valid_openaire_id_types)}})
        return _filter
