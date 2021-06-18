import os.path
from kuha_common.query import QueryController
from kuha_oai_pmh_repo_handler.metadataformats import (
    MetadataFormatBase,
    DDICMetadataFormat,
    OAIDataciteMetadataFormat,
    valid_openaire_id_types
)
from kuha_oai_pmh_repo_handler.genshi_loader import GenPlate
from kuha_oai_pmh_repo_handler.constants import TEMPLATE_FOLDER
from kuha_oai_pmh_repo_handler.oai.constants import OAI_RESPONSE_LIST_SIZE
from cdcagg_common.records import Study

# Prototyping set with aggregate source.
_MAP_URL_TO_SOURCE = {'http://services.fsd.tuni.fi/v0/oai': 'FSD',
                      'https://www.da-ra.de/oaip': 'GESIS'}


# OAI-set source


async def _query_source_for_set(md, spec, correlation_id_header, on_set_cb):
    # Prototyping set with aggregate source.
    result = await QueryController().query_distinct(
        md.study_class, headers=md._corr_id_header,
        fieldname=md.study_class._provenance.attr_base_url,
        _filter={md.study_class._provenance.attr_direct: True})
    await on_set_cb(spec, name='Source archive')
    for baseurl in result[md.study_class._provenance.attr_base_url.path]:
        await on_set_cb('%s:%s' % (spec, _MAP_URL_TO_SOURCE.get(baseurl, baseurl)))


async def _get_source_from_record(md, study):
    # Prototyping set with aggregate source.
    sources = []
    for prov in study._provenance:
        if prov.attr_direct.get_value() is not True or prov.attr_base_url.get_value() is None:
            continue
        source = _MAP_URL_TO_SOURCE.get(prov.attr_base_url.get_value(),
                                        prov.attr_base_url.get_value())
        if source not in sources:
            sources.append(source)
    return sources


async def _filter_for_source(md, value):
    value = {v: k for k, v in _MAP_URL_TO_SOURCE.items()}[value]
    # TODO direct attibute must be true
    return {md.study_class._provenance.attr_base_url: value}


async def _fields_for_source(md):
    return [md.study_class._provenance]


class AggMetadataFormatBase(MetadataFormatBase):

    default_template_folders = MetadataFormatBase.default_template_folders + [
        os.path.join(os.path.dirname(os.path.realpath(__file__)), TEMPLATE_FOLDER)]
    study_class = Study
    sets = [MetadataFormatBase.get_set('language'),
            MetadataFormatBase.get_set('openaire_data'),
            MetadataFormatBase.MDSet(spec='source',
                                     fields=_fields_for_source,
                                     get=_get_source_from_record,
                                     query=_query_source_for_set,
                                     filter_=_filter_for_source)]

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
