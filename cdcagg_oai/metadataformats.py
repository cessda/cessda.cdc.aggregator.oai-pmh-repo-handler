import os.path
from kuha_common.query import QueryController
from kuha_oai_pmh_repo_handler.metadataformats import (
    MetadataFormatBase,
    DDICMetadataFormat
)
from kuha_oai_pmh_repo_handler.genshi_loader import GenPlate
from kuha_oai_pmh_repo_handler.constants import TEMPLATE_FOLDER
from cdcagg_common.records import Study

# Prototyping set with aggregate source.
_MAP_URL_TO_SOURCE = {'http://services.fsd.tuni.fi/v0/oai': 'FSD',
                      'https://www.da-ra.de/oaip': 'GESIS'}


async def _query_source_for_set(md, spec, correlation_id_header, on_set_cb):
    # Prototyping set with aggregate source.
    result = await QueryController().query_distinct(
        md.study_class, headers=md._corr_id_header,
        fieldname=md.study_class._provenance.attr_base_url,
        _filter={md.study_class._provenance.attr_direct: True})
    await on_set_cb(spec, name='Source archive')
    for baseurl in result[md.study_class._provenance.attr_base_url.path]:
        await on_set_cb('%s:%s' % (spec, _MAP_URL_TO_SOURCE.get(baseurl, baseurl)))


async def _get_source_from_record(study):
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


class AggMetadataFormatBase(MetadataFormatBase):

    _default_template_folders = MetadataFormatBase._default_template_folders + [
        os.path.join(os.path.dirname(os.path.realpath(__file__)), TEMPLATE_FOLDER)]
    study_class = Study
    _sets = [MetadataFormatBase.get_set('language'),
             MetadataFormatBase.MDSet(spec='source',
                                      get=_get_source_from_record,
                                      query=_query_source_for_set,
                                      filter_=_filter_for_source)]

    @property
    def _header_fields(self):
        return super()._header_fields + [self.study_class._aggregator_identifier,
                                         self.study_class._provenance]

    async def _on_record(self, study, **record_obj):
        setspecs = {}
        for set_ in self._sets:
            setspecs.update({set_.spec: await set_.get(study)})
        record_obj['study'] = study
        await self._add_record(study._aggregator_identifier.get_value(),
                               study.get_updated(), record_obj, setspecs)

    async def _has_record(self):
        result = await QueryController().query_single(
            self.study_class, headers=self._corr_id_header,
            _filter={self.study_class._aggregator_identifier:
                     self._oai.arguments.get_local_identifier()},
            fields=self.study_class._id)
        return bool(result)

    async def _get_record(self):
        await QueryController().query_single(
            self.study_class, on_record=self._on_record, headers=self._corr_id_header,
            _filter={self.study_class._aggregator_identifier: self._oai.arguments.get_local_identifier()},
            fields=self._header_fields + self._record_fields)


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
