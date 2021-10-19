# Copyright CESSDA ERIC 2021
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

import os.path
import datetime
from argparse import Namespace
from xml.etree import ElementTree
from inspect import iscoroutinefunction
from unittest import mock
from tornado.testing import AsyncHTTPTestCase
from kuha_common.testing import mock_coro
from kuha_common.testing.testcases import KuhaUnitTestCase
from kuha_common.document_store.constants import REC_STATUS_DELETED
from kuha_common.document_store import (
    query,
    client
)
from kuha_oai_pmh_repo_handler.oai.constants import (
    OAI_REC_NAMESPACE_IDENTIFIER,
    OAI_RESPONSE_LIST_SIZE,
    OAI_PROTOCOL_VERSION,
    OAI_RESPOND_WITH_REQ_URL,
    OAI_REPO_NAME
)
from kuha_oai_pmh_repo_handler import metadataformats as kuha_metadataformats
from cdcagg_common.records import Study
from cdcagg_oai import (
    serve,
    metadataformats
)

API_VERSION = 'v0'
OAI_URL = '/' + API_VERSION + '/oai'
XMLNS = {'oai': 'http://www.openarchives.org/OAI/2.0/',
         'oai_p': 'http://www.openarchives.org/OAI/2.0/provenance'}
MD_PREFIXES = ('oai_dc', 'oai_ddi25', 'oai_datacite')


class TestConfigure(KuhaUnitTestCase):

    @mock.patch.object(serve, 'conf')
    @mock.patch.object(serve.server, 'add_cli_args')
    @mock.patch.object(serve.controller, 'add_cli_args')
    @mock.patch.object(serve, 'setup_app_logging')
    def test_calls_conf_load(self, mock_setup_app_logging,
                             mock_server_add_cli_args,
                             mock_controller_add_cli_args,
                             mock_conf):
        serve.configure([])
        mock_conf.load.assert_called_once_with(
            prog='cdcagg_oai', package='cdcagg_oai', env_var_prefix='CDCAGG_')


def _query_single(result):
    async def _inner_query_single(record, on_record, **_discard):
        await on_record(result)
    return _inner_query_single


def _query_multiple(result):
    async def _inner_query_multiple(record, on_record, **_discard):
        is_coro = iscoroutinefunction(on_record)
        for rec in result[record.get_collection()]:
            if is_coro:
                await on_record(rec)
            else:
                on_record(rec)
    return _inner_query_multiple


class FakeDatetime(datetime.datetime):
    """Class datetime.datetime methods cannot be mocked
    without touching the container class. We just need the date
    to stay consistent while testing.
    """

    @classmethod
    def utcnow(cls):
        return cls(2019, 12, 12, 7, 14, 37, 685563)


class _Base(AsyncHTTPTestCase):

    _settings = None

    @classmethod
    def settings(cls, **kw):
        if cls._settings is not None:
            raise ValueError("_settings is already defined.")
        cls._settings = Namespace(
            document_store_url=kw.get('document_store_url',
                                      query.DEFAULT_DOCSTORE_URL),
            document_store_client_max_clients=kw.get('document_store_max_clients',
                                                     client.DS_CLIENT_MAX_CLIENTS),
            document_store_client_connect_timeout=kw.get('document_store_client_connect_timeout',
                                                         client.DS_CLIENT_CONNECT_TIMEOUT),
            document_store_client_request_timeout=kw.get('document_store_client_request_timeout',
                                                         client.DS_CLIENT_REQUEST_TIMEOUT),
            oai_pmh_respond_with_requested_url=kw.get('oai_pmh_respond_with_requested_url',
                                                      OAI_RESPOND_WITH_REQ_URL),
            oai_pmh_repo_name=kw.get('oai_pmh_repo_name',
                                     OAI_REPO_NAME),
            oai_pmh_protocol_version=kw.get('oai_pmh_protocol_version',
                                            OAI_PROTOCOL_VERSION),
            oai_pmh_list_size_oai_dc=kw.get('oai_pmh_list_size_oai_dc',
                                            OAI_RESPONSE_LIST_SIZE),
            oai_pmh_list_size_oai_ddi25=kw.get('oai_pmh_list_size_oai_ddi25',
                                               OAI_RESPONSE_LIST_SIZE),
            oai_pmh_list_size_oai_datacite=kw.get('oai_pmh_list_size_oai_datacite',
                                                  OAI_RESPONSE_LIST_SIZE),
            oai_set_sources_path=kw.get('oai_set_sources_path',
                                        os.path.abspath(
                                            os.path.join(
                                                os.path.dirname(os.path.realpath(__file__)),
                                                'data', 'sources_definitions.yaml'))),
            oai_set_configurable_path=kw.get('oai_set_configurable_path',
                                             os.path.abspath(
                                                 os.path.join(
                                                     os.path.dirname(os.path.realpath(__file__)),
                                                     'data', 'configurable_sets.yaml'))),
            oai_pmh_namespace_identifier=kw.get('oai_pmh_namespace_identifier',
                                                OAI_REC_NAMESPACE_IDENTIFIER),
            oai_pmh_deleted_records=kw.get('oai_pmh_deleted_records',
                                           metadataformats.MDFormat._deleted_records_default),
            oai_pmh_base_url=kw.get('oai_pmh_base_url',
                                    'base'),
            oai_pmh_admin_email=kw.get('oai_pmh_admin_email',
                                       'email'),
            template_folder=kw.get(
                'template_folder',
                metadataformats.AggMetadataFormatBase.default_template_folders))
        return cls._settings

    @classmethod
    def _clear_settings(cls):
        cls._settings = None

    def setUp(self):
        self._patchers = []
        super().setUp()

    def tearDown(self):
        for patcher in self._patchers:
            patcher.stop()
        self._clear_settings()
        defaults = self.settings()
        client.configure(defaults)
        query.configure(defaults)
        self._clear_settings()

    def _init_patcher(self, patcher):
        mocked = patcher.start()
        self._patchers.append(patcher)
        return mocked

    def get_app(self):
        if self._settings is None:
            self.settings()
        mdformats = serve.load_metadataformats('cdcagg.oai.metadataformats')
        for mdf in mdformats:
            mdf.configure(self._settings)
        ctrl = serve.controller.from_settings(self._settings, mdformats)
        return serve.http_api.get_app(API_VERSION, controller=ctrl)


class TestHTTPResponses(_Base):

    def setUp(self):
        super().setUp()
        # Mock out query oontroller methods in order to control returned records
        self._mock_query_single = self._init_patcher(mock.patch(
            'kuha_common.query.QueryController.query_single'))
        self._mock_query_multiple = self._init_patcher(mock.patch(
            'kuha_common.query.QueryController.query_multiple'))
        self._mock_query_distinct = self._init_patcher(mock.patch(
            'kuha_common.query.QueryController.query_distinct'))
        self._mock_query_count = self._init_patcher(mock.patch(
            'kuha_common.query.QueryController.query_count'))

    def oai_request(self, return_record=None, return_relatives=None, **req_args):
        return_relatives = return_relatives or {}
        verb = req_args.get('verb', 'GetRecord')
        md_prefix = req_args.get('metadata_prefix', 'oai_dc')
        identifier = req_args.get('identifier', 'some_id')
        self._mock_query_single.side_effect = mock_coro(func=_query_single(return_record))
        self._mock_query_multiple.side_effect = mock_coro(func=_query_multiple(return_relatives))
        self._requested_url = (OAI_URL + '?verb={verb}&metadataPrefix={md_prefix}&'
                               'identifier={id}'.format(
                                   verb=verb, md_prefix=md_prefix, id=identifier))
        return self.fetch(self._requested_url)

    def _assert_oai_header_request_attributes(self, response, expected_attrs, msg=None):
        response_xml = self.resp_to_xmlel(response)
        request_element = response_xml.find('./oai:request', XMLNS)
        self.assertEqual(request_element.attrib, expected_attrs, msg=msg)

    @staticmethod
    def resp_to_xmlel(resp):
        return ElementTree.fromstring(resp.body)

    def test_responds_with_missing_verb(self):
        xml = self.resp_to_xmlel(self.fetch(OAI_URL))
        self.assertEqual(''.join(xml.find('oai:error', XMLNS).itertext()), 'Missing verb')

    # IDENTIFY

    def test_GET_identify_returns_default_deletedRecord(self):
        study = Study()
        self._mock_query_single.side_effect = mock_coro(study)
        resp_xml = self.resp_to_xmlel(self.fetch(OAI_URL + '?verb=Identify'))
        self.assertEqual(''.join(resp_xml.find('./oai:Identify/oai:deletedRecord', XMLNS).itertext()),
                         'transient')

    # GETRECORD

    def test_GET_getrecord_returns_correct_oai_header(self):
        study = Study()
        study.add_study_number('study_id')
        study._provenance.add_value('2020-01-01T23:00.00Z', altered=True, base_url='some.base',
                                    identifier='some:identifier', datestamp='1999-01-01',
                                    direct=True, metadata_namespace='somenamespace')
        for metadata_prefix in MD_PREFIXES:
            with self.subTest(metadata_prefix=metadata_prefix):
                resp = self.oai_request(study, verb='GetRecord', metadata_prefix=metadata_prefix,
                                        identifier='study_id')
                self._assert_oai_header_request_attributes(resp, {'verb': 'GetRecord',
                                                                  'identifier': 'study_id',
                                                                  'metadataPrefix': metadata_prefix})

    def _assert_origindesc(self, origindesc_el, exp_attrs,
                           exp_baseurl, exp_identifier,
                           exp_datestamp, exp_namespace):
        self.assertEqual(origindesc_el.attrib, exp_attrs)
        self.assertEqual(''.join(origindesc_el.find('./oai_p:baseUrl', XMLNS).itertext()),
                         exp_baseurl)
        self.assertEqual(''.join(origindesc_el.find('./oai_p:identifier', XMLNS).itertext()),
                         exp_identifier)
        self.assertEqual(''.join(origindesc_el.find('./oai_p:datestamp', XMLNS).itertext()),
                         exp_datestamp)
        self.assertEqual(''.join(origindesc_el.find('./oai_p:metadataNamespace', XMLNS).itertext()),
                         exp_namespace)

    def test_GET_getrecord_returns_correct_provenance_info(self):
        study = Study()
        study.add_study_number('study_id')
        study._provenance.add_value('2020-01-01T23:00.00Z', altered=True, base_url='some.base',
                                    identifier='some:identifier', datestamp='1999-01-01',
                                    direct=True, metadata_namespace='somenamespace')
        study._provenance.add_value('2019-01-01T23:00.00Z', altered=False, base_url='another.base',
                                    identifier='another:identifier', datestamp='1998-01-01',
                                    direct=False, metadata_namespace='anothernamespace')
        for metadata_prefix in MD_PREFIXES:
            with self.subTest(metadata_prefix=metadata_prefix):
                if metadata_prefix == 'oai_datacite':
                    study.add_identifiers('some_doi', 'en', agency='DOI')
                resp = self.oai_request(study, verb='GetRecord', metadata_prefix=metadata_prefix,
                                        identifier='study_id')
                xmlel = self.resp_to_xmlel(resp)
                origindesc_el = xmlel.find(
                    './oai:GetRecord/oai:record/oai:about/oai_p:provenance/oai_p:originDescription', XMLNS)
                self._assert_origindesc(origindesc_el, {'altered': 'True', 'harvestDate': '2020-01-01T23:00.00Z'},
                                        'some.base', 'some:identifier', '1999-01-01', 'somenamespace')
                # Nested origindesc
                nested_origindesc_el = origindesc_el.find('./oai_p:originDescription', XMLNS)
                self._assert_origindesc(nested_origindesc_el, {'altered': 'False',
                                                               'harvestDate': '2019-01-01T23:00.00Z'},
                                        'another.base', 'another:identifier', '1998-01-01',
                                        'anothernamespace')

    def test_GET_getrecord_returns_correct_xml_if_record_is_deleted(self):
        """Header is correct if a record has been deleted

        If a repository does keep track of deletions then the
        datestamp of the deleted record must be the date and time that
        it was deleted. Responses to GetRecord request for a deleted
        record must then include a header with the attribute
        status="deleted", and must not include metadata or about
        parts.
          - http://www.openarchives.org/OAI/2.0/openarchivesprotocol.htm
        """
        for mdprefix in MD_PREFIXES:
            with self.subTest(metadata_prefix=mdprefix):
                study = Study()
                study.add_study_number('someid')
                study._metadata.attr_status.set_value(REC_STATUS_DELETED)
                study.set_deleted('2000-01-01T23:00:00Z')
                if mdprefix == 'oai_datacite':
                    study.add_identifiers('some_doi', 'en', agency='DOI')
                response_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                                  metadata_prefix=mdprefix,
                                                                  identifier='someid'))
                header_el = response_el.find('./oai:GetRecord/oai:record/oai:header', XMLNS)
                # Datestamp of the deleted record must be the date and time that it was deleted.
                self.assertEqual(''.join(header_el.find('./oai:datestamp', XMLNS).itertext()),
                                 '2000-01-01T23:00:00Z')
                # Responses for a deleted record must include a header with the attribute status="deleted"
                self.assertEqual(header_el.get('status'), 'deleted')
                # ... and must not include metadata
                self.assertIsNone(response_el.find('./oai:GetRecord/oai:record/oai:metadata', XMLNS))
                # ... or about parts
                self.assertIsNone(response_el.find('./oai:GetRecord/oai:record/oai:about', XMLNS))

    def test_GET_getrecord_returns_correct_source_sets(self):
        """Make sure record headers contains correct OAI sets for SourceAggMDSet.
        Consults the HTTP response for XML serialized OAI-sets of a single record."""
        for mdprefix in MD_PREFIXES:
            with self.subTest(metadata_prefix=mdprefix):
                study = Study()
                study.add_study_number('someid')
                study._provenance.add_value('someharvestdate', altered=True,
                                            base_url='http://services.fsd.tuni.fi/v0/oai',
                                            identifier='someidentifier', datestamp='somedatestamp',
                                            direct=True, metadata_namespace='somenamespace')
                exp_sets = ['source:FSD']
                if mdprefix == 'oai_datacite':
                    study.add_identifiers('some_doi', 'en', agency='DOI')
                    exp_sets.append('openaire_data')
                response_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                                  metadata_prefix=mdprefix,
                                                                  identifier='someid'))
                header_el = response_el.find('./oai:GetRecord/oai:record/oai:header', XMLNS)
                set_els = header_el.findall('./oai:setSpec', XMLNS)
                self.assertEqual(len(set_els), len(exp_sets))
                for set_el in set_els:
                    self.assertIn(''.join(set_el.itertext()), exp_sets)

    def test_GET_getrecord_returns_correct_configurable_sets(self):
        """Make sure record headers contains correct OAI sets for ConfigurableAggMDSet.
        Consults the HTTP response for XML serialized OAI-sets of a single record."""
        for mdprefix in MD_PREFIXES:
            with self.subTest(metadata_prefix=mdprefix):
                study = Study()
                study.add_study_number('some_number')
                study._provenance.add_value('someharvestdate', altered=True,
                                            base_url='http://somebaseurl',
                                            identifier='someidentifier', datestamp='somedatestamp',
                                            direct=True, metadata_namespace='somenamespace')
                # 'id_2' is defined in the set configuration file.
                study._aggregator_identifier.add_value('id_1')
                exp_sets = ['thematic:social_sciences']
                if mdprefix == 'oai_datacite':
                    study.add_identifiers('some_doi', 'en', agency='DOI')
                    exp_sets.append('openaire_data')
                resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                              metadata_prefix=mdprefix,
                                                              identifier='someid'))
                header_el = resp_el.find('./oai:GetRecord/oai:record/oai:header', XMLNS)
                set_els = header_el.findall('./oai:setSpec', XMLNS)
                self.assertEqual(len(set_els), len(exp_sets))
                for set_el in set_els:
                    self.assertIn(''.join(set_el.itertext()), exp_sets)

    # LISTSETS

    def test_GET_listsets_returns_correct_sets(self):
        async def _query_distinct(record, headers=None, fieldname=None, _filter=None):
            return {fieldname.path: {
                'study_titles.language': ['fi', 'en'],
                '_provenance.base_url': ['some.base.url', 'http://services.fsd.tuni.fi/v0/oai']
            }}[fieldname.path]
        self._mock_query_distinct.side_effect = mock_coro(func=_query_distinct)
        resp = self.fetch(OAI_URL + '?verb=ListSets')
        xml_el = self.resp_to_xmlel(resp)
        set_els = xml_el.findall('./oai:ListSets/oai:set', XMLNS)
        exp_sets = {'language': ('Language', None),
                    'language:fi': ('', None),
                    'language:en': ('', None),
                    'source': ('Source archive', None),
                    'source:some.base.url': ('', None),
                    'source:FSD': ('', None),
                    'openaire_data': ('OpenAIRE', None),
                    'thematic': ('Thematic', 'Thematic grouping of records'),
                    'thematic:social_sciences': ('Social sciences', 'Studies in social sciences'),
                    'thematic:humanities': ('Humanities', 'Studies in humanities')}
        self.assertEqual(len(set_els), len(exp_sets))
        for set_el in set_els:
            spec = ''.join(set_el.find('./oai:setSpec', XMLNS).itertext())
            name = ''.join(set_el.find('./oai:setName', XMLNS).itertext())
            self.assertIn(spec, exp_sets)
            exp_name, exp_desc = exp_sets.pop(spec)
            self.assertEqual(name, exp_name)
            desc_el = set_el.find('./oai:setDescription', XMLNS)
            desc = ''.join(desc_el.itertext()) if desc_el is not None else None
            self.assertEqual(desc, exp_desc)
        self.assertEqual(exp_sets, {})

    # LISTMETADATAFORMATS

    def test_GET_listmetadataformats_returns_available_metadataformats(self):
        expected = {
            'oai_dc': (
                'http://www.openarchives.org/OAI/2.0/oai_dc.xsd',
                'http://www.openarchives.org/OAI/2.0/oai_dc/'),
            'oai_ddi25': (
                'http://www.ddialliance.org/Specification/DDI-Codebook/2.5/XMLSchema/codebook.xsd',
                'ddi:codebook:2_5'),
            'oai_datacite': (
                'http://schema.datacite.org/meta/kernel-3/metadata.xsd',
                'http://datacite.org/schema/kernel-3')}
        self.assertEqual(list(MD_PREFIXES), list(expected.keys()), msg="Inconsistency in expected metadata and "
                         "declared metadata prefixes. Test must be fixed.")
        response = self.fetch(OAI_URL + '?verb=ListMetadataFormats')
        xmlel = self.resp_to_xmlel(response)
        for mdel in xmlel.findall('./oai:ListMetadataFormats/oai:metadataFormat', XMLNS):
            mdprefix = ''.join(mdel.find('./oai:metadataPrefix', XMLNS).itertext())
            self.assertIn(mdprefix, expected)
            exp_schema, exp_ns = expected.pop(mdprefix)
            self.assertEqual(''.join(mdel.find('./oai:schema', XMLNS).itertext()),
                             exp_schema)
            self.assertEqual(''.join(mdel.find('./oai:metadataNamespace', XMLNS).itertext()),
                             exp_ns)
        self.assertEqual(expected, {})

    # LISTRECORDS

    def test_GET_listrecords_returns_correct_xml_for_deleted_records(self):
        # Mock & format
        study_1, study_2, study_3 = [Study() for _ in range(3)]
        self._mock_query_count.side_effect = mock_coro(3)
        study_1.add_study_number('study_1')
        study_1._metadata.attr_status.set_value(REC_STATUS_DELETED)
        study_1.set_deleted('2000-01-01T23:24:25Z')
        study_1._provenance.add_value('2020-01-01T23:00.00Z', altered=True, base_url='some.base',
                                      identifier='some:identifier', datestamp='1999-01-01',
                                      direct=True, metadata_namespace='somenamespace')
        study_1._aggregator_identifier.set_value('first_identifier')
        study_2.add_study_number('study_2')
        study_2.set_updated('2001-01-01T23:23:23Z')
        study_2._provenance.add_value('2020-01-01T23:00.00Z', altered=True, base_url='some.base',
                                      identifier='some:identifier', datestamp='1999-01-01',
                                      direct=True, metadata_namespace='somenamespace')
        study_2._aggregator_identifier.set_value('second_identifier')
        study_3._metadata.attr_status.set_value(REC_STATUS_DELETED)
        study_3.set_deleted('2002-01-01T23:24:25Z')
        study_3.add_study_number('study_3')
        study_3._provenance.add_value('2020-01-01T23:00.00Z', altered=True, base_url='some.base',
                                      identifier='some:identifier', datestamp='1999-01-01',
                                      direct=True, metadata_namespace='somenamespace')
        study_3._aggregator_identifier.set_value('third_identifier')
        studies = [study_1, study_2, study_3]
        self._mock_query_multiple.side_effect = mock_coro(func=_query_multiple(
            {'studies': studies,
             'variables': [],
             'questions': []}))
        for mdprefix in MD_PREFIXES:
            with self.subTest(metadata_prefix=mdprefix):
                if mdprefix == 'oai_datacite':
                    for study in studies:
                        study.add_identifiers('some_doi', 'en', agency='DOI')
                exp_headers = {'first_identifier': ('2000-01-01T23:24:25Z', True),
                               'second_identifier': ('2001-01-01T23:23:23Z', False),
                               'third_identifier': ('2002-01-01T23:24:25Z', True)}
                resp = self.fetch(OAI_URL + '?verb=ListRecords&metadataPrefix={md}'.format(md=mdprefix))
                self.assertEqual(resp.code, 200)
                xml = self.resp_to_xmlel(resp)
                rec_els = xml.findall('./oai:ListRecords/oai:record', XMLNS)
                self.assertEqual(len(rec_els), 3)
                for rec_el in rec_els:
                    identifier = ''.join(rec_el.find('./oai:header/oai:identifier', XMLNS).itertext())
                    self.assertIn(identifier, exp_headers)
                    exp_dt, exp_deleted_status = exp_headers.pop(identifier)
                    self.assertEqual(''.join(rec_el.find('./oai:header/oai:datestamp', XMLNS).itertext()),
                                     exp_dt)
                    if exp_deleted_status:
                        self.assertEqual(rec_el.find('./oai:header', XMLNS).get('status'), 'deleted')
                        self.assertIsNone(rec_el.find('./oai:metadata', XMLNS))
                self.assertEqual(exp_headers, {})


@mock.patch('kuha_oai_pmh_repo_handler.oai.protocol.datetime.datetime',
            FakeDatetime, spec=datetime.datetime)
class TestQueries(_Base):

    maxDiff = None

    def setUp(self):
        super().setUp()
        self._init_patcher(mock.patch('kuha_common.query.QueryController.query_count'))
        self._mock_fetch = self._init_patcher(mock.patch(
            'kuha_common.document_store.client.JSONStreamClient.fetch'))

    def _assert_filter_is(self, exp_filter):
        calls = self._mock_fetch.call_args_list
        self.assertEqual(len(calls), 1)
        cargs, _ = calls.pop()
        self.assertEqual(cargs[2]['_filter'], exp_filter)

    def _listrecords_with_set(self, set_str, exp_filter, assert_func=None):
        assert_func = assert_func or self._assert_filter_is
        for mdprefix in MD_PREFIXES:
            with self.subTest(metadata_prefix=mdprefix):
                if mdprefix == 'oai_datacite':
                    exp_filter.update({'identifiers.agency': {'$in': [
                        'DOI', 'ARK', 'Handle', 'PURL', 'URN', 'URL']}})
                self.fetch(OAI_URL + '?verb=ListRecords&set={set_str}&'
                           'metadataPrefix={md}'.format(md=mdprefix, set_str=set_str))
                assert_func(exp_filter)

    def test_GET_listrecords_query_filter_for_source_set_and_value(self):
        exp_filter = {'_metadata.updated': {'$lt': {'$isodate': '2019-12-12T07:14:38Z'}},
                      "_provenance": {"$elemMatch": {"base_url": "http://services.fsd.tuni.fi/v0/oai",
                                                     "direct": True}}}
        self._listrecords_with_set('source:FSD', exp_filter)

    def test_GET_listrecords_executes_correct_query_for_source_set(self):
        exp_filter = {'_metadata.updated': {'$lt': {'$isodate': '2019-12-12T07:14:38Z'}},
                      '_provenance': {'$elemMatch': {'base_url': {'$exists': True}, 'direct': True}}}
        self._listrecords_with_set('source', exp_filter)

    def _assert_filter_for_configurable(self, exp_filter):
        exp_filter = dict(exp_filter)
        calls = self._mock_fetch.call_args_list
        self.assertEqual(len(calls), 1)
        cargs, _ = calls.pop()
        cfilter = cargs[2]['_filter']
        self.assertCountEqual(list(cfilter.keys()), list(exp_filter.keys()))
        c_agg_ids_filter = cfilter.pop('_aggregator_identifier')
        exp_agg_ids_filter = exp_filter.pop('_aggregator_identifier')
        self.assertEqual(list(c_agg_ids_filter.keys()), list(exp_agg_ids_filter.keys()))
        self.assertCountEqual(c_agg_ids_filter['$in'], exp_agg_ids_filter['$in'])
        self.assertEqual(cfilter, exp_filter)

    def test_GET_listrecords_query_filter_for_configurable_set_and_value(self):
        exp_filter = {'_metadata.updated': {'$lt': {'$isodate': '2019-12-12T07:14:38Z'}},
                      '_aggregator_identifier': {'$in': ['id_1', 'id_2']}}
        self._listrecords_with_set('thematic:social_sciences',
                                   exp_filter,
                                   assert_func=self._assert_filter_for_configurable)

    def test_GET_listrecords_executes_correct_query_for_configurable_set(self):
        exp_filter = {'_metadata.updated': {'$lt': {'$isodate': '2019-12-12T07:14:38Z'}},
                      '_aggregator_identifier': {'$in': ['id_1', 'id_2', 'id_3', 'id_4']}}
        self._listrecords_with_set('thematic',
                                   exp_filter,
                                   assert_func=self._assert_filter_for_configurable)


class TestConfigurations(_Base):

    def setUp(self):
        self._stored = dict(kuha_metadataformats._STORED)
        self.settings(oai_pmh_deleted_records='persistent')
        super().setUp()
        self._mock_query_single = self._init_patcher(mock.patch(
            'kuha_common.query.QueryController.query_single'))

    def tearDown(self):
        kuha_metadataformats._STORED = self._stored
        super().tearDown()

    # IDENTIFY

    def test_GET_identify_returns_changed_deletedRecord(self):
        study = Study()
        self._mock_query_single.side_effect = mock_coro(study)
        resp_xml = TestHTTPResponses.resp_to_xmlel(self.fetch(OAI_URL + '?verb=Identify'))
        self.assertEqual(''.join(resp_xml.find('./oai:Identify/oai:deletedRecord', XMLNS).itertext()),
                         'persistent')
