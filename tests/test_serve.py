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
THE_XMLNS = 'http://www.w3.org/XML/1998/namespace'
MD_PREFIXES = ('oai_dc', 'oai_ddi25', 'oai_datacite')
XML_LANG_ATT = '{%s}lang' % (THE_XMLNS,)
DATACITE_XMLNS = dict(**XMLNS, **{'datacite': 'http://datacite.org/schema/kernel-3',
                                  'xml': THE_XMLNS})
OAIDC_XMLNS = dict(**XMLNS, **{'dc': 'http://purl.org/dc/elements/1.1/',
                               'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/'})


def _get_xmllang(element):
    return element.get(XML_LANG_ATT)


def _study_for_datacite():
    study = Study()
    study.add_study_number('some_number')
    study.add_identifiers('some_id', 'en', agency='DOI')
    study._aggregator_identifier.add_value('agg_id_1')
    study._provenance.add_value('someharvestdate', altered=True,
                                base_url='http://somebaseurl',
                                identifier='someidentifier', datestamp='somedatestamp',
                                direct=True, metadata_namespace='somenamespace')
    return study


def _study_for_oaidc():
    study = Study()
    study.add_study_number('some_number')
    study._aggregator_identifier.add_value('agg_id_1')
    study._provenance.add_value('someharvestdate', altered=True,
                                base_url='http://somebaseurl',
                                identifier='someidentifier', datestamp='somedatestamp',
                                direct=True, metadata_namespace='somenamespace')
    return study


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


@mock.patch.object(serve.server, 'serve')
@mock.patch.object(serve, 'configure')
@mock.patch.object(serve.controller, 'from_settings')
class TestMain(KuhaUnitTestCase):

    @mock.patch.object(serve.http_api, 'get_app')
    def test_calls_http_api_get_app_with_app_class_param(self,
                                                         mock_get_app,
                                                         mock_from_settings,
                                                         mock_configure,
                                                         mock_serve):
        mock_configure.return_value = Namespace(
            print_configuration=False, api_version='v0', port=6003)
        serve.main()
        mock_get_app.assert_called_once_with(
            'v0', controller=mock_from_settings.return_value, app_class=serve.metrics.CDCAggWebApp)

    @mock.patch.object(serve.metrics.CDCAggWebApp, 'set_oai_route_handler_class')
    def test_calls_app_set_oai_route_handler_class(self,
                                                   mock_set_oai_route_handler_class,
                                                   mock_from_settings,
                                                   mock_configure,
                                                   mock_serve):
        mock_from_settings.return_value = mock.Mock(stylesheet_url='/v0/oai/static/oai2.xsl')
        mock_configure.return_value = Namespace(
            print_configuration=False, api_version='v0', port=6003)
        serve.main()
        mock_set_oai_route_handler_class.assert_called_once_with(serve.http_api.OAIRouteHandler)

    @mock.patch.object(serve.metrics.CDCAggWebApp, 'add_handlers')
    def test_calls_app_add_handlers(self,
                                    mock_add_handlers,
                                    mock_from_settings,
                                    mock_configure,
                                    mock_serve):
        mock_configure.return_value = Namespace(
            print_configuration=False, api_version='v0', port=6003)
        serve.main()
        mock_add_handlers.assert_called_once_with('.*', [('/metrics', serve.metrics.CDCAggMetricsHandler)])


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
            oai_pmh_stylesheet_url=kw.get('oai_pmh_stylesheet_url',
                                          '/v0/oai/static/oai2.xsl'),
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
        study._aggregator_identifier.add_value('some_agg_id_1')
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
        self.assertEqual(''.join(origindesc_el.find('./oai_p:baseURL', XMLNS).itertext()),
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
        study._aggregator_identifier.add_value('some_agg_id_1')
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
                self._assert_origindesc(origindesc_el, {'altered': 'true', 'harvestDate': '2020-01-01T23:00.00Z'},
                                        'some.base', 'some:identifier', '1999-01-01', 'somenamespace')
                # Nested origindesc
                nested_origindesc_el = origindesc_el.find('./oai_p:originDescription', XMLNS)
                self._assert_origindesc(nested_origindesc_el, {'altered': 'false',
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
                study._aggregator_identifier.add_value('some_agg_id_1')
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
                study._aggregator_identifier.add_value('some_agg_id_1')
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

    def test_GET_getrecord_oai_ddi25_returns_stdyDscr_holdings_uri(self):
        study = Study()
        study.add_study_number('some_number')
        study._aggregator_identifier.add_value('agg_id_1')
        study.add_study_uris('some_study_uri', language='fi')
        study.add_study_uris('another_study_uri', language='en')
        study._provenance.add_value('someharvestdate', altered=True,
                                    base_url='http://somebaseurl',
                                    identifier='someidentifier', datestamp='somedatestamp',
                                    direct=True, metadata_namespace='somenamespace')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_ddi25',
                                                      identifier='someid'))
        expected = {'fi': 'some_study_uri',
                    'en': 'another_study_uri'}
        xmlns = dict(**XMLNS, **{'ddi': 'ddi:codebook:2_5', 'xml': 'http://www.w3.org/XML/1998/namespace'})
        for holdings_el in resp_el.findall('./oai:GetRecord/oai:record/oai:metadata/ddi:codeBook/ddi:stdyDscr/'
                                           'ddi:citation/ddi:holdings', xmlns):
            lang = holdings_el.get('{%s}lang' % (xmlns['xml'],))
            exp_uri = expected.pop(lang)
            self.assertEqual(holdings_el.get('URI'), exp_uri)
        self.assertEqual(expected, {})

    def test_GET_getrecord_oai_ddi25_returns_document_titles(self):
        study = Study()
        study.add_study_number('some_number')
        study._aggregator_identifier.add_value('agg_id_1')
        study.add_document_titles('some_doc', language='en')
        study.add_document_titles('joku_doc', language='fi')
        study._provenance.add_value('someharvestdate', altered=True,
                                    base_url='http://somebaseurl',
                                    identifier='someidentifier', datestamp='somedatestamp',
                                    direct=True, metadata_namespace='somenamespace')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_ddi25',
                                                      identifier='someid'))
        expected = {'fi': 'joku_doc',
                    'en': 'some_doc'}
        xmlns = dict(**XMLNS, **{'ddi': 'ddi:codebook:2_5', 'xml': 'http://www.w3.org/XML/1998/namespace'})
        for doc_titl_el in resp_el.findall('./oai:GetRecord/oai:record/oai:metadata/ddi:codeBook/ddi:docDscr/'
                                           'ddi:citation/ddi:titlStmt/ddi:titl', xmlns):
            lang = doc_titl_el.get('{%s}lang' % (xmlns['xml'],))
            exp_title = expected.pop(lang)
            self.assertEqual(''.join(doc_titl_el.itertext()), exp_title)
        self.assertEqual(expected, {})

    def test_GET_getrecord_oai_ddi25_returns_data_kinds(self):
        study = Study()
        study.add_study_number('some_number')
        study._aggregator_identifier.add_value('agg_id_1')
        study.add_data_kinds('some kind', 'en')
        study.add_data_kinds('joku kind', 'fi')
        study._provenance.add_value('someharvestdate', altered=True,
                                    base_url='http://somebaseurl',
                                    identifier='someidentifier', datestamp='somedatestamp',
                                    direct=True, metadata_namespace='somenamespace')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_ddi25',
                                                      identifier='someid'))
        expected = {'fi': 'joku kind',
                    'en': 'some kind'}
        xmlns = dict(**XMLNS, **{'ddi': 'ddi:codebook:2_5', 'xml': 'http://www.w3.org/XML/1998/namespace'})
        for doc_titl_el in resp_el.findall('./oai:GetRecord/oai:record/oai:metadata/ddi:codeBook/ddi:stdyDscr/'
                                           'ddi:stdyInfo/ddi:sumDscr/ddi:dataKind', xmlns):
            lang = doc_titl_el.get('{%s}lang' % (xmlns['xml'],))
            exp_title = expected.pop(lang)
            self.assertEqual(''.join(doc_titl_el.itertext()), exp_title)
        self.assertEqual(expected, {})

    # TEST OAI DATACITE

    def test_GET_getrecord_oai_datacite_returns_resourcetype(self):
        """Make sure #33 at BitBucket is implemented"""
        study = _study_for_datacite()
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        restype_els = resp_el.findall('./oai:GetRecord/oai:record/oai:metadata'
                                      '/datacite:resource/datacite:resourceType',
                                      DATACITE_XMLNS)
        self.assertEqual(len(restype_els), 1)
        restype_el = restype_els.pop()
        self.assertEqual(''.join(restype_el.itertext()), 'Dataset')
        self.assertEqual(restype_el.get('resourceTypeGeneral'), 'Dataset')

    def test_GET_getrecord_oai_datacite_returns_publisher(self):
        """Make sure #31 at BitBucket is fixed

        Publisher should render study.distributors as primary source
        and prefer english.
        """
        study = _study_for_datacite()
        # These two should not render.
        study.add_distributors('jakelija', 'fi')
        study.add_publishers('publisher', 'en')
        # This should render.
        study.add_distributors('distributor', 'en')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        publisher_els = resp_el.findall('./oai:GetRecord/oai:record/oai:metadata'
                                        '/datacite:resource/datacite:publisher',
                                        DATACITE_XMLNS)
        self.assertEqual(len(publisher_els), 1)
        self.assertEqual(''.join(publisher_els.pop().itertext()),
                         'distributor')

    def test_GET_getrecord_oai_datacite_returns_publicationyear(self):
        """Make sure #30 at BitBucket is fixed

        Format the value of publicationYear to only hold year.
        Change the primary lookup from study.publication_years.value to
        study.publication_years.attr_distribution_date.value.
        """
        study = _study_for_datacite()
        study.add_publication_years('1800', 'en', distribution_date='2002-01-02')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        publication_year_els = resp_el.findall('./oai:GetRecord/oai:record/oai:metadata'
                                               '/datacite:resource/datacite:publicationYear',
                                               DATACITE_XMLNS)
        self.assertEqual(len(publication_year_els), 1)
        self.assertEqual(''.join(publication_year_els.pop().itertext()),
                         '2002')

    def test_GET_getrecord_oai_datacite_returns_dates(self):
        """Make sure #29 at BitBucket is fixed.

        Include property Date in oai_datacite. Use
        study.publication_years.attr_distribution_date.value
        """
        study = _study_for_datacite()
        study.add_publication_years('1900', 'en', distribution_date='2002-01-02')
        study.add_publication_years('1800', 'fi', distribution_date='2003-03-04')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        xmlns = dict(**XMLNS, **{'datacite': 'http://datacite.org/schema/kernel-3',
                                 'xml': 'http://www.w3.org/XML/1998/namespace'})
        date_els = resp_el.findall('./oai:GetRecord/oai:record/oai:metadata/datacite:resource'
                                   '/datacite:dates/datacite:date', xmlns)
        self.assertEqual(len(date_els), 2)
        expected = ['2002-01-02', '2003-03-04']
        for date_el in date_els:
            val = ''.join(date_el.itertext())
            self.assertIn(val, expected)
            expected.remove(val)
        self.assertEqual(expected, [])

    def test_GET_getrecord_oai_datacite_identifier(self):
        study = Study()
        study.add_study_number('some_number')
        study.add_identifiers('some_id', 'en', agency='DOI')
        study._aggregator_identifier.add_value('agg_id_1')
        study._provenance.add_value('someharvestdate', altered=True,
                                    base_url='http://somebaseurl',
                                    identifier='someidentifier', datestamp='somedatestamp',
                                    direct=True, metadata_namespace='somenamespace')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='some_id'))
        xmlns = dict(**XMLNS, **{'datacite': 'http://datacite.org/schema/kernel-3',
                                 'xml': THE_XMLNS})
        id_els = resp_el.findall(
            './oai:GetRecord/oai:record/oai:metadata/datacite:resource/datacite:identifier',
            xmlns)
        self.assertEqual(len(id_els), 1)
        id_el = id_els.pop()
        self.assertEqual(''.join(id_el.itertext()), 'some_id')
        self.assertEqual(id_el.get('identifierType'), 'DOI')

    def test_GET_getrecord_oai_datacite_creators(self):
        study = _study_for_datacite()
        study.add_principal_investigators('some pi', 'en', organization='some org')
        study.add_principal_investigators('joku pi', 'fi', organization='joku org')
        study.add_principal_investigators('another pi', 'en', organization='another org')
        study.add_principal_investigators('toinen pi', 'fi')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='some_id'))
        xmlns = dict(**XMLNS, **{'datacite': 'http://datacite.org/schema/kernel-3',
                                 'xml': THE_XMLNS})
        creator_els = resp_el.findall(
            './oai:GetRecord/oai:record/oai:metadata/datacite:resource/'
            'datacite:creators/datacite:creator',
            xmlns)
        self.assertEqual(len(creator_els), 4)
        exp = {'some pi': ('en', 'some org'),
               'joku pi': ('fi', 'joku org'),
               'another pi': ('en', 'another org'),
               'toinen pi': ('fi', '')}
        for creator_el in creator_els:
            # Attribute 'xml:lang' is not allowed to appear in element 'creator'.
            self.assertNotIn(XML_LANG_ATT, creator_el.attrib)
            self.assertEqual(creator_el.attrib, {})
            name_el = creator_el.find('./datacite:creatorName', xmlns)
            # Attribute 'xml:lang' is not allowed to appear in element 'creatorName'.
            self.assertNotIn(XML_LANG_ATT, name_el.attrib)
            name = ''.join(name_el.itertext())
            self.assertIn(name, exp)
            aff_el = creator_el.find('./datacite:affiliation', xmlns)
            exp_lang, exp_aff = exp.pop(name)
            self.assertEqual(_get_xmllang(aff_el), exp_lang)
            self.assertEqual(''.join(aff_el.itertext()), exp_aff)

    def test_GET_getrecord_oai_datacite_titles(self):
        study = _study_for_datacite()
        study.add_study_titles('joku title', 'fi')
        study.add_study_titles('some title', 'en')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='some_id'))
        xmlns = dict(**XMLNS, **{'datacite': 'http://datacite.org/schema/kernel-3',
                                 'xml': THE_XMLNS})
        title_els = resp_el.findall(
            './oai:GetRecord/oai:record/oai:metadata/datacite:resource/datacite:titles/datacite:title',
            xmlns)
        exp = {'joku title': 'fi', 'some title': 'en'}
        self.assertEqual(len(title_els), len(exp))
        for title_el in title_els:
            title = ''.join(title_el.itertext())
            self.assertIn(title, exp)
            exp_lang = exp.pop(title)
            self.assertEqual(_get_xmllang(title_el), exp_lang)
        self.assertEqual(exp, {})

    def test_GET_getrecord_oai_datacite_publishers_prefers_english(self):
        """In Datacite, there can only be one publisher. Kuha records may have multiple.
        Prioritize english content. Otherwise take the first one.
        """
        study = _study_for_datacite()
        study.add_distributors('joku jakelija', 'fi')
        study.add_distributors('some distributor', 'en')
        study.add_publishers('joku julkaisija', 'fi')
        study.add_publishers('some publ', 'en')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        publ_els = resp_el.findall('./oai:GetRecord/oai:record/oai:metadata'
                                   '/datacite:resource/datacite:publisher',
                                   DATACITE_XMLNS)
        self.assertEqual(len(publ_els), 1)
        publ_el = publ_els.pop()
        self.assertEqual(''.join(publ_el.itertext()), 'some distributor')
        # Attribute 'xml:lang' is not allowed to appear in element 'publisher'.
        self.assertNotIn(XML_LANG_ATT, publ_el.attrib)

    def test_GET_getrecord_oai_datacite_publishers_takes_the_first_one(self):
        """In Datacite, there can only be one publisher. Kuha records may have multiple.
        Prioritize english content. Otherwise take the first one.
        """
        study = _study_for_datacite()
        study.add_publishers('någon publ', 'sv')
        study.add_publishers('joku julkaisija', 'fi')
        study.add_distributors('någon distr', 'sv')
        study.add_distributors('joku jakelija', 'fi')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        publ_els = resp_el.findall('./oai:GetRecord/oai:record/oai:metadata'
                                   '/datacite:resource/datacite:publisher',
                                   DATACITE_XMLNS)
        self.assertEqual(len(publ_els), 1)
        publ_el = publ_els.pop()
        self.assertEqual(''.join(publ_el.itertext()), 'någon distr')

    def test_GET_getrecord_oai_datacite_publisher_alternative_source_prefer_english(self):
        study = _study_for_datacite()
        study.add_publishers('joku julkaisija', 'fi')
        study.add_publishers('some publ', 'en')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        publ_els = resp_el.findall('./oai:GetRecord/oai:record/oai:metadata'
                                   '/datacite:resource/datacite:publisher',
                                   DATACITE_XMLNS)
        self.assertEqual(len(publ_els), 1)
        publ_el = publ_els.pop()
        self.assertEqual(''.join(publ_el.itertext()), 'some publ')

    def test_GET_getrecord_oai_datacite_publisher_alternative_source(self):
        study = _study_for_datacite()
        study.add_publishers('någon publ', 'sv')
        study.add_publishers('joku julkaisija', 'fi')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        publ_els = resp_el.findall('./oai:GetRecord/oai:record/oai:metadata'
                                   '/datacite:resource/datacite:publisher',
                                   DATACITE_XMLNS)
        self.assertEqual(len(publ_els), 1)
        publ_el = publ_els.pop()
        self.assertEqual(''.join(publ_el.itertext()), 'någon publ')

    def test_GET_getrecord_oai_datacite_publicationyear(self):
        study = _study_for_datacite()
        study.add_publication_years('2010', 'en', distribution_date='2011-01-02')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        publ_els = resp_el.findall('./oai:GetRecord/oai:record/oai:metadata'
                                   '/datacite:resource/datacite:publicationYear',
                                   DATACITE_XMLNS)
        self.assertEqual(len(publ_els,), 1)
        publ_el = publ_els.pop()
        self.assertEqual(''.join(publ_el.itertext()), '2011')

    def test_GET_getrecord_oai_datacite_publicationyear_unformatted(self):
        study = _study_for_datacite()
        study.add_publication_years('2010', 'en', distribution_date='2012')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        publ_els = resp_el.findall('./oai:GetRecord/oai:record/oai:metadata'
                                   '/datacite:resource/datacite:publicationYear',
                                   DATACITE_XMLNS)
        self.assertEqual(len(publ_els,), 1)
        publ_el = publ_els.pop()
        self.assertEqual(''.join(publ_el.itertext()), '2012')

    def test_GET_getrecord_oai_datacite_publicationyear_alternative(self):
        study = _study_for_datacite()
        study.add_publication_years('2010-01-02', 'en')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        publ_els = resp_el.findall('./oai:GetRecord/oai:record/oai:metadata'
                                   '/datacite:resource/datacite:publicationYear',
                                   DATACITE_XMLNS)
        self.assertEqual(len(publ_els), 1)
        publ_el = publ_els.pop()
        self.assertEqual(''.join(publ_el.itertext()), '2010')

    def test_GET_getrecord_oai_datacite_publicationyear_alternative_unformatted(self):
        study = _study_for_datacite()
        study.add_publication_years('2010', 'en')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        publ_els = resp_el.findall('./oai:GetRecord/oai:record/oai:metadata'
                                   '/datacite:resource/datacite:publicationYear',
                                   DATACITE_XMLNS)
        self.assertEqual(len(publ_els), 1)
        publ_el = publ_els.pop()
        self.assertEqual(''.join(publ_el.itertext()), '2010')

    def test_GET_getrecord_oai_datacite_subjects(self):
        study = _study_for_datacite()
        study.add_keywords(None, 'en', system_name='some system',
                           uri='some.uri', description='some keyword')
        study.add_keywords(None, 'en', system_name='another system',
                           uri='another.uri', description='another keyword')
        study.add_classifications(None, 'en', system_name='yasystem',
                                  uri='ya.uri', description='some class')
        study.add_classifications(None, 'fi', system_name='yasysteemi',
                                  uri='ya.uri/fi', description='joku luokka')
        exp = {'some keyword': ('some system', 'some.uri', 'en'),
               'another keyword': ('another system', 'another.uri', 'en'),
               'some class': ('yasystem', 'ya.uri', 'en'),
               'joku luokka': ('yasysteemi', 'ya.uri/fi', 'fi')}
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        subj_els = resp_el.findall('./oai:GetRecord/oai:record/oai:metadata'
                                   '/datacite:resource/datacite:subjects/datacite:subject',
                                   DATACITE_XMLNS)
        self.assertEqual(len(subj_els), len(exp))
        for subj_el in subj_els:
            val = ''.join(subj_el.itertext())
            self.assertIn(val, exp)
            exp_scheme, exp_uri, exp_lang = exp.pop(val)
            self.assertEqual(subj_el.get('subjectScheme'), exp_scheme)
            self.assertEqual(subj_el.get('schemeURI'), exp_uri)
            self.assertEqual(_get_xmllang(subj_el), exp_lang)
        self.assertEqual(exp, {})

    def test_GET_getrecord_oai_datacite_dates(self):
        study = _study_for_datacite()
        study.add_publication_years(None, 'en', distribution_date='2002-01-02')
        study.add_publication_years(None, 'fi', distribution_date='2003-03-04')
        exp = ['2002-01-02', '2003-03-04']
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        date_els = resp_el.findall('./oai:GetRecord/oai:record/oai:metadata'
                                   '/datacite:resource/datacite:dates/datacite:date',
                                   DATACITE_XMLNS)
        for date_el in date_els:
            # Attribute 'xml:lang' is not allowed to appear in element 'date'
            self.assertNotIn(XML_LANG_ATT, date_el.attrib)
            val = ''.join(date_el.itertext())
            self.assertIn(val, exp)
            exp.remove(val)
            self.assertEqual(date_el.get('dateType'), 'Issued')
        self.assertEqual(exp, [])

    def test_GET_getrecord_oai_datacite_rights(self):
        study = _study_for_datacite()
        study.add_data_access('some rights', 'en')
        study.add_data_access('joku rights', 'fi')
        exp = ['some rights', 'joku rights']
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        rights_els = resp_el.findall('./oai:GetRecord/oai:record/oai:metadata'
                                     '/datacite:resource/datacite:rightsList/datacite:rights',
                                     DATACITE_XMLNS)
        self.assertEqual(len(rights_els), len(exp))
        for rights_el in rights_els:
            # Attribute 'xml:lang' is not allowed to appear in element 'rights'
            self.assertNotIn(XML_LANG_ATT, rights_el.attrib)
            val = ''.join(rights_el.itertext())
            self.assertIn(val, exp)
            exp.remove(val)
        self.assertEqual(exp, [])

    def test_GET_getrecord_oai_datacite_description(self):
        study = _study_for_datacite()
        study.add_abstract('some abstract', 'en')
        study.add_abstract('joku abstrakti', 'fi')
        exp = {'some abstract': 'en',
               'joku abstrakti': 'fi'}
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        desc_els = resp_el.findall('./oai:GetRecord/oai:record/oai:metadata'
                                   '/datacite:resource/datacite:descriptions/datacite:description',
                                   DATACITE_XMLNS)
        self.assertEqual(len(desc_els), len(exp))
        for desc_el in desc_els:
            val = ''.join(desc_el.itertext())
            self.assertIn(val, exp)
            exp_lang = exp.pop(val)
            self.assertEqual(_get_xmllang(desc_el), exp_lang)
            # descriptionType='Abstract' is a constant
            self.assertEqual(desc_el.get('descriptionType'), 'Abstract')
        self.assertEqual(exp, {})

    def test_GET_getrecord_oai_datacite_geolocations(self):
        study = _study_for_datacite()
        study.add_geographic_coverages('some coverage', 'en')
        study.add_geographic_coverages('joku coverage', 'fi')
        exp = {'some coverage': 'en',
               'joku coverage': 'fi'}
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        loc_els = resp_el.findall(
            './oai:GetRecord/oai:record/oai:metadata'
            '/datacite:resource/datacite:geoLocations'
            '/datacite:geoLocation/datacite:geoLocationPlace',
            DATACITE_XMLNS)
        self.assertEqual(len(loc_els), len(exp))
        for loc_el in loc_els:
            val = ''.join(loc_el.itertext())
            self.assertIn(val, exp)
            exp_lang = exp.pop(val)
            self.assertEqual(_get_xmllang(loc_el), exp_lang)
        self.assertEqual(exp, {})

    def test_GET_getrecord_oai_datacite_relatedIdentifier(self):
        """Related Publication identifiers are mapped to relatedIdentifier

        Page at https://guidelines.openaire.eu/en/latest/data/field_relatedidentifier.html
        lists a controlled list of values for identifier type. Agency must be one of them,
        or relatedIdentifier cannot be rendered.

        See also https://guidelines.openaire.eu/en/latest/data/use_of_datacite.html#related-publications-and-datasets-information
        """
        study = _study_for_datacite()
        study.add_related_publications(None, language='en',
                                       identifier='first.id',
                                       identifier_agency='DOI')
        study.add_related_publications(None, language='en',
                                       identifier='second.id',
                                       identifier_agency='ISBN')
        study.add_related_publications(None, language='en',
                                       identifier='second.id',
                                       identifier_agency='ARK')
        # The rest should be ignored
        study.add_related_publications(None, language='en',
                                       identifier='second.id',
                                       identifier_agency='ARK')
        study.add_related_publications(None, language='en',
                                       identifier='third.id',
                                       identifier_agency='Unknown')
        study.add_related_publications(None, language='en',
                                       identifier='fourth.id')
        exp = [('first.id', 'DOI'), ('second.id', 'ISBN'), ('second.id', 'ARK')]
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        relid_els = resp_el.findall(
            './oai:GetRecord/oai:record/oai:metadata'
            '/datacite:resource/datacite:relatedIdentifiers/datacite:relatedIdentifier',
            DATACITE_XMLNS)
        self.assertEqual(len(relid_els), len(exp))
        for relid_el in relid_els:
            self.assertEqual(relid_el.get('relationType'), 'IsCitedBy')
            _id_type = (''.join(relid_el.itertext()),
                        relid_el.get('relatedIdentifierType'))
            self.assertIn(_id_type, exp)
            exp.remove(_id_type)
        self.assertEqual(exp, [])

    def test_GET_getrecord_oai_datacite_discards_relatedIdentifier(self):
        study = _study_for_datacite()
        study.add_related_publications(None, language='en',
                                       identifier_agency='ARK')
        study.add_related_publications(None, language='en',
                                       identifier='some.id',
                                       identifier_agency='')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        relid_els = resp_el.findall(
            './oai:GetRecord/oai:record/oai:metadata'
            '/datacite:resource/datacite:relatedIdentifiers/datacite:relatedIdentifier',
            DATACITE_XMLNS)
        self.assertEqual(relid_els, [])

    def test_GET_getrecord_oai_datacite_contributor(self):
        study = _study_for_datacite()
        study.add_grant_numbers('info:eu-repo/grantAgreement/EC/FP7/282896',
                                language='en', agency='some agency')
        study.add_grant_numbers('info:eu-repo/grantAgreement/funder/program/projectid',
                                language='fi', agency='joku agency')
        study.add_grant_numbers('some_grant_number',
                                language='en', agency='some agency')
        exp = [('info:eu-repo/grantAgreement/EC/FP7/282896', 'some agency'),
               ('info:eu-repo/grantAgreement/funder/program/projectid', 'joku agency')]
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        contributor_els = resp_el.findall(
            './oai:GetRecord/oai:record/oai:metadata'
            '/datacite:resource/datacite:contributors/datacite:contributor',
            DATACITE_XMLNS)
        self.assertEqual(len(contributor_els), len(exp))
        for contributor_el in contributor_els:
            self.assertEqual(contributor_el.get('contributorType'), 'Funder')
            cname_els = contributor_el.findall('./datacite:contributorName', DATACITE_XMLNS)
            self.assertEqual(len(cname_els), 1)
            nameid_els = contributor_el.findall('./datacite:nameIdentifier', DATACITE_XMLNS)
            self.assertEqual(len(nameid_els), 1)
            cname_el = cname_els.pop()
            # Attribute 'xml:lang' is not allowed to appear in element 'contributorName'
            self.assertNotIn(XML_LANG_ATT, cname_el.attrib)
            nameid_el = nameid_els.pop()
            # Attribute 'xml:lang' is not allowed to appear in element 'nameIdentifier'
            self.assertNotIn(XML_LANG_ATT, nameid_el)
            agency = ''.join(cname_el.itertext())
            nameid = ''.join(nameid_el.itertext())
            self.assertIn((nameid, agency), exp)
            exp.remove((nameid, agency))
        self.assertEqual(exp, [])

    def test_GET_getrecord_oai_datacite_does_not_contain_contributor(self):
        study = _study_for_datacite()
        study.add_grant_numbers('some_grant_number',
                                language='en', agency='some agency')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_datacite',
                                                      identifier='someid'))
        contributor_els = resp_el.findall(
            './oai:GetRecord/oai:record/oai:metadata'
            '/datacite:resource/datacite:contributors/datacite:contributor',
            DATACITE_XMLNS)
        self.assertEqual(contributor_els, [])

    # // TEST OAI DATACITE

    # TEST OAI DC

    def _assert_oai_dc_contains(self, resp_el, dc_xpath, expected):
        dc_els = resp_el.findall(
            f'./oai:GetRecord/oai:record/oai:metadata/oai_dc:dc/{dc_xpath}',
            OAIDC_XMLNS)
        self.assertEqual(len(dc_els), len(expected))
        for dc_el in dc_els:
            val = ''.join(dc_el.itertext())
            self.assertIn(val, expected)
            exp_lang = expected.pop(val)
            self.assertEqual(_get_xmllang(dc_el), exp_lang)
        self.assertEqual(expected, {})

    def test_GET_getrecord_oai_dc_contains_dc_type(self):
        """Make sure dc:type is present.

        Test against #36 at Bitbucket.
        """
        study = _study_for_oaidc()
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_dc',
                                                      identifier='agg_id_1'))
        self._assert_oai_dc_contains(resp_el, 'dc:type', {'Dataset': 'en'})

    def test_GET_getrecord_oai_dc_contains_dc_identifier(self):
        study = _study_for_oaidc()
        study.add_identifiers('some_id', language='en')
        study.add_identifiers('some_id', language='fi')
        study.add_document_uris('some_uri', language='fi')
        study.add_document_uris('some_uri', language='en')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_dc',
                                                      identifier='agg_id_1'))
        self._assert_oai_dc_contains(resp_el, 'dc:identifier', {'some_id': None,
                                                                'some_uri': None})

    def test_GET_getrecord_oai_dc_contains_distinct_uris_in_dc_identifier(self):
        study = _study_for_oaidc()
        study.add_identifiers('some_id', language='en')
        study.add_document_uris('some_uri', language='fi')
        study.add_study_uris('some_uri', language='sv')
        study.add_study_uris('another_uri', language='sv')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_dc',
                                                      identifier='agg_id_1'))
        self._assert_oai_dc_contains(resp_el, 'dc:identifier', {'some_id': None,
                                                                'some_uri': None,
                                                                'another_uri': None})

    def test_GET_getrecord_oai_dc_contains_dc_title(self):
        study = _study_for_oaidc()
        study.add_study_titles('sometitle', language='en')
        study.add_study_titles('jokutitle', language='fi')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_dc',
                                                      identifier='agg_id_1'))
        self._assert_oai_dc_contains(resp_el, 'dc:title', {'sometitle': 'en',
                                                           'jokutitle': 'fi'})

    def test_GET_getrecord_oai_dc_contains_dc_creator(self):
        study = _study_for_oaidc()
        study.add_principal_investigators('somepi', language='en')
        study.add_principal_investigators('jokupi', language='fi')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_dc',
                                                      identifier='agg_id_1'))
        self._assert_oai_dc_contains(resp_el, 'dc:creator', {'somepi': 'en',
                                                             'jokupi': 'fi'})

    def test_GET_getrecord_oai_dc_contains_dc_publisher(self):
        study = _study_for_oaidc()
        study.add_publishers('somepublisher', language='en')
        study.add_publishers('jokupublisher', language='fi')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_dc',
                                                      identifier='agg_id_1'))
        self._assert_oai_dc_contains(resp_el, 'dc:publisher',
                                     {'somepublisher': 'en',
                                      'jokupublisher': 'fi'})

    def test_GET_getrecord_oai_dc_contains_dc_description(self):
        study = _study_for_oaidc()
        study.add_abstract('someabs', language='en')
        study.add_abstract('jokuabs', language='fi')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_dc',
                                                      identifier='agg_id_1'))
        self._assert_oai_dc_contains(resp_el, 'dc:description',
                                     {'someabs': 'en',
                                      'jokuabs': 'fi'})

    def test_GET_getrecord_oai_dc_contains_dc_subject(self):
        study = _study_for_oaidc()
        study.add_keywords('somekeyword', language='en')
        study.add_keywords(None, language='fi', description='joku keyword')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_dc',
                                                      identifier='agg_id_1'))
        self._assert_oai_dc_contains(resp_el, 'dc:subject',
                                     {'somekeyword': 'en',
                                      'joku keyword': 'fi'})

    def test_GET_getrecord_oai_dc_contains_dc_language(self):
        study = _study_for_oaidc()
        study.add_study_titles('sometitle', language='en')
        study.add_study_titles('othertitle', language='en')
        study.add_study_titles('otsikko', language='fi')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_dc',
                                                      identifier='agg_id_1'))
        self._assert_oai_dc_contains(resp_el, 'dc:language',
                                     {'en': None,
                                      'fi': None})

    def test_GET_getrecord_oai_dc_contains_dc_date(self):
        study = _study_for_oaidc()
        study.add_publication_years('2000', language='en')
        study.add_publication_years(None, language='fi', distribution_date='1800')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_dc',
                                                      identifier='agg_id_1'))
        self._assert_oai_dc_contains(resp_el, 'dc:date',
                                     {'2000': 'en',
                                      '1800': 'fi'})

    def test_GET_getrecord_oai_dc_contains_dc_rights(self):
        study = _study_for_oaidc()
        study.add_data_collection_copyrights('somecopyright', language='en')
        study.add_data_collection_copyrights('jokucopyright', language='fi')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_dc',
                                                      identifier='agg_id_1'))
        self._assert_oai_dc_contains(resp_el, 'dc:rights',
                                     {'somecopyright': 'en',
                                      'jokucopyright': 'fi'})

    def test_GET_getrecord_oai_dc_contains_dc_coverage(self):
        study = _study_for_oaidc()
        study.add_study_area_countries('somecountry', language='en')
        study.add_study_area_countries('jokumaa', language='fi')
        resp_el = self.resp_to_xmlel(self.oai_request(study, verb='GetRecord',
                                                      metadata_prefix='oai_dc',
                                                      identifier='agg_id_1'))
        self._assert_oai_dc_contains(resp_el, 'dc:coverage',
                                     {'somecountry': 'en',
                                      'jokumaa': 'fi'})


    # // TEST OAI DC

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
                    'language:fi': ('Language fi', None),
                    'language:en': ('Language en', None),
                    'source': ('Source archive', None),
                    'source:FSD': ('FSD metadata', 'FSD metadata description'),
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

    def test_GET_getrecord_oai_ddi25_includes_fields(self):
        self.fetch(OAI_URL + '?verb=GetRecord&metadataPrefix=oai_ddi25&identifier=someid')
        calls = self._mock_fetch.call_args_list
        self.assertEqual(len(calls), 1)
        cargs, ckwargs = calls[0]
        self.assertCountEqual(cargs[2]['fields'], [
            'data_kinds',
            'document_titles',
            'study_uris',
            'parallel_study_titles',
            'citation_requirements',
            'principal_investigators',
            'study_area_countries',
            'collection_modes',
            'keywords',
            '_aggregator_identifier',
            'study_number',
            '_provenance',
            'deposit_requirements',
            'publishers',
            'geographic_coverages',
            'publication_dates',
            'copyrights',
            'file_names',
            'identifiers',
            'analysis_units',
            'time_methods',
            'universes',
            'publication_years',
            'distributors',
            'data_collection_copyrights',
            'instruments',
            'study_titles',
            '_metadata',
            'data_access',
            'abstracts',
            'collection_periods',
            'related_publications',
            'document_uris',
            'sampling_procedures',
            'data_access_descriptions',
            'classifications',
            'funding_agencies',
            'grant_numbers'])

    def test_GET_getrecord_oai_datacite_includes_fields(self):
        self.fetch(OAI_URL + '?verb=GetRecord&metadataPrefix=oai_datacite&identifier=someid')
        calls = self._mock_fetch.call_args_list
        self.assertEqual(len(calls), 1)
        cargs, ckwargs = calls[0]
        self.assertCountEqual(cargs[2]['fields'], [
            'study_titles',
            'classifications',
            'geographic_coverages',
            'identifiers',
            '_metadata',
            'keywords',
            '_aggregator_identifier',
            'abstracts',
            'study_number',
            '_provenance',
            'data_access',
            'publication_years',
            'distributors',
            'publishers',
            'principal_investigators',
            'grant_numbers',
            'related_publications'
        ])

    def test_GET_getrecord_oai_dc_includes_fields(self):
        self.fetch(OAI_URL + '?verb=GetRecord&metadataPrefix=oai_dc&identifier=someid')
        calls = self._mock_fetch.call_args_list
        self.assertEqual(len(calls), 1)
        cargs, ckwargs = calls[0]
        self.assertCountEqual(cargs[2]['fields'], [
            '_aggregator_identifier',
            '_metadata',
            '_provenance',
            'study_number',
            'study_titles',
            'identifiers',
            'principal_investigators',
            'publishers',
            'document_uris',
            'study_uris',
            'abstracts',
            'keywords',
            'publication_years',
            'study_area_countries',
            'data_collection_copyrights'])


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


def get_line(sbuffer, lineno):
    for index in range(lineno):
        if index == lineno - 1:
            return next(sbuffer)
        next(sbuffer)


class TestNoXMLStylesheet(_Base):
    """Make sure XML Stylesheet is included

    CDCAGG OAI-PMH contains two templates that are not from Kuha2:
    get_record and list_records. Test that they also contain the
    stylesheets.
    """
    oai_pmh_stylesheet_url = ''

    def setUp(self):
        self.settings(oai_pmh_stylesheet_url=self.oai_pmh_stylesheet_url)
        super().setUp()
        self._mock_query_single = self._init_patcher(mock.patch(
            'kuha_common.query.QueryController.query_single'))
        self._mock_query_multiple = self._init_patcher(mock.patch(
            'kuha_common.query.QueryController.query_multiple'))
        self._mock_query_count = self._init_patcher(mock.patch(
            'kuha_common.query.QueryController.query_count'))

    def _assert_oai_response_success(self, response):
        xmlel = ElementTree.fromstring(response.body)
        self.assertIsNone(xmlel.find('oai:error', XMLNS))

    def _assert_stylesheet_line(self, response):
        self._assert_oai_response_success(response)
        stylesheet_line = get_line(response.buffer, 2).decode('utf8')
        stylesheet_line = stylesheet_line.split('?>')[0]
        self.assertTrue(stylesheet_line.startswith('<?xml-stylesheet type=\'text/xsl\' href='))
        stylesheet_url = stylesheet_line.split("'")[-2]
        self.assertEqual(stylesheet_url, self.oai_pmh_stylesheet_url)

    def _assert_no_stylesheet(self, response):
        self._assert_oai_response_success(response)
        stylesheet_line = get_line(response.buffer, 2).decode('utf8')
        self.assertNotIn('xml-stylesheet', stylesheet_line)

    def _assert_method(self, response):
        return self._assert_no_stylesheet(response)

    def test_get_record(self):
        self._mock_query_single.side_effect = mock_coro(func=_query_single(_study_for_oaidc()))
        self._assert_method(self.fetch(OAI_URL + '?verb=GetRecord&identifier=someid&metadataPrefix=oai_dc'))

    def test_list_records(self):
        self._mock_query_count.side_effect = mock_coro(1)
        self._mock_query_multiple.side_effect = mock_coro(func=_query_multiple({'studies': [_study_for_oaidc()]}))
        self._assert_method(self.fetch(OAI_URL + '?verb=ListRecords&metadataPrefix=oai_dc'))

class TestHasXMLStylesheet(TestNoXMLStylesheet):

    oai_pmh_stylesheet_url = '/some/path'

    def _assert_method(self, response):
        return self._assert_stylesheet_line(response)
