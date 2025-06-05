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

import os.path
import copy
from argparse import Namespace

from tornado.testing import AsyncHTTPTestCase

from kuha_common.document_store import query, client
from kuha_oai_pmh_repo_handler.oai.constants import (
    OAI_REC_NAMESPACE_IDENTIFIER,
    OAI_RESPONSE_LIST_SIZE,
    OAI_PROTOCOL_VERSION,
    OAI_RESPOND_WITH_REQ_URL,
    OAI_REPO_NAME
)
from kuha_oai_pmh_repo_handler import metadataformats as kuha_metadataformats
from cdcagg_oai import serve, metadataformats


API_VERSION = 'v0'


def isolate_oai_pmh_route_handler_class():
    copy_initial_oai_route_handler_class = copy.copy(serve.metrics.CDCAggWebApp._oai_route_handler_class)

    def _reset():
        serve.metrics.CDCAggWebApp._oai_route_handler_class = copy_initial_oai_route_handler_class
    return _reset


def isolate_kuha_metadataformats_storage():
    stored = dict(kuha_metadataformats._STORED)

    def _reset():
        kuha_metadataformats._STORED = stored
    return _reset


def testcasebase(parent_test_case):
    class _TestCaseBase(parent_test_case):
        def setUp(self):
            self._resets = []
            super().setUp()

        def tearDown(self):
            for reset in self._resets:
                reset()
            super().tearDown()

        def _init_patcher(self, patcher):
            _mock_obj = patcher.start()
            self._resets.append(patcher.stop)
            return _mock_obj
    return _TestCaseBase


class CDCAggOAIHTTPTestBase(testcasebase(AsyncHTTPTestCase)):

    _settings = None

    @classmethod
    def settings(cls, **kw):
        if cls._settings is not None:
            raise ValueError("_settings is already defined.")
        cls._settings = Namespace(
            api_version=kw.get('api_version', API_VERSION),
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
        isolation_resets = [isolate_oai_pmh_route_handler_class(), isolate_kuha_metadataformats_storage()]
        super().setUp()
        self._resets.extend(isolation_resets)

    def tearDown(self):
        self._clear_settings()
        defaults = self.settings()
        client.configure(defaults)
        query.configure(defaults)
        self._clear_settings()
        super().tearDown()

    def get_app(self):
        if self._settings is None:
            self.settings()
        mdformats = serve.load_metadataformats('cdcagg.oai.metadataformats')
        for mdf in mdformats:
            mdf.configure(self._settings)
        app = serve.app_setup(self._settings, mdformats)
        return app
