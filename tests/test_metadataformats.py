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

from tempfile import NamedTemporaryFile
from unittest import mock
from argparse import Namespace
from yaml.parser import ParserError
from kuha_common.testing.testcases import KuhaUnitTestCase
from kuha_common.testing import mock_coro
from cdcagg_common.records import Study
from cdcagg_oai import metadataformats


CONFIGURABLE_SETS = """
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
"""


SOURCES = """
-
  url: 'http://some.url'
  source: 'some source'
-
  url: 'http://another.url'
  source: 'another source'
"""


INVALID_YAML = """
outer: {inner: value)
"""


class TestConfigurableMDSet(KuhaUnitTestCase):

    def tearDown(self):
        metadataformats.ConfigurableAggMDSet.spec = None
        metadataformats.ConfigurableAggMDSet._loaded_filepath = None
        super().tearDown()

    def test_init_raises_NotImplementedError_if_no_spec(self):
        with self.assertRaises(NotImplementedError):
            metadataformats.ConfigurableAggMDSet(mock.Mock())

    def test_add_cli_args_adds_args(self):
        mock_parser = mock.Mock()
        metadataformats.ConfigurableAggMDSet.add_cli_args(mock_parser)
        mock_parser.add.assert_called_once_with(
            '--oai-set-configurable-path',
            help='Path to look for configurable OAI set definitions. Leave unset to discard '
            'configurable set.', env_var='OPRH_OS_CONFIGURABLE_PATH', type=str)

    def test_configure_raises_FileNotFoundError_for_invalid_file(self):
        settings = Namespace(oai_set_configurable_path='/some/invalid/path')
        with self.assertRaises(FileNotFoundError):
            metadataformats.ConfigurableAggMDSet.configure(settings)

    def test_configure_raises_ParseError_for_invalid_yaml_syntax(self):
        with NamedTemporaryFile(mode='w', delete=False) as somefile:
            somefile.write(INVALID_YAML)
            somefile.close()
            settings = Namespace(oai_set_configurable_path=somefile.name)
            with self.assertRaises(ParserError):
                metadataformats.ConfigurableAggMDSet.configure(settings)

    def test_configure_returns_False_if_file_not_given(self):
        # oai_set_configurable_path attribute in Namespace object is None if it is
        # declared to parser but not given via configuration options.
        rval = metadataformats.ConfigurableAggMDSet.configure(Namespace(oai_set_configurable_path=None))
        self.assertFalse(rval)

    def test_configure_accepts_a_valid_file(self):
        with NamedTemporaryFile(mode='w', delete=False) as somefile:
            somefile.write(CONFIGURABLE_SETS)
            somefile.close()
            settings = Namespace(oai_set_configurable_path=somefile.name)
            # Should not raise here
            rval = metadataformats.ConfigurableAggMDSet.configure(settings)
        self.assertEqual(metadataformats.ConfigurableAggMDSet._loaded_filepath, somefile.name)
        self.assertIsNone(rval)


class TestSourceAggMDSet(KuhaUnitTestCase):

    def tearDown(self):
        metadataformats.SourceAggMDSet._source_defs = None
        super().tearDown()

    def test_add_cli_args(self):
        mock_parser = mock.Mock()
        metadataformats.SourceAggMDSet.add_cli_args(mock_parser)
        mock_parser.add.assert_called_once_with(
            '--oai-set-sources-path', help='Full path to sources definitions',
            env_var='OPRH_OS_SOURCES_PATH', default=metadataformats.SourceAggMDSet._default_filepath,
            type=str)

    def test_configure_raises_FileNotFoundError_for_invalid_file(self):
        settings = Namespace(oai_set_sources_path='some/invalid/path')
        with self.assertRaises(FileNotFoundError):
            metadataformats.SourceAggMDSet.configure(settings)

    def test_configure_raises_ParseError_for_invalid_yaml_syntax(self):
        with NamedTemporaryFile(mode='w', delete=False) as somefile:
            somefile.write(INVALID_YAML)
            somefile.close()
            settings = Namespace(oai_set_sources_path=somefile.name)
            with self.assertRaises(ParserError):
                metadataformats.SourceAggMDSet.configure(settings)

    def test_configure_accepts_a_valid_file(self):
        with NamedTemporaryFile() as somefile:
            settings = Namespace(oai_set_sources_path=somefile.name)
            # If this does not raise, we're good.
            metadataformats.SourceAggMDSet.configure(settings)
        self.assertEqual(metadataformats.SourceAggMDSet._source_defs, [])


class TestSourceAggMDSetWithYAMLFile(KuhaUnitTestCase):

    def setUp(self):
        super().setUp()
        self._sourcesfile = NamedTemporaryFile(mode='w', delete=False)
        self._sourcesfile.write(SOURCES)
        self._sourcesfile.close()
        settings = Namespace(oai_set_sources_path=self._sourcesfile.name)
        metadataformats.SourceAggMDSet.configure(settings)

    def tearDown(self):
        metadataformats.SourceAggMDSet._source_defs = None
        self._sourcesfile.close()
        super().tearDown()

    def test_get_reads_sources_from_file(self):
        mock_mdformat = mock.Mock()
        source_set = metadataformats.SourceAggMDSet(mock_mdformat)
        study = Study()
        study._provenance.add_value('someharvestdate', altered=True, base_url='http://some.url',
                                    identifier='someidentifier', datestamp='somedatestamp',
                                    direct=True, metadata_namespace='somenamespace')
        self._loop.run_until_complete(self.await_and_store_result(source_set.get(study)))
        self.assertEqual(self._stored_result, ['some source'])

    def test_get_returns_original_url_if_no_source_found(self):
        mock_mdformat = mock.Mock()
        source_set = metadataformats.SourceAggMDSet(mock_mdformat)
        study = Study()
        study._provenance.add_value('someharvestdate', altered=True, base_url='http://yetanother.url',
                                    identifier='someidentifier', datestamp='somedatestamp',
                                    direct=True, metadata_namespace='somenamespace')
        self._loop.run_until_complete(self.await_and_store_result(source_set.get(study)))
        self.assertEqual(self._stored_result, ['http://yetanother.url'])

    def test_query_calls_param(self):
        # Mock & format
        mock_query_distinct = self.init_patcher(mock.patch(
            'kuha_common.query.QueryController.query_distinct'))
        mock_query_distinct.return_value = {'_provenance.base_url': [
            'http://yetanother.url', 'http://some.url', 'http://another.url']}
        mock_mdformat = mock.Mock(study_class=Study, corr_id_header={'key': 'value'})
        source_set = metadataformats.SourceAggMDSet(mock_mdformat)
        mock_on_set_cb = mock.Mock(side_effect=mock_coro())
        # Call
        self._loop.run_until_complete(self.await_and_store_result(source_set.query(mock_on_set_cb)))
        # Assert
        exp_calls = {'source': 'Source archive',
                     'source:http://yetanother.url': None,
                     'source:some source': None,
                     'source:another source': None}
        calls = mock_on_set_cb.call_args_list
        self.assertEqual(len(calls), len(exp_calls))
        for call in calls:
            cargs, ckwargs = call
            self.assertEqual(len(cargs), 1)
            self.assertIn(cargs[0], exp_calls)
            exp_name_param = exp_calls.pop(cargs[0])
            if exp_name_param:
                self.assertEqual(ckwargs, {'name': exp_name_param})
            else:
                self.assertEqual(len(ckwargs), 0)