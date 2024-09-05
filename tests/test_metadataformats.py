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
from tempfile import NamedTemporaryFile
from unittest import mock, TestCase, IsolatedAsyncioTestCase
from argparse import Namespace
from yaml.parser import ParserError
from cdcagg_common.records import Study
from cdcagg_oai import metadataformats
from . import testcasebase


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
  setname: 'some source name'
  description: 'some source desc'
-
  url: 'http://another.url'
  source: 'another source'
  setname: 'another source name'
"""


INVALID_YAML = """
outer: {inner: value)
"""


def _configurable_sets_with_path(filename):
    return """
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
  - path: '{ext_path}'
""".format(ext_path=os.path.abspath(os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'data', filename)))


class TestConfigurableMDSet(TestCase):

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
        self.assertTrue(rval)

    def _assert_configure_raises_InvalidMappingConfig(self, filecontent):
        with NamedTemporaryFile(mode='w', delete=False) as somefile:
            somefile.write(filecontent)
            somefile.close()
            settings = Namespace(oai_set_configurable_path=somefile.name)
            with self.assertRaises(metadataformats.InvalidMappingConfig):
                metadataformats.ConfigurableAggMDSet.configure(settings)

    def test_configure_raises_config_no_spec(self):
        self._assert_configure_raises_InvalidMappingConfig(
            "name: 'somename'\n"
            "nodes:\n"
            "  - spec: 'somespec'\n"
            "    name: 'name'\n"
            "    identifiers:\n"
            "    - id_1")

    def test_configure_raises_config_invalid_spec(self):
        self._assert_configure_raises_InvalidMappingConfig(
            "spec: True\n"
            "name: 'somename'\n"
            "nodes:\n"
            "  - spec: 'somespec'\n"
            "    name: 'name'\n"
            "    identifiers:\n"
            "    - id_1")

    def test_configure_raises_config_no_name(self):
        self._assert_configure_raises_InvalidMappingConfig(
            "spec: 'spec'\n"
            "nodes:\n"
            "  - spec: 'somespec'\n"
            "    name: 'name'\n"
            "    identifiers:\n"
            "    - id_1")

    def test_configure_raises_config_invalid_name(self):
        self._assert_configure_raises_InvalidMappingConfig(
            "spec: 'spec'\n"
            "name: ''\n"
            "nodes:\n"
            "  - spec: 'somespec'\n"
            "    name: 'name'\n"
            "    identifiers:\n"
            "    - id_1")

    def test_configure_raises_config_no_nodes(self):
        self._assert_configure_raises_InvalidMappingConfig(
            "spec: 'spec'\n"
            "name: 'somename'")

    def test_configure_raises_config_invalid_nodes(self):
        self._assert_configure_raises_InvalidMappingConfig(
            "spec: 'spec'\n"
            "name: 'somename'\n"
            "nodes: 'somenodes'")

    def test_configure_raises_node_no_spec(self):
        self._assert_configure_raises_InvalidMappingConfig(
            "spec: 'spec'\n"
            "name: 'somename'\n"
            "nodes:\n"
            "  - name: 'name'\n"
            "    identifiers:\n"
            "    - id_1")

    def test_configure_raises_node_invalid_spec(self):
        self._assert_configure_raises_InvalidMappingConfig(
            "spec: 'spec'\n"
            "name: 'somename'\n"
            "nodes:\n"
            "  - spec: ''\n"
            "    name: 'name'\n"
            "    identifiers:\n"
            "    - id_1")

    def test_configure_raises_node_no_name(self):
        self._assert_configure_raises_InvalidMappingConfig(
            "spec: 'spec'\n"
            "name: 'somename'\n"
            "nodes:\n"
            "  - spec: 'somespec'\n"
            "    identifiers:\n"
            "    - id_1")

    def test_configure_raises_node_invalid_name(self):
        self._assert_configure_raises_InvalidMappingConfig(
            "spec: 'spec'\n"
            "name: 'somename'\n"
            "nodes:\n"
            "  - spec: 'somespec'\n"
            "    name: 1\n"
            "    identifiers:\n"
            "    - id_1")

    def test_configure_raises_node_no_identifiers(self):
        self._assert_configure_raises_InvalidMappingConfig(
            "spec: 'spec'\n"
            "name: 'somename'\n"
            "nodes:\n"
            "  - spec: 'somespec'\n"
            "    name: 'name'")

    def test_configure_raises_node_invalid_identifiers(self):
        self._assert_configure_raises_InvalidMappingConfig(
            "spec: 'spec'\n"
            "name: 'somename'\n"
            "nodes:\n"
            "  - spec: 'somespec'\n"
            "    name: 'name'\n"
            "    identifiers: \n"
            "      key: 'value'")


class TestConfigurableMDSetAsync(IsolatedAsyncioTestCase):

    async def test_get_config_with_path_single_node(self):
        conf_set = _configurable_sets_with_path('ext_confset.yaml')
        with NamedTemporaryFile(mode='w', delete=False) as somefile:
            somefile.write(conf_set)
            somefile.close()
            settings = Namespace(oai_set_configurable_path=somefile.name)
            metadataformats.ConfigurableAggMDSet.configure(settings)
            conf_agg_set = metadataformats.ConfigurableAggMDSet('metadataformat')
        cnf = await conf_agg_set._get_config()
        expected = [{'description': 'Studies in social sciences',
                     'identifiers': ['id_1', 'id_2'],
                     'name': 'Social sciences',
                     'spec': 'social_sciences'},
                    {'description': 'Studies in history',
                     'identifiers': ['id_5', 'id_6'],
                     'name': 'History',
                     'spec': 'history'}]
        self.assertEqual(cnf['nodes'], expected)

    async def test_get_config_with_path_multiple_nodes(self):
        conf_set = _configurable_sets_with_path('ext_confsets.yaml')
        with NamedTemporaryFile(mode='w', delete=False) as somefile:
            somefile.write(conf_set)
            somefile.close()
            settings = Namespace(oai_set_configurable_path=somefile.name)
            metadataformats.ConfigurableAggMDSet.configure(settings)
            conf_agg_set = metadataformats.ConfigurableAggMDSet('metadataformat')
        cnf = await conf_agg_set._get_config()
        expected = [{'description': 'Studies in social sciences',
                     'identifiers': ['id_1', 'id_2'],
                     'name': 'Social sciences',
                     'spec': 'social_sciences'},
                    {'description': 'Studies in history',
                     'identifiers': ['id_5', 'id_6'],
                     'name': 'History',
                     'spec': 'history'},
                    {'description': 'Literature Studies',
                     'identifiers': ['id_7', 'id_8'],
                     'name': 'Literature',
                     'spec': 'literature'}]
        self.assertEqual(cnf['nodes'], expected)


class TestSourceAggMDSet(TestCase):

    def tearDown(self):
        metadataformats.SourceAggMDSet._source_defs = None
        super().tearDown()

    def test_add_cli_args(self):
        mock_parser = mock.Mock()
        metadataformats.SourceAggMDSet.add_cli_args(mock_parser)
        mock_parser.add.assert_called_once_with(
            '--oai-set-sources-path', help='Full path to sources definitions',
            env_var='OPRH_OS_SOURCES_PATH', type=str)

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


class TestSourceAggMDSetWithYAMLFile(testcasebase(IsolatedAsyncioTestCase)):

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

    async def test_get_reads_sources_from_file(self):
        mock_mdformat = mock.Mock()
        source_set = metadataformats.SourceAggMDSet(mock_mdformat)
        study = Study()
        study._provenance.add_value('someharvestdate', altered=True, base_url='http://some.url',
                                    identifier='someidentifier', datestamp='somedatestamp',
                                    direct=True, metadata_namespace='somenamespace')
        result = await source_set.get(study)
        self.assertEqual(result, ['some source'])

    async def test_get_returns_empty_list_if_no_source_found(self):
        mock_mdformat = mock.Mock()
        source_set = metadataformats.SourceAggMDSet(mock_mdformat)
        study = Study()
        study._provenance.add_value('someharvestdate', altered=True, base_url='http://yetanother.url',
                                    identifier='someidentifier', datestamp='somedatestamp',
                                    direct=True, metadata_namespace='somenamespace')
        result = await source_set.get(study)
        self.assertEqual(result, [])

    async def test_query_calls_param(self):
        # Mock & format
        mock_query_distinct = self._init_patcher(mock.patch(
            'kuha_common.query.QueryController.query_distinct'))
        mock_query_distinct.return_value = {'_provenance.base_url': [
            'http://yetanother.url', 'http://some.url', 'http://another.url']}
        mock_mdformat = mock.Mock(study_class=Study, corr_id_header={'key': 'value'})
        source_set = metadataformats.SourceAggMDSet(mock_mdformat)
        mock_on_set_cb = mock.AsyncMock()
        # Call
        await source_set.query(mock_on_set_cb)
        # Assert
        exp_calls = {'source': {'name': 'Source archive'},
                     'source:some source': {'name': 'some source name',
                                            'description': 'some source desc'},
                     'source:another source': {'name': 'another source name',
                                               'description': None}}
        calls = mock_on_set_cb.call_args_list
        self.assertEqual(len(calls), len(exp_calls))
        for call in calls:
            cargs, ckwargs = call
            self.assertEqual(len(cargs), 1)
            self.assertIn(cargs[0], exp_calls)
            exp_ckwargs = exp_calls.pop(cargs[0])
            self.assertEqual(ckwargs, exp_ckwargs)
