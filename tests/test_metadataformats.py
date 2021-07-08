from tempfile import NamedTemporaryFile
from unittest import mock
from argparse import Namespace
from kuha_common.testing.testcases import KuhaUnitTestCase
from kuha_common.testing import mock_coro
from cdcagg_common.records import Study
from cdcagg_oai import metadataformats


SOURCES = """
-
  url: 'http://some.url'
  source: 'some source'
-
  url: 'http://another.url'
  source: 'another source'
"""


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

    def test_configure_raises_ValueError_for_invalid_file(self):
        settings = Namespace(oai_set_sources_path='some/invalid/path')
        with self.assertRaises(ValueError):
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
