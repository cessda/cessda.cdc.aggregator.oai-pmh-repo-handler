from unittest import mock
from kuha_common.testing.testcases import KuhaUnitTestCase
from cdcagg_oai import serve


class TestConfigure(KuhaUnitTestCase):

    @mock.patch.object(serve, 'conf')
    @mock.patch.object(serve.server, 'add_cli_args')
    @mock.patch.object(serve.controller, 'add_cli_args')
    def test_calls_conf_load(self, mock_server_add_cli_args,
                             mock_controller_add_cli_args, mock_conf):
        serve.configure([])
        mock_conf.load.assert_called_once_with(
            prog='cdcagg_oai', package='cdcagg_oai', env_var_prefix='CDCAGG_')
