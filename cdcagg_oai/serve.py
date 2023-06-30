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
"""Entrypoint to start serving the OAI-PMH Repo Handler.

Handle command line arguments, application setup, discovery & load of plugins,
server startup and critical exception logging.
"""
import logging
from py12flogging.log_formatter import (
    set_ctx_populator,
    setup_app_logging
)

from kuha_common import (
    conf,
    server
)

from kuha_oai_pmh_repo_handler import (
    http_api,
    controller
)
from kuha_oai_pmh_repo_handler.serve import load_metadataformats


_logger = logging.getLogger(__name__)


def configure(mdformats):
    """Configure application.

    Define configuration options. Load settings.
    Configure metadataformats. Setup logging.
    Return loaded settings.

    :param list mdformats: Loaded metadataformats.
    :returns: Loaded settings.
    :rtype: :obj:`argparse.Namespace`
    """
    conf.load(prog='cdcagg_oai', package='cdcagg_oai', env_var_prefix='CDCAGG_')
    conf.add('--api-version', help='API version is prepended to URLs',
             default='v0', type=str, env_var='OAIPMH_API_VERSION')
    conf.add('--port', help='Port to listen to', type=int, env_var='OAIPMH_PORT',
             default=6003)
    conf.add_print_arg()
    conf.add_config_arg()
    conf.add_loglevel_arg()
    server.add_cli_args()
    controller.add_cli_args()
    for mdformat in mdformats:
        mdformat.add_cli_args(conf)
    settings = conf.get_conf()
    set_ctx_populator(server.serverlog_ctx_populator)
    setup_app_logging(conf.get_package(), loglevel=settings.loglevel, port=settings.port)
    for mdformat in mdformats:
        mdformat.configure(settings)
    server.configure(settings)
    return settings


def main():
    """Starts the server.

    Load metadataformats using entrypoint discovery group
    `cdcagg.oai.metadataformats`. Call :func:`configure` to
    define, load and setup configurations. Initiate controller
    and start server.
    """
    mdformats = load_metadataformats('cdcagg.oai.metadataformats')
    settings = configure(mdformats)
    if settings.print_configuration:
        print('Print active configuration and exit\n')
        conf.print_conf()
        return
    try:
        ctrl = controller.from_settings(settings, mdformats)
        app = http_api.get_app(settings.api_version, controller=ctrl)
    except Exception:
        _logger.exception('Exception in application setup')
        raise
    try:
        server.serve(app, settings.port)
    except KeyboardInterrupt:
        _logger.warning('Shutdown by CTRL + C', exc_info=True)
    except Exception:
        _logger.exception('Unhandled exception in main()')
        raise
    finally:
        _logger.info('Exiting')
