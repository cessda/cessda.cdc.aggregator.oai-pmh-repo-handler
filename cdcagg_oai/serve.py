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
    return settings


def main():
    mdformats = load_metadataformats('cdcagg.oai.metadataformats')
    settings = configure(mdformats)
    if settings.print_configuration:
        print('Print active configuration and exit\n')
        conf.print_conf()
        return
    try:
        ctrl = controller.from_settings(settings, mdformats)
        app = http_api.get_app(settings.api_version, controller=ctrl)
    except:
        _logger.exception('Exception in application setup')
        raise
    try:
        server.serve(app, settings.port)
    except KeyboardInterrupt:
        _logger.warning('Shutdown by CTRL + C', exc_info=True)
    except:
        _logger.exception('Unhandled exception in main()')
        raise
    finally:
        _logger.info('Exiting')
