"""Provides /metrics endpoint

This module contains CDCAggMetricsHandler and CDCAggWebApp which are
used to integrate Prometheus metrics and serve them via Tornado web
server.

Due to the nature of Tornado servers, this code should be built to
support multiprocess applications (single server application with
multiple child processes). This imposes some limitations to the
features that can be used with the prometheus client.

- Registries can not be used as normal, all instantiated metrics are exported
  - Registering metrics to a registry later used by a
    MultiProcessCollector may cause duplicate metrics to be exported
- Custom collectors do not work (e.g. cpu and memory metrics)
- Info and Enum metrics do not work
- The pushgateway cannot be used
- Gauges cannot use the pid label
- Exemplars are not supported

Also, the deployment must use a file storage to share metrics between
worker processes.

For more information, see the prometheus client docs
(https://github.com/prometheus/client_python#multiprocess-mode-eg-gunicorn).
"""

from prometheus_client import (
    Gauge,
    Counter,
    REGISTRY,
    GC_COLLECTOR,
    PLATFORM_COLLECTOR,
    PROCESS_COLLECTOR,
)
from prometheus_client.exposition import choose_encoder

from kuha_common import server, query
from cdcagg_common.records import Study


# Disable default metrics
REGISTRY.unregister(GC_COLLECTOR)
REGISTRY.unregister(PLATFORM_COLLECTOR)
REGISTRY.unregister(PROCESS_COLLECTOR)

# Define Aggregator OAI-PMH metrics
#
# Service provider (Publisher) metrics
metric_records_total = Gauge("records_total", "Total number of records included")
metric_publishers_total = Gauge("publishers_total", "Total number of distinct publishers")
metric_publishers_counts = Gauge("publishers_counts", "Number of records included per Publisher", ["publisher"])
# Requests metrics
metric_external_catalogue_requests = Counter(
    "external_catalogue_requests", "Total number of external catalogue requests received"
)
metric_external_catalogue_requests_per_harvester = Counter(
    "external_catalogue_requests_per_harvester",
    "Number of external catalogue requests received per harvester", ["harvester"])
metric_successful_catalogue_requests = Counter("successful_catalogue_requests",
                                               "Number of successful external catalogue requests")
metric_unsuccessful_catalogue_requests = Counter("unsuccessful_catalogue_requests",
                                                 "Number of unsuccessful external catalogue requests")


class CDCAggMetricsHandler(server.RequestHandler):
    """Interface for prometheus server

    Provides a HTTP GET for collecting metrics using pull model over
    HTTP.
    """

    async def get(self):
        """HTTP GET handler for prometheus metrics
        """
        query_ctrl = query.QueryController(headers=self._correlation_id.as_header())
        # Total number of records included
        metric_records_total.set(await query_ctrl.query_count(Study))
        # Number of Publishers (Service Providers) included and
        # Number of records included per Publisher
        distinct_base_urls = await query_ctrl.query_distinct(Study, fieldname=Study._provenance.attr_base_url)
        publishers_total_count = 0
        for base_url in distinct_base_urls[Study._provenance.attr_base_url.path]:
            count = await query_ctrl.query_count(Study, _filter={
                Study._provenance: {
                    query.QueryController.fk_constants.elem_match: {
                        Study._provenance.attr_base_url: base_url,
                        Study._provenance.attr_direct: True}}})
            if count == 0:
                continue
            publishers_total_count += 1
            metric_publishers_counts.labels(publisher=base_url).set(count)
        metric_publishers_total.set(publishers_total_count)
        encoder, content_type = choose_encoder(self.request.headers.get("accept"))
        self.set_header("Content-Type", content_type)
        self.finish(encoder(REGISTRY))


class CDCAggWebApp(server.WebApplication):
    """Override the default WebApplication to control log_request
    method"""
    _oai_route_handler_class = None

    @classmethod
    def set_oai_route_handler_class(cls, oai_route_handler_class):
        """Set oai route handler class

        All requests handled by the oai_route_handler_class are
        considered oai-pmh harvesting requests.

        :param oai_route_handler_class: Handler responsible for OAI-PMH requests
        :raises ValueError: If oai_route_handler_class is already defined
        """
        if cls._oai_route_handler_class:
            raise ValueError("oai_route_handler_class already defined")
        cls._oai_route_handler_class = oai_route_handler_class

    def log_request(self, handler):
        """Override log_request to gather OAI-PMH request metrics

        :param handler: Current request handler
        """
        super().log_request(handler)
        if not isinstance(handler, self._oai_route_handler_class):
            # requests to other endpoints are not considered
            # OAI-PMH harvesting requests.
            return
        metric_external_catalogue_requests.inc()
        metric_external_catalogue_requests_per_harvester.labels(
            harvester=handler.request.headers.get('User-Agent')).inc()
        if handler.get_status() < 300:
            metric_successful_catalogue_requests.inc()
        else:
            metric_unsuccessful_catalogue_requests.inc()
