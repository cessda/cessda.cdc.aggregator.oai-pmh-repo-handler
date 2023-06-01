"""Provides /metrics endpoint

This module contains CDCAggMetricsHandler and CDCAggWebApp which are
used to integrate Prometheus metrics and serve them via Tornado web
server.
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

records_total = Gauge("records_total", "Total number of records included")
publishers_total = Gauge("publishers_total", "Total number of distinct publishers")
publishers_counts = Gauge("publishers_counts", "Number of records included per Publisher", ['publisher'])


class CDCAggMetricsHandler(server.RequestHandler):
    async def get(self):
        query_ctrl = query.QueryController(headers=self._correlation_id.as_header())
        # Total number of records included
        records_total.set(await query_ctrl.query_count(Study))
        # Number of Publishers (Service Providers) included and
        # Number of records included per Publisher
        distinct_base_urls = await query_ctrl.query_distinct(Study, fieldname=Study._provenance.attr_base_url)
        for base_url in distinct_base_urls[Study._provenance.attr_base_url.path]:
            count = await query_ctrl.query_count(Study, _filter={
                Study._provenance: {
                    query.QueryController.fk_constants.elem_match: {
                        Study._provenance.attr_base_url: base_url,
                        Study._provenance.attr_direct: True}}})
            if count == 0:
                continue
            publishers_total.inc()
            publishers_counts.labels(publisher=base_url).set(count)
        encoder, content_type = choose_encoder(self.request.headers.get("accept"))
        self.set_header("Content-Type", content_type)
        self.finish(encoder(REGISTRY))


class CDCAggWebApp(server.WebApplication):
    def log_request(self, handler):
        super().log_request(handler)
        handler_name = type(handler).__name__
        method = handler.request.method
        status = handler.get_status()
        # requests_total.labels(handler_name, method, status).inc()
