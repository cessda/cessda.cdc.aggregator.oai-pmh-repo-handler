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
import os
from collections import defaultdict
from prometheus_client import (
    CollectorRegistry,
    Gauge,
    Counter,
    Summary,
    REGISTRY,
    GC_COLLECTOR,
    PLATFORM_COLLECTOR,
    PROCESS_COLLECTOR,
)
from prometheus_client.multiprocess import MultiProcessCollector
from prometheus_client.exposition import choose_encoder

from kuha_common import server, query
from kuha_common.document_store.constants import REC_STATUS_DELETED
from cdcagg_common.records import Study


# Disable default metrics
REGISTRY.unregister(GC_COLLECTOR)
REGISTRY.unregister(PLATFORM_COLLECTOR)
REGISTRY.unregister(PROCESS_COLLECTOR)


_METRICS = {
    # Define Aggregator OAI-PMH metrics - requests metrics
    "requests_total": Counter("requests_total", "Total number of external catalogue requests received"),
    "requests_per_user_agent": Counter(
        "requests_per_user_agent",
        "Number of external catalogue requests received per user-agent",
        ["harvester"],
    ),
    "requests_succeeded": Counter("requests_succeeded", "Number of successful catalogue requests"),
    "requests_failed": Counter("requests_failed", "Number of failed catalogue requests"),
    "requests_duration": Summary("requests_duration", "Response time in milliseconds", ["verb", "metadataPrefix"]),
    # Define Aggregator OAI-PMH metrics - Service provider (Publisher) metrics
    "records_total": None,
    "records_total_without_deleted": None,
    "publishers_total": None,
    "publishers_counts": None,
    "publishers_counts_without_deleted": None,
    "registry": None,
}


class _Gauge(Gauge):
    _MULTIPROC_MODES = set(list(Gauge._MULTIPROC_MODES) + ["current"])


def _file_filter(_file):
    typ, mode, *_ = os.path.basename(_file).split("_")
    return typ != "gauge" or mode != "current"


class _MultiProcessCollector(MultiProcessCollector):
    @staticmethod
    def merge(files, accumulate=True):
        """Override merge to apply :func:`_file_filter`

        This method is called from
        MultiProcessCollector.collect(). Files for Gauge metrics where
        multiprocess mode is 'current' should be filtered out before
        calling MultiProcessCollector._read_metrics(). Otherwise we
        get metrics for each running python process, which makes no
        sense to metrics that represent the current state of the
        application data.

        :param files: Iterable filepaths
        :param accumulate: Set False to avoid compound accumulation (see overridden method)
        :returns: Metrics values. See MultiProcessCollector._accumulate_metrics().
        """
        metrics = MultiProcessCollector._read_metrics(filter(_file_filter, files))
        return MultiProcessCollector._accumulate_metrics(metrics, accumulate)


def _initialize_metrics_registry():
    if not _METRICS["registry"]:
        # prometheus-client does not allow setting
        # 'PROMETHEUS_MULTIPROC_DIR' configuration option via other
        # mechanisms than environment variable (see
        # prometheus_client/values.py::get_value_class()).
        if "PROMETHEUS_MULTIPROC_DIR" in os.environ:
            registry = CollectorRegistry()
            _MultiProcessCollector(registry)
            _common_kwargs = {"multiprocess_mode": "current", "registry": registry}
        else:
            registry = REGISTRY
            # The mode is not read at all when not using the MultiProcessCollector.
            # Leaving it to the default value to improve future
            # compatibility in case of changes in prometheus-client code.
            _common_kwargs = {"registry": registry}
        _METRICS["registry"] = registry
        _METRICS["records_total"] = _Gauge("records_total", "Total number of records included", **_common_kwargs)
        _METRICS["records_total_without_deleted"] = _Gauge(
            "records_total_without_deleted",
            "Total number of records included without logically deleted records",
            **_common_kwargs
        )
        _METRICS["publishers_total"] = _Gauge(
            "publishers_total", "Total number of distinct publishers", **_common_kwargs
        )
        _METRICS["publishers_counts"] = _Gauge(
            "publishers_counts", "Number of records included per Publisher", ["publisher"], **_common_kwargs
        )
        _METRICS["publishers_counts_without_deleted"] = _Gauge(
            "publishers_counts_without_deleted",
            "Number of records included per Publisher without logically deleted records",
            ["publisher"],
            **_common_kwargs
        )
    return (
        _METRICS["registry"],
        _METRICS["records_total"],
        _METRICS["records_total_without_deleted"],
        _METRICS["publishers_total"],
        _METRICS["publishers_counts"],
        _METRICS["publishers_counts_without_deleted"],
    )


async def _count_number_of_publishers(base_urls_counts):
    async def _inner_count_number_of_publishers(study):
        direct_base_url = None
        for prov in study._provenance:
            if prov.attr_direct.get_value() is True:
                direct_base_url = prov.attr_base_url.get_value()
                break
        if direct_base_url is None:
            # This should never happen. There is something wrong
            # with the record if we end up in there.
            # The cdcagg_client should take care to always assign
            # a direct provenance, but the Document Store does
            # have a REST API which can be used to alter records.
            raise ValueError(f"Study {study.get_id()} does not "
                             "have a direct provenance. It is most likely a "
                             "corrupted database entry.")
        # Found the direct base_url. Increase number of records for this publisher.
        base_urls_counts[direct_base_url]["overall"] += 1
        if not study.is_deleted():
            # Study is not deleted. Increase number of non-deleted records for this publisher.
            base_urls_counts[direct_base_url]["non_deleted"] += 1
    return _inner_count_number_of_publishers


class CDCAggMetricsHandler(server.RequestHandler):
    """Interface for prometheus server

    Provides a HTTP GET for collecting metrics using pull model over
    HTTP.
    """

    async def get(self):
        """HTTP GET handler for prometheus metrics"""
        (
            registry,
            metric_records_total,
            metric_records_total_without_deleted,
            metric_publishers_total,
            metric_publishers_counts,
            metric_publishers_counts_without_deleted,
        ) = _initialize_metrics_registry()
        query_ctrl = query.QueryController(headers=self._correlation_id.as_header())
        # Total number of records included
        metric_records_total.set(await query_ctrl.query_count(Study))
        # Total number of records included without deleted ones
        metric_records_total_without_deleted.set(
            await query_ctrl.query_count(
                Study, _filter={Study._metadata.attr_status: {query_ctrl.fk_constants.not_equal: REC_STATUS_DELETED}}
            )
        )
        #
        # Number of Publishers (Service Providers) included and
        # Number of records included per Publisher
        #
        # 1. Query all study fields _metadata & _provenance
        # 2. Find every _provenance.base_url which is contained within an object where direct is True.
        # 3. Find out if _metadata.status = DELETED.
        # Result is a dict:
        # {<base_url>: {"overall": <overall_count>, "non_deleted": <non_deleted_count>}}
        # len(result_dict) is the publishers total count.
        # result_dict[<base_url>]["overall"] is the number of records for this publisher.
        # result_dict[<base_url>]["non_deleted"] is the number of non-deleted records for this publisher.
        base_urls_counts = defaultdict(lambda: {"overall": 0, "non_deleted": 0})
        count_base_urls_counts = await _count_number_of_publishers(base_urls_counts)
        await query_ctrl.query_multiple(Study, count_base_urls_counts, fields=[Study._metadata, Study._provenance])
        metric_publishers_total.set(len(base_urls_counts))
        for base_url, counts in base_urls_counts.items():
            metric_publishers_counts.labels(publisher=base_url).set(counts["overall"])
            metric_publishers_counts_without_deleted.labels(publisher=base_url).set(counts["non_deleted"])
        encoder, content_type = choose_encoder(self.request.headers.get("accept"))
        self.set_header("Content-Type", content_type)
        self.finish(encoder(registry))


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
        _METRICS["requests_total"].inc()
        _METRICS["requests_per_user_agent"].labels(harvester=handler.request.headers.get("User-Agent")).inc()
        if handler.get_status() < 300:
            _METRICS["requests_succeeded"].inc()
            if not handler.oai_protocol.response.context["error"]:
                # If the response is an oai error, the duration
                # probably should not be mixed with succesfull oai responses.
                _METRICS["requests_duration"].labels(
                    verb=handler.oai_protocol.arguments.verb,
                    metadataPrefix=handler.oai_protocol.arguments.metadata_prefix,
                ).observe(1000.0 * handler.request.request_time())
        else:
            _METRICS["requests_failed"].inc()
