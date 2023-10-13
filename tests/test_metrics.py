"""Test /metrics endpoint & module internals
"""
from unittest import mock, TestCase, IsolatedAsyncioTestCase
from kuha_common.document_store.records import REC_STATUS_DELETED
from cdcagg_common import Study
from cdcagg_oai import metrics
from . import CDCAggOAIHTTPTestBase


# ###################### #
# Test /metrics endpoint #
# ###################### #


class TestMetricsEndpoint(CDCAggOAIHTTPTestBase):
    """Test /metrics endpoint with HTTP Requests"""

    maxDiff = None

    async def _query_multiple_side_eff(self, _, handler, **kwargs):
        for study in self._query_multiple_result:
            await handler(study)

    def setUp(self):
        super().setUp()
        self._mock_query_count = self._init_patcher(mock.patch("kuha_common.query.QueryController.query_count"))
        self._mock_query_multiple = self._init_patcher(
            mock.patch("kuha_common.query.QueryController.query_multiple", side_effect=self._query_multiple_side_eff)
        )
        self._query_multiple_result = []

    def _fetch_resp_body_lines(self):
        resp = self.fetch("/metrics")
        return resp.body.decode("utf8").split("\n")

    def test_calls_query_count_twice(self):
        self.fetch("/metrics")
        self.assertEqual(self._mock_query_count.call_count, 2)
        self._mock_query_count.assert_has_awaits(
            [
                mock.call(Study),
                mock.call(
                    Study,
                    _filter={Study._metadata.attr_status: {"$ne": "deleted"}},
                ),
            ],
            any_order=False,
        )

    def test_calls_query_multiple(self):
        self.fetch("/metrics")
        self.assertEqual(self._mock_query_multiple.call_count, 1)
        args, kwargs = self._mock_query_multiple.call_args_list[0]
        self.assertEqual(len(args), 2)
        self.assertEqual(args[0], Study)
        self.assertEqual(kwargs, {"fields": [Study._metadata, Study._provenance]})

    def test_returns_records_total(self):
        self._mock_query_count.side_effect = [10, 1]
        self.assertIn("records_total 10.0", self._fetch_resp_body_lines())

    def test_returns_records_total_without_deleted(self):
        self._mock_query_count.side_effect = [10, 2]
        self.assertIn("records_total_without_deleted 2.0", self._fetch_resp_body_lines())

    def test_returns_publishers_total(self):
        study = Study()
        study._provenance.add_value(
            "someharvestdate",
            altered=True,
            base_url="some.base.url",
            identifier="some_identifier",
            datestamp="somedatestamp",
            direct=False,
            metadata_namespace="some_namespace",
        )
        study._provenance.add_value(
            "someharvestdate",
            altered=True,
            base_url="another.base.url",
            identifier="some_identifier",
            datestamp="somedatestamp",
            direct=True,
            metadata_namespace="some_namespace",
        )
        self._query_multiple_result.append(study)
        self._mock_query_count.return_value = 1
        self.assertIn("publishers_total 1.0", self._fetch_resp_body_lines())

    def test_returns_publishers_total_is_zero_if_no_records_are_returned(self):
        self._mock_query_count.side_effect = [1, 1]
        self.assertIn("publishers_total 0.0", self._fetch_resp_body_lines())

    def test_returns_publishers_counts(self):
        for _ in range(20):
            study = Study()
            study._provenance.add_value(
                "someharvestdate",
                altered=True,
                base_url="another.base.url",
                identifier="some_identifier",
                datestamp="somedatestamp",
                direct=False,
                metadata_namespace="some_namespace",
            )
            study._provenance.add_value(
                "someharvestdate",
                altered=True,
                base_url="some.base.url",
                identifier="some_identifier",
                datestamp="somedatestamp",
                direct=True,
                metadata_namespace="some_namespace",
            )
            self._query_multiple_result.append(study)
        self._mock_query_count.side_effect = [1, 1]
        self.assertIn('publishers_counts{publisher="some.base.url"} 20.0', self._fetch_resp_body_lines())

    def test_returns_publishers_counts_without_deleted(self):
        for index in range(20):
            study = Study()
            study._provenance.add_value(
                "someharvestdate",
                altered=True,
                base_url="another.base.url",
                identifier="some_identifier",
                datestamp="somedatestamp",
                direct=False,
                metadata_namespace="some_namespace",
            )
            study._provenance.add_value(
                "someharvestdate",
                altered=True,
                base_url="some.base.url",
                identifier="some_identifier",
                datestamp="somedatestamp",
                direct=True,
                metadata_namespace="some_namespace",
            )
            if index % 2:
                study.set_status(REC_STATUS_DELETED)
            self._query_multiple_result.append(study)
        self._mock_query_count.side_effect = [1, 1]
        body_lines = self._fetch_resp_body_lines()
        self.assertIn('publishers_counts_without_deleted{publisher="some.base.url"} 10.0', body_lines)
        self.assertIn('publishers_counts{publisher="some.base.url"} 20.0', body_lines)


# ################################### #
# Unittests against metrics.py module #
# ################################### #


class TestCDCAggMetricsHandler(IsolatedAsyncioTestCase):
    async def _query_multiple_side_eff(self, _, handler, **kwargs):
        for study in self._query_multiple_result:
            await handler(study)

    @mock.patch.object(metrics, "_initialize_metrics_registry", return_value=(mock.Mock() for _ in range(6)))
    @mock.patch("kuha_common.query.QueryController.query_multiple")
    @mock.patch("kuha_common.query.QueryController.query_count")
    async def test_raises_ValueError_if_study_has_no_direct_provenance(
        self, mock_query_count, mock_query_multiple, mock_initialize_metrics_registry
    ):
        mock_query_multiple.side_effect = self._query_multiple_side_eff
        study = Study()
        study._provenance.add_value(
            "someharvestdate",
            altered=True,
            base_url="some.base.url",
            identifier="some_identifier",
            datestamp="somedatestamp",
            direct=False,
            metadata_namespace="some_namespace",
        )
        self._query_multiple_result = [study]
        mock_query_count.return_value = 1
        handler = metrics.CDCAggMetricsHandler(mock.MagicMock(), mock.Mock())
        handler._correlation_id = mock.Mock(as_header=mock.Mock(return_value={}))
        with self.assertRaises(ValueError):
            await handler.get()


@mock.patch.object(metrics.MultiProcessCollector, "_read_metrics")
@mock.patch.object(metrics.MultiProcessCollector, "_accumulate_metrics")
class TestMultiProcessCollector(TestCase):
    """Unittests against metrics._MultiProcessCollector"""

    def test_merge_calls_read_metrics_with_filtered_files(self, mock_accumulate_metrics, mock_read_metrics):
        files = [
            "/path/to/gauge_all_1.db",
            "/path/to/gauge_current_1.db",
            "/path/to/gauge_current_2.db",
            "/path/to/counter_1.db",
            "/path/to/counter_current_1.db",
        ]
        metrics._MultiProcessCollector.merge(files)
        self.assertEqual(mock_read_metrics.call_count, 1)
        call_files_iterable = mock_read_metrics.call_args_list[0][0][0]
        self.assertEqual(
            list(call_files_iterable),
            ["/path/to/gauge_all_1.db", "/path/to/counter_1.db", "/path/to/counter_current_1.db"],
        )

    def test_merge_calls_accumulate_metrics_correctly(self, mock_accumulate_metrics, mock_read_metrics):
        for accumulate in (True, False):
            with self.subTest(accumulate=accumulate):
                metrics._MultiProcessCollector.merge(["file1", "file2"], accumulate=accumulate)
                mock_accumulate_metrics.assert_called_once_with(mock_read_metrics.return_value, accumulate)
            mock_accumulate_metrics.reset_mock()

    def test_merge_returns_accumulate_metrics_return_value(self, mock_accumulate_metrics, mock_read_metrics):
        rval = metrics._MultiProcessCollector.merge(["file1", "file2"])
        self.assertEqual(rval, mock_accumulate_metrics.return_value)


@mock.patch.object(metrics, "_Gauge")
class TestInitializeMetricsRegistry(TestCase):
    def setUp(self):
        super().setUp()
        self._stored_metrics = dict(metrics._METRICS)

    def tearDown(self):
        metrics._METRICS = self._stored_metrics
        super().tearDown()

    @mock.patch.object(metrics, "CollectorRegistry")
    @mock.patch.object(metrics, "_MultiProcessCollector")
    @mock.patch.object(metrics.os, "environ", new={"PROMETHEUS_MULTIPROC_DIR": "/some/dir"})
    def test_initializes_MultiProcessCollector_if_environ_has_PROMETHEUS_MULTIPROC_DIR(
        self, mock_MultiProcessCollector, mock_CollectorRegistry, mock_Gauge
    ):
        metrics._initialize_metrics_registry()
        mock_MultiProcessCollector.assert_called_once_with(mock_CollectorRegistry.return_value)

    @mock.patch.object(metrics, "_MultiProcessCollector")
    @mock.patch.object(metrics.os, "environ", new={})
    def test_does_not_initialize_MultiProcessCollector_if_environ_does_not_have_PROMETHEUS_MULTIPROC_DIR(
        self, mock_MultiProcessCollector, mock_Gauge
    ):
        metrics._initialize_metrics_registry()
        mock_MultiProcessCollector.assert_not_called()

    @mock.patch.object(metrics, "CollectorRegistry")
    @mock.patch.object(metrics, "_MultiProcessCollector")
    @mock.patch.object(metrics.os, "environ", new={"PROMETHEUS_MULTIPROC_DIR": "/some/dir"})
    def test_passes_correct_args_to_Gauges_if_environ_has_PROMETHEUS_MULTIPROC_DIR(
        self, mock_MultiProcessCollector, mock_CollectorRegistry, mock_Gauge
    ):
        metrics._initialize_metrics_registry()
        self.assertEqual(mock_Gauge.call_count, 5)
        mock_Gauge.assert_has_calls(
            [
                mock.call(
                    "records_total",
                    "Total number of records included",
                    multiprocess_mode="current",
                    registry=mock_CollectorRegistry.return_value,
                ),
                mock.call(
                    "records_total_without_deleted",
                    "Total number of records included without logically deleted records",
                    multiprocess_mode="current",
                    registry=mock_CollectorRegistry.return_value,
                ),
                mock.call(
                    "publishers_total",
                    "Total number of distinct publishers",
                    multiprocess_mode="current",
                    registry=mock_CollectorRegistry.return_value,
                ),
                mock.call(
                    "publishers_counts",
                    "Number of records included per Publisher",
                    ["publisher"],
                    multiprocess_mode="current",
                    registry=mock_CollectorRegistry.return_value,
                ),
                mock.call(
                    "publishers_counts_without_deleted",
                    "Number of records included per Publisher without logically deleted records",
                    ["publisher"],
                    multiprocess_mode="current",
                    registry=mock_CollectorRegistry.return_value,
                ),
            ]
        )

    @mock.patch.object(metrics.os, "environ", new={})
    def test_passes_correct_args_to_Gauges_if_environ_does_not_have_PROMETHEUS_MULTIPROC_DIR(self, mock_Gauge):
        metrics._initialize_metrics_registry()
        self.assertEqual(mock_Gauge.call_count, 5)
        mock_Gauge.assert_has_calls(
            [
                mock.call(
                    "records_total",
                    "Total number of records included",
                    registry=metrics.REGISTRY,
                ),
                mock.call(
                    "records_total_without_deleted",
                    "Total number of records included without logically deleted records",
                    registry=metrics.REGISTRY,
                ),
                mock.call(
                    "publishers_total",
                    "Total number of distinct publishers",
                    registry=metrics.REGISTRY,
                ),
                mock.call(
                    "publishers_counts",
                    "Number of records included per Publisher",
                    ["publisher"],
                    registry=metrics.REGISTRY,
                ),
                mock.call(
                    "publishers_counts_without_deleted",
                    "Number of records included per Publisher without logically deleted records",
                    ["publisher"],
                    registry=metrics.REGISTRY,
                ),
            ]
        )


class TestCDCAggWebApp(TestCase):
    def setUp(self):
        super().setUp()
        self._stored_class = metrics.CDCAggWebApp._oai_route_handler_class
        self._stored_metrics = dict(metrics._METRICS)

    def tearDown(self):
        metrics.CDCAggWebApp._oai_route_handler_class = self._stored_class
        metrics._METRICS = self._stored_metrics
        super().tearDown()

    def test_set_oai_route_handler_class_sets_oai_route_handler_class(self):
        self.assertEqual(metrics.CDCAggWebApp._oai_route_handler_class, None)
        metrics.CDCAggWebApp.set_oai_route_handler_class("some_class")
        self.assertEqual(metrics.CDCAggWebApp._oai_route_handler_class, "some_class")

    def test_set_oai_route_handler_class_sets_raises_ValueError_if_already_set(self):
        metrics.CDCAggWebApp.set_oai_route_handler_class("some_class")
        with self.assertRaises(ValueError):
            metrics.CDCAggWebApp.set_oai_route_handler_class("another_class")

    @mock.patch.object(metrics.server.WebApplication, "log_request")
    def test_log_request_increments_requests_failed_if_handler_get_status_returns_300_or_more(self, mock_log_request):
        mock_handler = mock.Mock()
        metrics.CDCAggWebApp.set_oai_route_handler_class(mock_handler.__class__)
        app = metrics.CDCAggWebApp()
        mock_requests_failed_metric = mock.Mock()
        metrics._METRICS["requests_failed"] = mock_requests_failed_metric
        for status_code in (300, 301, 400, 500):
            mock_handler.get_status.return_value = status_code
            with self.subTest(status_code=status_code):
                app.log_request(mock_handler)
                mock_requests_failed_metric.inc.assert_called_once_with()
            mock_requests_failed_metric.reset_mock()
