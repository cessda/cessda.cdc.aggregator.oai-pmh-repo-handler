"""Test /metrics endpoint
"""
from unittest import mock
from cdcagg_common import Study
from . import CDCAggOAIHTTPTestBase


class TestMetricsEndpoint(CDCAggOAIHTTPTestBase):
    maxDiff = None

    def setUp(self):
        super().setUp()
        self._mock_query_distinct = self._init_patcher(mock.patch("kuha_common.query.QueryController.query_distinct"))
        self._mock_query_count = self._init_patcher(mock.patch("kuha_common.query.QueryController.query_count"))

    def _fetch_resp_body_lines(self):
        resp = self.fetch("/metrics")
        return resp.body.decode("utf8").split("\n")

    def test_calls_query_count_twice_if_there_is_no_base_url(self):
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

    def test_calls_query_count_four_times_if_there_is_single_base_url(self):
        self._mock_query_distinct.return_value = {"_provenance.base_url": ["some.base.url"]}
        self._mock_query_count.return_value = 1
        self.fetch("/metrics")
        self.assertEqual(self._mock_query_count.call_count, 4)
        self._mock_query_count.assert_has_awaits(
            [
                mock.call(Study),
                mock.call(
                    Study,
                    _filter={Study._metadata.attr_status: {"$ne": "deleted"}},
                ),
                mock.call(
                    Study,
                    _filter={
                        Study._provenance: {
                            "$elemMatch": {
                                Study._provenance.attr_base_url: "some.base.url",
                                Study._provenance.attr_direct: True,
                            }
                        }
                    },
                ),
                mock.call(
                    Study,
                    _filter={
                        "$and": [
                            {
                                Study._provenance: {
                                    "$elemMatch": {
                                        Study._provenance.attr_base_url: "some.base.url",
                                        Study._provenance.attr_direct: True,
                                    }
                                }
                            },
                            {Study._metadata.attr_status: {"$ne": "deleted"}},
                        ]
                    },
                ),
            ],
            any_order=False,
        )

    def test_calls_query_count_eight_times_if_there_is_three_base_urls(self):
        self._mock_query_distinct.return_value = {
            "_provenance.base_url": ["some.base.url", "another.base.url", "third.base.url"]
        }
        self._mock_query_count.return_value = 1
        self.fetch("/metrics")
        self.assertEqual(self._mock_query_count.call_count, 8)

    def test_returns_records_total(self):
        self._mock_query_count.side_effect = [10, 1]
        self.assertIn("records_total 10.0", self._fetch_resp_body_lines())

    def test_returns_records_total_without_deleted(self):
        self._mock_query_count.side_effect = [10, 2]
        self.assertIn("records_total_without_deleted 2.0", self._fetch_resp_body_lines())

    def test_returns_publishers_total(self):
        self._mock_query_distinct.return_value = {"_provenance.base_url": ["some.base.url"]}
        self._mock_query_count.return_value = 1
        self.assertIn("publishers_total 1.0", self._fetch_resp_body_lines())

    def test_returns_publishers_total_is_zero_if_count_query_returns_0(self):
        self._mock_query_distinct.return_value = {"_provenance.base_url": ["some.base.url"]}
        self._mock_query_count.side_effect = [1, 1, 0]
        self.assertIn("publishers_total 0.0", self._fetch_resp_body_lines())

    def test_returns_publishers_counts(self):
        self._mock_query_distinct.return_value = {"_provenance.base_url": ["some.base.url"]}
        self._mock_query_count.side_effect = [1, 1, 20, 0]
        self.assertIn('publishers_counts{publisher="some.base.url"} 20.0', self._fetch_resp_body_lines())

    def test_returns_publishers_counts_without_deleted(self):
        self._mock_query_distinct.return_value = {"_provenance.base_url": ["some.base.url"]}
        self._mock_query_count.side_effect = [1, 1, 1, 30]
        self.assertIn(
            'publishers_counts_without_deleted{publisher="some.base.url"} 30.0', self._fetch_resp_body_lines()
        )
