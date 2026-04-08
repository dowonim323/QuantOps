import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tools.financial_db as financial_db_module
from pipelines.financial_crawler import REPORT_TASKS, _apply_updates, _run_parallel, fetch_reports
from tools.crawler import (
    FinancialCrawler,
    UnsupportedWiseReportSymbol,
    _build_dataframe_from_payload,
)
from tools.financial_db import FinancialDBBatchWriter, load_db, update_db


def _payload(*, labels: list[str], rows: list[dict[str, object]]) -> dict[str, object]:
    return {
        "YYMM": labels,
        "DATA": rows,
    }


class TestFinancialCrawlerBrowserless(unittest.TestCase):
    def test_ratio_payload_uses_existing_metric_order(self):
        payload = _payload(
            labels=[
                "2024/12<br />(IFRS연결)",
                "2025/12(E)<br />(IFRS연결)",
                "전년대비<br />(YoY)",
            ],
            rows=[
                {"ACC_NM": "EPS", "LVL": 1, "DATA1": 100, "DATA2": 120},
                {"ACC_NM": "PER", "LVL": 1, "DATA1": 10.5, "DATA2": 9.3},
                {"ACC_NM": "보통주.수정주가(기말)＜당기＞", "LVL": 1, "DATA1": 1, "DATA2": 2},
            ],
        )

        df = _build_dataframe_from_payload("ratio", payload)

        self.assertEqual(
            list(df.index),
            [
                "EPS",
                "BPS",
                "CPS",
                "SPS",
                "PER",
                "PBR",
                "PCR",
                "PSR",
                "EV/EBITDA",
                "DPS",
                "현금배당수익률",
                "현금배당성향(%)",
            ],
        )
        self.assertEqual(list(df.columns), ["2024/12"])
        self.assertEqual(df.loc["EPS", "2024/12"], 100.0)
        self.assertTrue(pd.isna(df.loc["BPS", "2024/12"]))
        self.assertEqual(df.loc["PER", "2024/12"], 10.5)

    def test_payload_skips_estimated_periods_without_shifting_data_alignment(self):
        payload = _payload(
            labels=[
                "2024/12<br />(IFRS연결)",
                "2025/03(E)<br />(IFRS연결)",
                "2025/06<br />(IFRS연결)",
                "전년대비<br />(YoY)",
            ],
            rows=[
                {
                    "ACC_NM": "매출액(수익)",
                    "LVL": 1,
                    "DATA1": 10,
                    "DATA2": 999,
                    "DATA3": 30,
                },
            ],
        )

        df = _build_dataframe_from_payload("income", payload)

        self.assertEqual(list(df.columns), ["2024/12", "2025/06"])
        self.assertEqual(df.loc["매출액(수익)", "2024/12"], 10.0)
        self.assertEqual(df.loc["매출액(수익)", "2025/06"], 30.0)

    def test_income_payload_normalizes_legacy_metric_name(self):
        payload = _payload(
            labels=[
                "2024/12<br />(IFRS연결)",
                "2025/12(E)<br />(IFRS연결)",
            ],
            rows=[
                {"ACC_NM": "매출액(수익)", "LVL": 1, "DATA1": 10, "DATA2": 11},
                {"ACC_NM": "*[구.K-IFRS]영업이익", "LVL": 1, "DATA1": None, "DATA2": None},
                {"ACC_NM": "....제품매출액", "LVL": 3, "DATA1": 1, "DATA2": 2},
            ],
        )

        df = _build_dataframe_from_payload("income", payload)

        self.assertEqual(list(df.index), ["매출액(수익)", "*[구K-IFRS]영업이익"])
        self.assertNotIn("제품매출액", df.index)

    def test_balance_payload_keeps_level_one_and_three_rows(self):
        payload = _payload(
            labels=[
                "2024/12<br />(IFRS연결)",
                "2025/12(E)<br />(IFRS연결)",
            ],
            rows=[
                {"ACC_NM": "자산총계", "LVL": 1, "DATA1": 100, "DATA2": 110},
                {"ACC_NM": "....재고자산", "LVL": 3, "DATA1": 10, "DATA2": 11},
                {"ACC_NM": "........상품", "LVL": 4, "DATA1": 1, "DATA2": 2},
            ],
        )

        df = _build_dataframe_from_payload("balance", payload)

        self.assertEqual(list(df.index), ["자산총계", "재고자산"])
        self.assertNotIn("상품", df.index)

    def test_update_db_round_trip_preserves_browserless_shape(self):
        fixtures = {
            ("ratio", "year"): _build_dataframe_from_payload(
                "ratio",
                _payload(
                    labels=[
                        "2024/12<br />(IFRS연결)",
                        "2025/12(E)<br />(IFRS연결)",
                    ],
                    rows=[
                        {"ACC_NM": "EPS", "LVL": 1, "DATA1": 100, "DATA2": 120},
                        {"ACC_NM": "BPS", "LVL": 1, "DATA1": 1000, "DATA2": 1100},
                        {"ACC_NM": "현금배당성향(%)", "LVL": 1, "DATA1": 15, "DATA2": 18},
                    ],
                ),
            ),
            ("income", "year"): _build_dataframe_from_payload(
                "income",
                _payload(
                    labels=["2024/12<br />(IFRS연결)", "2025/12(E)<br />(IFRS연결)"],
                    rows=[
                        {"ACC_NM": "매출액(수익)", "LVL": 1, "DATA1": 10, "DATA2": 11},
                        {"ACC_NM": "영업이익", "LVL": 1, "DATA1": 2, "DATA2": 3},
                        {"ACC_NM": "*[구.K-IFRS]영업이익", "LVL": 1, "DATA1": None, "DATA2": None},
                    ],
                ),
            ),
            ("balance", "year"): _build_dataframe_from_payload(
                "balance",
                _payload(
                    labels=["2024/12<br />(IFRS연결)", "2025/12(E)<br />(IFRS연결)"],
                    rows=[
                        {"ACC_NM": "자산총계", "LVL": 1, "DATA1": 100, "DATA2": 110},
                        {"ACC_NM": "....재고자산", "LVL": 3, "DATA1": 10, "DATA2": 11},
                    ],
                ),
            ),
            ("cashflow", "year"): _build_dataframe_from_payload(
                "cashflow",
                _payload(
                    labels=["2024/12<br />(IFRS연결)", "2025/12(E)<br />(IFRS연결)"],
                    rows=[
                        {"ACC_NM": "영업활동으로인한현금흐름", "LVL": 1, "DATA1": 7, "DATA2": 8},
                        {"ACC_NM": "....당기순이익", "LVL": 3, "DATA1": 3, "DATA2": 4},
                    ],
                ),
            ),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            db_map = {
                group_key: Path(temp_dir) / f"{group_key[0]}_{group_key[1]}.db"
                for group_key in fixtures
            }
            with patch("tools.financial_db.GROUP_DB_MAP", db_map):
                for (report_type, period), df in fixtures.items():
                    with self.subTest(report_type=report_type, period=period):
                        update_db(report_type, period, "005930", df)
                        loaded = load_db(report_type, period, "005930")

                        pd.testing.assert_index_equal(loaded.index, df.index)
                        pd.testing.assert_index_equal(loaded.columns, df.columns)
                        pd.testing.assert_frame_equal(
                            loaded,
                            df.apply(pd.to_numeric, errors="coerce"),
                            check_dtype=False,
                        )

    def test_update_db_clears_stale_values_for_missing_ratio_metric(self):
        initial_df = _build_dataframe_from_payload(
            "ratio",
            _payload(
                labels=["2024/12<br />(IFRS연결)"],
                rows=[
                    {"ACC_NM": "EPS", "LVL": 1, "DATA1": 100},
                    {"ACC_NM": "BPS", "LVL": 1, "DATA1": 1000},
                ],
            ),
        )
        refreshed_df = _build_dataframe_from_payload(
            "ratio",
            _payload(
                labels=["2024/12<br />(IFRS연결)"],
                rows=[
                    {"ACC_NM": "EPS", "LVL": 1, "DATA1": 120},
                ],
            ),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ratio_year.db"
            with patch("tools.financial_db.GROUP_DB_MAP", {("ratio", "year"): db_path}):
                update_db("ratio", "year", "005930", initial_df)
                update_db("ratio", "year", "005930", refreshed_df)

                loaded = load_db("ratio", "year", "005930")

                self.assertEqual(loaded.loc["EPS", "2024/12"], 120.0)
                self.assertTrue(pd.isna(loaded.loc["BPS", "2024/12"]))

    def test_update_db_removes_stale_non_ratio_rows(self):
        initial_df = _build_dataframe_from_payload(
            "income",
            _payload(
                labels=["2024/12<br />(IFRS연결)"],
                rows=[
                    {"ACC_NM": "매출액(수익)", "LVL": 1, "DATA1": 10},
                    {"ACC_NM": "영업이익", "LVL": 1, "DATA1": 2},
                ],
            ),
        )
        refreshed_df = _build_dataframe_from_payload(
            "income",
            _payload(
                labels=["2024/12<br />(IFRS연결)"],
                rows=[
                    {"ACC_NM": "매출액(수익)", "LVL": 1, "DATA1": 11},
                ],
            ),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "income_year.db"
            with patch("tools.financial_db.GROUP_DB_MAP", {("income", "year"): db_path}):
                update_db("income", "year", "005930", initial_df, drop_missing_metrics=True)
                update_db("income", "year", "005930", refreshed_df, drop_missing_metrics=True)

                loaded = load_db("income", "year", "005930")

                self.assertEqual(list(loaded.index), ["매출액(수익)"])
                self.assertEqual(loaded.loc["매출액(수익)", "2024/12"], 11.0)

    def test_crawl_refreshes_page_context_when_payload_shape_is_invalid(self):
        class RefreshingCrawler(FinancialCrawler):
            def __init__(self):
                super().__init__()
                self.calls: list[bool] = []

            def _fetch_payload(
                self,
                code: str,
                report_type: str,
                period: str,
                *,
                max_retries: int,
                force_refresh: bool = False,
            ):
                self.calls.append(force_refresh)
                if not force_refresh:
                    return {"YYMM": [], "DATA": []}

                return _payload(
                    labels=["2024/12<br />(IFRS연결)"],
                    rows=[{"ACC_NM": "EPS", "LVL": 1, "DATA1": 100}],
                )

        crawler = RefreshingCrawler()
        try:
            df = crawler.crawl("005930", report_type="ratio", period="year")
        finally:
            crawler.close()

        self.assertEqual(crawler.calls, [False, True])
        self.assertEqual(df.loc["EPS", "2024/12"], 100.0)

    def test_load_page_context_retries_missing_encparam(self):
        class ContextRetryCrawler(FinancialCrawler):
            def __init__(self):
                super().__init__()
                self.calls = 0

            def _request_text(self, *args, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    return "<html><body>temporary page</body></html>"
                return "<script>var param = { encparam: 'abc123' };</script>"

        crawler = ContextRetryCrawler()
        try:
            context = crawler._load_page_context("001540", "income", max_retries=3)
        finally:
            crawler.close()

        self.assertEqual(crawler.calls, 2)
        self.assertEqual(context.encparam, "abc123")

    def test_load_page_context_marks_unsupported_symbol(self):
        class UnsupportedCrawler(FinancialCrawler):
            def _request_text(self, *args, **kwargs):
                return "<script>alert('올바른 종목이 아닙니다.');location.replace('../company/c1030001.aspx?cn=&cmp_cd=005930&menuType=block');</script>"

        crawler = UnsupportedCrawler()
        try:
            with self.assertRaises(UnsupportedWiseReportSymbol):
                crawler._load_page_context("008110", "income", max_retries=2)
        finally:
            crawler.close()

    def test_fetch_reports_collapses_unsupported_symbol_to_single_skip(self):
        with patch(
            "pipelines.financial_crawler.FinancialCrawler.crawl",
            side_effect=UnsupportedWiseReportSymbol("WiseReport does not support symbol 008110"),
        ):
            results, failures = fetch_reports("008110", REPORT_TASKS, max_retry=2)

        self.assertEqual(results, [])
        self.assertEqual(failures, ["skipped (WiseReport does not support symbol 008110)"])

    def test_run_parallel_discards_partial_symbol_results(self):
        frame = pd.DataFrame(
            [[1.0]],
            index=pd.Index(["EPS"], name="metric"),
            columns=pd.Index(["2024/12"]),
        )
        names = pd.Series({"005930": "삼성전자"})

        with patch(
            "pipelines.financial_crawler._process_single_code",
            return_value=([
                ("ratio", "year", frame),
            ], ["income:year - exception occurred (boom)"]),
        ):
            successful_codes, failures, collected_reports = _run_parallel(
                ["005930"],
                names,
                [("ratio", "year")],
                1,
            )

        self.assertEqual(successful_codes, set())
        self.assertEqual(len(collected_reports), 0)
        self.assertIn("005930 (삼성전자)", failures[0])

    def test_apply_updates_rolls_back_partial_symbol_writes(self):
        initial_ratio = _build_dataframe_from_payload(
            "ratio",
            _payload(
                labels=["2024/12<br />(IFRS연결)"],
                rows=[{"ACC_NM": "EPS", "LVL": 1, "DATA1": 100}],
            ),
        )
        initial_income = _build_dataframe_from_payload(
            "income",
            _payload(
                labels=["2024/12<br />(IFRS연결)"],
                rows=[{"ACC_NM": "매출액(수익)", "LVL": 1, "DATA1": 10}],
            ),
        )
        next_ratio = _build_dataframe_from_payload(
            "ratio",
            _payload(
                labels=["2024/12<br />(IFRS연결)"],
                rows=[{"ACC_NM": "EPS", "LVL": 1, "DATA1": 120}],
            ),
        )
        next_income = _build_dataframe_from_payload(
            "income",
            _payload(
                labels=["2024/12<br />(IFRS연결)"],
                rows=[{"ACC_NM": "매출액(수익)", "LVL": 1, "DATA1": 12}],
            ),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            db_map = {
                ("ratio", "year"): Path(temp_dir) / "ratio_year.db",
                ("income", "year"): Path(temp_dir) / "income_year.db",
            }
            with patch("tools.financial_db.GROUP_DB_MAP", db_map):
                update_db("ratio", "year", "005930", initial_ratio, drop_missing_metrics=True)
                update_db("income", "year", "005930", initial_income, drop_missing_metrics=True)

                call_count = {"value": 0}
                real_write_dataframe = financial_db_module._write_dataframe

                def flaky_write_dataframe(*args, **kwargs):
                    if call_count["value"] == 1:
                        raise RuntimeError("boom")
                    call_count["value"] += 1
                    return real_write_dataframe(*args, **kwargs)

                with patch("tools.financial_db._write_dataframe", side_effect=flaky_write_dataframe):
                    failures = _apply_updates(
                        [
                            ("ratio", "year", "005930", "삼성전자", next_ratio),
                            ("income", "year", "005930", "삼성전자", next_income),
                        ],
                        {"005930"},
                    )

                loaded_ratio = load_db("ratio", "year", "005930")
                loaded_income = load_db("income", "year", "005930")

                self.assertEqual(loaded_ratio.loc["EPS", "2024/12"], 100.0)
                self.assertEqual(loaded_income.loc["매출액(수익)", "2024/12"], 10.0)
                self.assertEqual(len(failures), 1)

    def test_batch_writer_reuses_single_sqlite_connection(self):
        ratio_df = _build_dataframe_from_payload(
            "ratio",
            _payload(
                labels=["2024/12<br />(IFRS연결)"],
                rows=[{"ACC_NM": "EPS", "LVL": 1, "DATA1": 100}],
            ),
        )
        income_df = _build_dataframe_from_payload(
            "income",
            _payload(
                labels=["2024/12<br />(IFRS연결)"],
                rows=[{"ACC_NM": "매출액(수익)", "LVL": 1, "DATA1": 10}],
            ),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            db_map = {
                ("ratio", "year"): Path(temp_dir) / "ratio_year.db",
                ("income", "year"): Path(temp_dir) / "income_year.db",
            }
            real_connect = sqlite3.connect
            with patch("tools.financial_db.sqlite3.connect", wraps=real_connect) as connect_mock:
                with FinancialDBBatchWriter(db_map) as batch_writer:
                    batch_writer.write_symbol_reports(
                        [("ratio", "year", "005930", ratio_df)],
                        drop_missing_metrics=True,
                    )
                    batch_writer.write_symbol_reports(
                        [("income", "year", "005930", income_df)],
                        drop_missing_metrics=True,
                    )

            self.assertEqual(connect_mock.call_count, 1)


if __name__ == "__main__":
    unittest.main()
