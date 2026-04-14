import os
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.quant_utils import (
    _evaluate_ranked_candidates,
    create_stock_objects,
    get_average_amount,
    get_quant_factors,
    get_stock_quote,
    select_stocks,
)
from tools.selection_store import _fetch_latest_quotes, load_stock_selection, save_stock_selection


def _make_fake_krx_reader() -> MagicMock:
    reader = MagicMock()
    reader.__enter__.return_value = reader
    reader.__exit__.return_value = None
    reader.compute_weekly_volatility.return_value = {}
    reader.compute_amounts.return_value = {}
    return reader


class TestStockSelectionOptimizations(unittest.TestCase):
    def test_create_stock_objects_is_lazy_and_cached(self):
        kis = MagicMock()
        first_stock = MagicMock(name="first_stock")
        kis.stock.return_value = first_stock
        df = pd.DataFrame(
            [
                {"단축코드": "005930", "한글명": "삼성전자"},
                {"단축코드": "000660", "한글명": "SK하이닉스"},
            ]
        )

        stocks = create_stock_objects(df, kis)

        self.assertEqual(kis.stock.call_count, 0)
        self.assertIs(stocks["005930"], first_stock)
        self.assertEqual(kis.stock.call_count, 1)
        self.assertIs(stocks.get("005930"), first_stock)
        self.assertEqual(kis.stock.call_count, 1)

    def test_select_stocks_filters_risky_candidates_before_quote_stage(self):
        df_codes = pd.DataFrame(
            [
                {
                    "단축코드": "SAFE",
                    "한글명": "안전주",
                    "거래정지": "N",
                    "정리매매": "N",
                    "관리종목": "N",
                    "시장경고": "0",
                    "경고예고": "N",
                },
                {
                    "단축코드": "RISK",
                    "한글명": "위험주",
                    "거래정지": "Y",
                    "정리매매": "N",
                    "관리종목": "N",
                    "시장경고": "0",
                    "경고예고": "N",
                },
            ]
        )

        def fake_quotes(df: pd.DataFrame, stocks: object) -> pd.DataFrame:
            self.assertEqual(df["단축코드"].tolist(), ["SAFE"])
            return df.assign(price=100.0, market_cap=1_000_000.0)

        fake_krx_reader = _make_fake_krx_reader()

        with patch("tools.quant_utils.get_stock_quote", side_effect=fake_quotes), patch(
            "tools.quant_utils.get_quant_factors",
            side_effect=lambda df, stocks, reader=None, volatility_map=None: df.assign(
                **{
                    "1/per": [1.0],
                    "1/pbr": [1.0],
                    "1/psr": [1.0],
                    "1/pcr": [1.0],
                    "poir_q": [1.0],
                    "poir_y": [1.0],
                    "peir_q": [1.0],
                    "peir_y": [1.0],
                    "gp/a": [1.0],
                    "asset_shrink": [0.0],
                    "income_to_debt_growth": [1.0],
                    "volatility": [1.0],
                }
            ),
        ), patch(
            "tools.quant_utils.get_rank",
            side_effect=lambda df, **kwargs: df.assign(
                rank_total=1.0,
                rank_value=1.0,
                rank_momentum=1.0,
                rank_quality=1.0,
            ),
        ), patch(
            "tools.quant_utils.get_average_amount",
            side_effect=lambda df, stocks, krx_reader=None: df.assign(amount=100_000_000.0),
        ), patch(
            "tools.quant_utils.get_f_scores",
            side_effect=lambda df, stocks, reader=None, paidin_event_symbols=None: df.assign(F_score=3),
        ), patch(
            "tools.quant_utils.KrxOHLCVReader",
            return_value=fake_krx_reader,
        ):
            selected = cast(pd.DataFrame, select_stocks(df_codes, {"SAFE": MagicMock()}, top_n=1))

        self.assertEqual(selected["단축코드"].tolist(), ["SAFE"])

    def test_select_stocks_applies_smallcap_filter_before_factor_stage(self):
        df_codes = pd.DataFrame(
            [
                {"단축코드": "A", "한글명": "A", "거래정지": "N", "정리매매": "N", "관리종목": "N", "시장경고": "0", "경고예고": "N"},
                {"단축코드": "B", "한글명": "B", "거래정지": "N", "정리매매": "N", "관리종목": "N", "시장경고": "0", "경고예고": "N"},
                {"단축코드": "C", "한글명": "C", "거래정지": "N", "정리매매": "N", "관리종목": "N", "시장경고": "0", "경고예고": "N"},
            ]
        )

        def fake_factors(df: pd.DataFrame, stocks: object) -> pd.DataFrame:
            self.assertEqual(df["단축코드"].tolist(), ["A"])
            return df.assign(
                **{
                    "1/per": 1.0,
                    "1/pbr": 1.0,
                    "1/psr": 1.0,
                    "1/pcr": 1.0,
                    "poir_q": 1.0,
                    "poir_y": 1.0,
                    "peir_q": 1.0,
                    "peir_y": 1.0,
                    "gp/a": 1.0,
                    "asset_shrink": 0.0,
                    "income_to_debt_growth": 1.0,
                    "volatility": 1.0,
                    "__fscore_eligible": True,
                }
            )

        fake_krx_reader = _make_fake_krx_reader()

        with patch(
            "tools.quant_utils.get_stock_quote",
            return_value=df_codes.assign(price=[100.0, 100.0, 100.0], market_cap=[10.0, 20.0, 100.0]),
        ), patch(
            "tools.quant_utils.get_quant_factors",
            side_effect=lambda df, stocks, reader=None, volatility_map=None: fake_factors(df, stocks),
        ), patch(
            "tools.quant_utils.get_rank",
            side_effect=lambda df, **kwargs: df.assign(
                rank_total=1.0,
                rank_value=1.0,
                rank_momentum=1.0,
                rank_quality=1.0,
            ),
        ), patch(
            "tools.quant_utils.get_average_amount",
            side_effect=lambda df, stocks, krx_reader=None: df.assign(amount=100_000_000.0),
        ), patch(
            "tools.quant_utils.get_f_scores",
            side_effect=lambda df, stocks, reader=None, paidin_event_symbols=None: df.assign(F_score=3),
        ), patch(
            "tools.quant_utils.KrxOHLCVReader",
            return_value=fake_krx_reader,
        ):
            selected = cast(pd.DataFrame, select_stocks(df_codes, {"A": MagicMock()}, top_n=1))

        self.assertEqual(selected["단축코드"].tolist(), ["A"])

    def test_select_stocks_fetches_amount_after_ranking_subset(self):
        df_codes = pd.DataFrame(
            [
                {"단축코드": "A", "한글명": "A", "거래정지": "N", "정리매매": "N", "관리종목": "N", "시장경고": "0", "경고예고": "N"},
                {"단축코드": "B", "한글명": "B", "거래정지": "N", "정리매매": "N", "관리종목": "N", "시장경고": "0", "경고예고": "N"},
                {"단축코드": "C", "한글명": "C", "거래정지": "N", "정리매매": "N", "관리종목": "N", "시장경고": "0", "경고예고": "N"},
            ]
        )
        stocks = {code: MagicMock(name=code) for code in ["A", "B", "C"]}
        fake_krx_reader = _make_fake_krx_reader()

        with patch(
            "tools.quant_utils.get_stock_quote",
            return_value=df_codes.assign(price=[100.0, 100.0, 100.0], market_cap=[10.0, 20.0, 100.0]),
        ), patch(
            "tools.quant_utils.get_quant_factors",
            side_effect=lambda df, stocks, reader=None, volatility_map=None: df.assign(
                **{
                    "1/per": 1.0,
                    "1/pbr": 1.0,
                    "1/psr": 1.0,
                    "1/pcr": 1.0,
                    "poir_q": 1.0,
                    "poir_y": 1.0,
                    "peir_q": 1.0,
                    "peir_y": 1.0,
                    "gp/a": 1.0,
                    "asset_shrink": 0.0,
                    "income_to_debt_growth": 1.0,
                    "volatility": 1.0,
                }
            ),
        ), patch(
            "tools.quant_utils.get_rank",
            side_effect=lambda df, **kwargs: df.assign(
                rank_total=range(1, len(df) + 1),
                rank_value=1.0,
                rank_momentum=1.0,
                rank_quality=1.0,
            ),
        ), patch(
            "tools.quant_utils.get_average_amount",
            side_effect=lambda df, stocks, krx_reader=None: df.assign(
                amount=[100_000_000.0 if idx == 0 else 10_000.0 for idx in range(len(df))]
            ),
        ) as amount_mock, patch(
            "tools.quant_utils.get_f_scores",
            side_effect=lambda df, stocks, reader=None, paidin_event_symbols=None: df.assign(F_score=3),
        ) as f_score_mock, patch(
            "tools.quant_utils.KrxOHLCVReader",
            return_value=fake_krx_reader,
        ):
            selected = cast(pd.DataFrame, select_stocks(df_codes, stocks, top_n=1))

        self.assertEqual(amount_mock.call_count, 1)
        amount_input = amount_mock.call_args.args[0]
        f_score_input = f_score_mock.call_args.args[0]
        self.assertEqual(amount_input["단축코드"].tolist(), ["A"])
        self.assertEqual(f_score_input["단축코드"].tolist(), ["A"])
        self.assertEqual(selected["단축코드"].tolist(), ["A"])

    def test_select_stocks_include_full_data_returns_post_filter_snapshot(self):
        df_codes = pd.DataFrame(
            [
                {"단축코드": "A", "한글명": "A", "거래정지": "N", "정리매매": "N", "관리종목": "N", "시장경고": "0", "경고예고": "N"},
                {"단축코드": "B", "한글명": "B", "거래정지": "N", "정리매매": "N", "관리종목": "N", "시장경고": "0", "경고예고": "N"},
            ]
        )
        fake_krx_reader = _make_fake_krx_reader()

        with patch(
            "tools.quant_utils.get_stock_quote",
            return_value=df_codes.assign(price=[100.0, 100.0], market_cap=[10.0, 20.0]),
        ), patch(
            "tools.quant_utils.get_quant_factors",
            side_effect=lambda df, stocks, reader=None, volatility_map=None: df.assign(
                **{
                    "1/per": 1.0,
                    "1/pbr": 1.0,
                    "1/psr": 1.0,
                    "1/pcr": 1.0,
                    "poir_q": 1.0,
                    "poir_y": 1.0,
                    "peir_q": 1.0,
                    "peir_y": 1.0,
                    "gp/a": 1.0,
                    "asset_shrink": 0.0,
                    "income_to_debt_growth": 1.0,
                    "volatility": 1.0,
                }
            ),
        ), patch(
            "tools.quant_utils.get_rank",
            side_effect=lambda df, **kwargs: df.assign(
                rank_total=range(1, len(df) + 1),
                rank_value=1.0,
                rank_momentum=1.0,
                rank_quality=1.0,
            ),
        ), patch(
            "tools.quant_utils.get_average_amount",
            side_effect=lambda df, stocks, krx_reader=None: df.assign(
                amount=[100_000_000.0 if idx == 0 else 10_000.0 for idx in range(len(df))]
            ),
        ), patch(
            "tools.quant_utils.get_f_scores",
            side_effect=lambda df, stocks, reader=None, paidin_event_symbols=None: df.assign(F_score=3),
        ), patch(
            "tools.quant_utils.KrxOHLCVReader",
            return_value=fake_krx_reader,
        ):
            selected, snapshot = select_stocks(df_codes, {"A": MagicMock(), "B": MagicMock()}, top_n=1, include_full_data=True)

        self.assertEqual(selected["단축코드"].tolist(), ["A"])
        self.assertEqual(snapshot["단축코드"].tolist(), ["A"])
        self.assertEqual(snapshot["amount"].tolist(), [100_000_000.0])

    def test_select_stocks_prefetches_financials_for_smallcap_subset(self):
        df_codes = pd.DataFrame(
            [
                {"단축코드": "A", "한글명": "A", "거래정지": "N", "정리매매": "N", "관리종목": "N", "시장경고": "0", "경고예고": "N"},
                {"단축코드": "B", "한글명": "B", "거래정지": "N", "정리매매": "N", "관리종목": "N", "시장경고": "0", "경고예고": "N"},
                {"단축코드": "C", "한글명": "C", "거래정지": "N", "정리매매": "N", "관리종목": "N", "시장경고": "0", "경고예고": "N"},
            ]
        )
        fake_reader = MagicMock()
        fake_reader.__enter__.return_value = fake_reader
        fake_reader.__exit__.return_value = None
        fake_krx_reader = _make_fake_krx_reader()

        with patch(
            "tools.quant_utils.get_stock_quote",
            return_value=df_codes.assign(price=[100.0, 100.0, 100.0], market_cap=[10.0, 20.0, 100.0]),
        ), patch(
            "tools.quant_utils.get_quant_factors",
            side_effect=lambda df, stocks, reader=None, volatility_map=None: df.assign(
                **{
                    "1/per": 1.0,
                    "1/pbr": 1.0,
                    "1/psr": 1.0,
                    "1/pcr": 1.0,
                    "poir_q": 1.0,
                    "poir_y": 1.0,
                    "peir_q": 1.0,
                    "peir_y": 1.0,
                    "gp/a": 1.0,
                    "asset_shrink": 0.0,
                    "income_to_debt_growth": 1.0,
                    "volatility": 1.0,
                    "__fscore_eligible": True,
                }
            ),
        ), patch(
            "tools.quant_utils.get_rank",
            side_effect=lambda df, **kwargs: df.assign(
                rank_total=range(1, len(df) + 1),
                rank_value=1.0,
                rank_momentum=1.0,
                rank_quality=1.0,
            ),
        ), patch(
            "tools.quant_utils.get_average_amount",
            side_effect=lambda df, stocks, krx_reader=None: df.assign(amount=100_000_000.0),
        ), patch(
            "tools.quant_utils.get_f_scores",
            side_effect=lambda df, stocks, reader=None, paidin_event_symbols=None: df.assign(F_score=3),
        ), patch(
            "tools.quant_utils.FinancialDBReader",
            return_value=fake_reader,
        ), patch(
            "tools.quant_utils.KrxOHLCVReader",
            return_value=fake_krx_reader,
        ):
            select_stocks(df_codes, {"A": MagicMock()}, top_n=1)

        fake_reader.prefetch_quarter_statements.assert_called_once_with(["A"])

    def test_get_average_amount_uses_krx_reader_values(self):
        df = pd.DataFrame(
            [
                {"단축코드": "A"},
                {"단축코드": "B"},
            ]
        )
        stocks = {"A": MagicMock(), "B": MagicMock()}
        fake_krx_reader = MagicMock()
        fake_krx_reader.compute_amounts.return_value = {"A": 111, "B": 222}

        result = get_average_amount(df, stocks, krx_reader=fake_krx_reader)

        self.assertEqual(result["amount"].tolist(), [111, 222])

    def test_get_average_amount_falls_back_when_krx_reader_has_no_data(self):
        df = pd.DataFrame(
            [
                {"단축코드": "A"},
            ]
        )
        stock = MagicMock()
        stock.daily_chart.return_value = MagicMock(bars=[MagicMock(amount=300), MagicMock(amount=900)])
        stocks = {"A": stock}
        fake_krx_reader = MagicMock()
        fake_krx_reader.compute_amounts.return_value = {}

        result = get_average_amount(df, stocks, krx_reader=fake_krx_reader)

        self.assertEqual(result.loc[0, "amount"], 600)

    def test_get_quant_factors_uses_prefetched_volatility_map(self):
        df = pd.DataFrame(
            [
                {"단축코드": "A", "price": 100.0, "market_cap": 10.0},
            ]
        )
        stocks = {"A": MagicMock()}
        sample_frame = pd.DataFrame(
            [[1.0, 2.0]],
            index=pd.Index(["EPS", "BPS"]),
            columns=pd.Index(["2024/12", "2024/09"]),
        )
        fake_reader = MagicMock()
        fake_reader.load_quarter_statements.return_value = (
            sample_frame,
            pd.DataFrame([[1.0, 2.0]], index=pd.Index(["영업이익"]), columns=pd.Index(["2024/12", "2024/09"])),
            pd.DataFrame([[10.0, 11.0], [1.0, 1.0], [1.0, 1.0]], index=pd.Index(["자산총계", "단기차입금", "장기차입금"]), columns=pd.Index(["2024/12", "2024/09"])),
            pd.DataFrame([[1.0, 2.0]], index=pd.Index(["영업활동으로인한현금흐름"]), columns=pd.Index(["2024/12", "2024/09"])),
        )

        with patch("tools.quant_utils._compute_volatility") as volatility_mock:
            result = get_quant_factors(df, stocks, reader=fake_reader, volatility_map={"A": (-1.5, True)})

        volatility_mock.assert_not_called()
        self.assertEqual(result.loc[0, "volatility"], -1.5)

    def test_get_stock_quote_ignores_incoming_market_cap_for_kis_refresh(self):
        df = pd.DataFrame(
            [
                {"단축코드": "005930", "시장구분": "KOSPI", "market_cap": 999.0},
            ]
        )
        stocks = create_stock_objects(df, MagicMock())

        with patch(
            "tools.quant_utils.fetch_latest_quotes_batch",
            side_effect=lambda df, kis, retry, progress_desc: df.assign(price=100.0, market_cap=123.0),
        ):
            result = get_stock_quote(df, stocks)

        self.assertEqual(result.loc[0, "market_cap"], 123.0)

    def test_evaluate_ranked_candidates_attempts_bulk_paidin_fetch_once(self):
        class Stocks(dict):
            kis: Any
            pass

        stocks = Stocks({str(i): MagicMock(name=str(i)) for i in range(25)})
        stocks.kis = MagicMock()
        df_ranked = pd.DataFrame(
            {
                "단축코드": [str(i) for i in range(25)],
                "한글명": [str(i) for i in range(25)],
                "asset_shrink": [0.0] * 25,
                "rank_total": list(range(1, 26)),
            }
        )

        def fake_amount(df: pd.DataFrame, stocks_obj: object, krx_reader=None) -> pd.DataFrame:
            amounts = [100_000_000.0 if code in {"0", "1", "2", "20", "21"} else 10_000.0 for code in df["단축코드"]]
            return df.assign(amount=amounts)

        with patch(
            "tools.quant_utils.get_average_amount",
            side_effect=fake_amount,
        ), patch(
            "tools.quant_utils.get_f_scores",
            side_effect=lambda df, stocks, reader=None, paidin_event_symbols=None: df.assign(F_score=3),
        ), patch(
            "tools.quant_utils._fetch_paidin_event_symbols",
            return_value=None,
        ) as paidin_mock:
            result = _evaluate_ranked_candidates(
                df_ranked,
                stocks,
                top_n=5,
                include_full_data=False,
                reader=None,
            )

        paidin_mock.assert_called_once_with(stocks.kis)
        self.assertEqual(result["단축코드"].tolist(), ["0", "1", "2", "20", "21"])

    def test_evaluate_ranked_candidates_bulk_paidin_underlying_call_once_on_failure(self):
        class Stocks(dict):
            kis: Any
            pass

        stocks = Stocks({str(i): MagicMock(name=str(i)) for i in range(25)})
        stocks.kis = MagicMock()
        stocks.kis.paidin_capin.side_effect = RuntimeError("boom")
        df_ranked = pd.DataFrame(
            {
                "단축코드": [str(i) for i in range(25)],
                "한글명": [str(i) for i in range(25)],
                "asset_shrink": [0.0] * 25,
                "rank_total": list(range(1, 26)),
            }
        )

        def fake_amount(df: pd.DataFrame, stocks_obj: object, krx_reader=None) -> pd.DataFrame:
            amounts = [100_000_000.0 if code in {"0", "1", "2", "20", "21"} else 10_000.0 for code in df["단축코드"]]
            return df.assign(amount=amounts)

        with patch(
            "tools.quant_utils.get_average_amount",
            side_effect=fake_amount,
        ), patch(
            "tools.quant_utils.get_f_scores",
            side_effect=lambda df, stocks, reader=None, paidin_event_symbols=None: df.assign(F_score=3),
        ):
            result = _evaluate_ranked_candidates(
                df_ranked,
                stocks,
                top_n=5,
                include_full_data=False,
                reader=None,
            )

        stocks.kis.paidin_capin.assert_called_once()
        self.assertEqual(result["단축코드"].tolist(), ["0", "1", "2", "20", "21"])

    def test_evaluate_ranked_candidates_expands_windows_until_top_n(self):
        stocks = {str(i): MagicMock(name=str(i)) for i in range(25)}
        df_ranked = pd.DataFrame(
            {
                "단축코드": [str(i) for i in range(25)],
                "한글명": [str(i) for i in range(25)],
                "asset_shrink": [0.0] * 25,
                "rank_total": list(range(1, 26)),
            }
        )

        def fake_amount(df: pd.DataFrame, stocks_obj: object, krx_reader=None) -> pd.DataFrame:
            amounts = [100_000_000.0 if code in {"0", "1", "2", "20", "21"} else 10_000.0 for code in df["단축코드"]]
            return df.assign(amount=amounts)

        with patch(
            "tools.quant_utils.get_average_amount",
            side_effect=fake_amount,
        ), patch(
            "tools.quant_utils.get_f_scores",
            side_effect=lambda df, stocks, reader=None, paidin_event_symbols=None: df.assign(F_score=3),
        ):
            result = _evaluate_ranked_candidates(
                df_ranked,
                stocks,
                top_n=5,
                include_full_data=False,
                reader=None,
            )

        self.assertEqual(result["단축코드"].tolist(), ["0", "1", "2", "20", "21"])

    def test_select_stocks_include_full_data_returns_tuple_when_empty(self):
        df_codes = pd.DataFrame(
            [
                {"단축코드": "A", "한글명": "A", "거래정지": "Y", "정리매매": "N", "관리종목": "N", "시장경고": "0", "경고예고": "N"},
            ]
        )

        selected, snapshot = select_stocks(
            df_codes,
            {},
            top_n=1,
            include_full_data=True,
        )

        self.assertIsInstance(selected, pd.DataFrame)
        self.assertIsInstance(snapshot, pd.DataFrame)
        self.assertEqual(list(selected.columns), ["단축코드", "한글명"])
        self.assertEqual(list(snapshot.columns), ["단축코드", "한글명"])
        self.assertTrue(selected.empty)
        self.assertTrue(snapshot.empty)

    def test_select_stocks_handles_all_volatility_early_returns(self):
        df_codes = pd.DataFrame(
            [
                {"단축코드": "A", "한글명": "A", "거래정지": "N", "정리매매": "N", "관리종목": "N", "시장경고": "0", "경고예고": "N"},
            ]
        )
        fake_krx_reader = _make_fake_krx_reader()

        with patch(
            "tools.quant_utils.get_stock_quote",
            return_value=df_codes.assign(price=[100.0], market_cap=[10.0]),
        ), patch(
            "tools.quant_utils._compute_volatility",
            return_value=(float("nan"), False),
        ), patch(
            "tools.quant_utils.get_average_amount",
            side_effect=lambda df, stocks, krx_reader=None: df.assign(amount=100_000_000.0),
        ), patch(
            "tools.quant_utils.KrxOHLCVReader",
            return_value=fake_krx_reader,
        ):
            selected, snapshot = select_stocks(
                df_codes,
                {"A": MagicMock()},
                top_n=1,
                include_full_data=True,
            )

        self.assertEqual(selected["단축코드"].tolist(), ["A"])
        self.assertIn("1/per", snapshot.columns)
        self.assertIn("volatility", snapshot.columns)
        self.assertNotIn("__fscore_eligible", snapshot.columns)

    def test_load_stock_selection_rerank_fetches_amount_when_snapshot_lacks_it(self):
        snapshot = pd.DataFrame(
            [
                {
                    "단축코드": "005930",
                    "한글명": "삼성전자",
                    "eps": 1.0,
                    "bps": 1.0,
                    "sps": 1.0,
                    "cps": 1.0,
                    "delta_oper_income_q": 1.0,
                    "delta_oper_income_y": 1.0,
                    "delta_earnings_q": 1.0,
                    "delta_earnings_y": 1.0,
                    "asset_shrink": 0.0,
                    "F_score": 3,
                    "gp/a": 1.0,
                    "income_to_debt_growth": 1.0,
                    "volatility": 1.0,
                }
            ]
        )

        reranked = snapshot.assign(
            price=100.0,
            market_cap=1_000_000.0,
            **{
                "1/per": 1.0,
                "1/pbr": 1.0,
                "1/psr": 1.0,
                "1/pcr": 1.0,
                "poir_q": 1.0,
                "poir_y": 1.0,
                "peir_q": 1.0,
                "peir_y": 1.0,
                "rank_total": 1.0,
                "rank_value": 1.0,
                "rank_momentum": 1.0,
                "rank_quality": 1.0,
            },
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "stock_selection_test.db"
            with patch("tools.selection_store.get_stock_selection_db_path", return_value=db_path):
                save_stock_selection(snapshot, table_date="20260406", strategy_id="krx_vmq")

                with patch(
                    "tools.selection_store._fetch_latest_quotes",
                    return_value=reranked,
                ), patch(
                    "tools.selection_store._recalculate_dynamic_metrics",
                    return_value=reranked,
                ), patch(
                    "tools.selection_store.apply_risk_filters",
                    side_effect=lambda df: df,
                ), patch(
                    "tools.selection_store.get_rank",
                    return_value=reranked,
                ), patch(
                    "tools.selection_store._fetch_latest_amounts",
                    return_value=reranked.assign(amount=12345.0),
                ) as amount_mock, patch(
                    "tools.selection_store.apply_custom_selection_filters",
                    side_effect=lambda df: df,
                ):
                    result = load_stock_selection(
                        table_date="20260406",
                        kis=MagicMock(),
                        rerank=True,
                        top_n=20,
                        strategy_id="krx_vmq",
                    )

        amount_mock.assert_called_once()
        self.assertEqual(result.loc[0, "amount"], 12345.0)

    def test_save_and_load_stock_selection_use_strategy_db_for_krx_vmq(self):
        snapshot = pd.DataFrame(
            [
                {
                    "단축코드": "005930",
                    "한글명": "삼성전자",
                }
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            db_dir = Path(temp_dir) / "db"
            quant_dir = db_dir / "quant"
            legacy_db_path = quant_dir / "stock_selection.db"
            vmq_db_path = quant_dir / "stock_selection_krx_vmq.db"
            quant_dir.mkdir(parents=True, exist_ok=True)

            with patch("tools.financial_db.DB_DIR", db_dir), patch(
                "tools.financial_db.QUANT_DATA_DIR", quant_dir,
            ), patch("tools.financial_db.STOCK_SELECTION_DB_PATH", legacy_db_path), patch(
                "tools.selection_store.DB_DIR", db_dir,
            ):
                save_stock_selection(snapshot, table_date="20260406", strategy_id="krx_vmq")
                result = load_stock_selection(
                    table_date="20260406",
                    kis=MagicMock(),
                    rerank=False,
                    top_n=20,
                    strategy_id="krx_vmq",
                )

            self.assertFalse(legacy_db_path.exists())
            self.assertTrue(vmq_db_path.exists())
            self.assertEqual(result["단축코드"].tolist(), ["005930"])
            self.assertEqual(result["한글명"].tolist(), ["삼성전자"])

    def test_load_stock_selection_does_not_fall_back_to_legacy_shared_db_for_krx_vmq(self):
        snapshot = pd.DataFrame(
            [
                {
                    "단축코드": "005930",
                    "한글명": "삼성전자",
                }
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            db_dir = Path(temp_dir) / "db"
            quant_dir = db_dir / "quant"
            legacy_db_path = quant_dir / "stock_selection.db"
            vmq_db_path = quant_dir / "stock_selection_krx_vmq.db"
            quant_dir.mkdir(parents=True, exist_ok=True)

            with patch("tools.financial_db.DB_DIR", db_dir), patch(
                "tools.financial_db.QUANT_DATA_DIR", quant_dir,
            ), patch("tools.financial_db.STOCK_SELECTION_DB_PATH", legacy_db_path), patch(
                "tools.selection_store.DB_DIR", db_dir,
            ):
                save_stock_selection(snapshot, table_date="20260406")
                with self.assertRaises(KeyError):
                    load_stock_selection(
                        table_date="20260406",
                        kis=MagicMock(),
                        rerank=False,
                        top_n=20,
                        strategy_id="krx_vmq",
                    )

            self.assertTrue(legacy_db_path.exists())
            self.assertFalse(vmq_db_path.exists())

    def test_load_stock_selection_rerank_keeps_saved_amount_when_refresh_partial(self):
        snapshot = pd.DataFrame(
            [
                {
                    "단축코드": "005930",
                    "한글명": "삼성전자",
                    "price": 100.0,
                    "market_cap": 1_000_000.0,
                    "amount": 100_000_000.0,
                    "eps": 1.0,
                    "bps": 1.0,
                    "sps": 1.0,
                    "cps": 1.0,
                    "delta_oper_income_q": 1.0,
                    "delta_oper_income_y": 1.0,
                    "delta_earnings_q": 1.0,
                    "delta_earnings_y": 1.0,
                    "asset_shrink": 0.0,
                    "F_score": 3,
                    "gp/a": 1.0,
                    "income_to_debt_growth": 1.0,
                    "volatility": 1.0,
                    "rank_total": 1.0,
                    "rank_value": 1.0,
                    "rank_momentum": 1.0,
                    "rank_quality": 1.0,
                    "1/per": 1.0,
                    "1/pbr": 1.0,
                    "1/psr": 1.0,
                    "1/pcr": 1.0,
                    "poir_q": 1.0,
                    "poir_y": 1.0,
                    "peir_q": 1.0,
                    "peir_y": 1.0,
                }
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "stock_selection_test.db"
            with patch("tools.selection_store.get_stock_selection_db_path", return_value=db_path):
                save_stock_selection(snapshot, table_date="20260406", strategy_id="krx_vmq")

                with patch(
                    "tools.selection_store._fetch_latest_quotes",
                    return_value=snapshot,
                ), patch(
                    "tools.selection_store._recalculate_dynamic_metrics",
                    return_value=snapshot,
                ), patch(
                    "tools.selection_store.apply_risk_filters",
                    side_effect=lambda df: df,
                ), patch(
                    "tools.selection_store.get_rank",
                    return_value=snapshot,
                ), patch(
                    "tools.selection_store._fetch_latest_amounts",
                    return_value=snapshot.assign(amount=pd.Series([None], dtype=float)),
                ), patch(
                    "tools.selection_store.apply_custom_selection_filters",
                    side_effect=lambda df: df,
                ):
                    result = load_stock_selection(
                        table_date="20260406",
                        kis=MagicMock(),
                        rerank=True,
                        top_n=20,
                        strategy_id="krx_vmq",
                    )

        self.assertEqual(result.loc[0, "amount"], 100_000_000.0)

    def test_load_stock_selection_does_not_reapply_smallcap_filter_to_saved_snapshot(self):
        rows = []
        for index, market_cap in enumerate((10.0, 20.0, 30.0, 40.0, 50.0), start=1):
            rows.append(
                {
                    "단축코드": f"CODE{index}",
                    "한글명": f"종목{index}",
                    "price": 100.0 + index,
                    "market_cap": market_cap,
                    "amount": 100_000_000.0,
                    "eps": 1.0,
                    "bps": 1.0,
                    "sps": 1.0,
                    "cps": 1.0,
                    "delta_oper_income_q": 1.0,
                    "delta_oper_income_y": 1.0,
                    "delta_earnings_q": 1.0,
                    "delta_earnings_y": 1.0,
                    "asset_shrink": 0.0,
                    "F_score": 3,
                    "gp/a": 1.0,
                    "income_to_debt_growth": 1.0,
                    "volatility": 1.0,
                    "rank_total": float(index),
                    "rank_value": float(index),
                    "rank_momentum": float(index),
                    "rank_quality": float(index),
                    "1/per": 1.0,
                    "1/pbr": 1.0,
                    "1/psr": 1.0,
                    "1/pcr": 1.0,
                    "poir_q": 1.0,
                    "poir_y": 1.0,
                    "peir_q": 1.0,
                    "peir_y": 1.0,
                }
            )

        snapshot = pd.DataFrame(rows)

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "stock_selection_test.db"
            with patch("tools.selection_store.get_stock_selection_db_path", return_value=db_path):
                save_stock_selection(snapshot, table_date="20260406", strategy_id="krx_vmq")

                with patch(
                    "tools.selection_store._fetch_latest_quotes",
                    return_value=snapshot,
                ), patch(
                    "tools.selection_store._recalculate_dynamic_metrics",
                    return_value=snapshot,
                ), patch(
                    "tools.selection_store.apply_risk_filters",
                    side_effect=lambda df: df,
                ), patch(
                    "tools.selection_store.get_rank",
                    return_value=snapshot,
                ), patch(
                    "tools.selection_store._fetch_latest_amounts",
                    return_value=snapshot,
                ), patch(
                    "tools.selection_store.apply_custom_selection_filters",
                    side_effect=lambda df: df,
                ):
                    result = load_stock_selection(
                        table_date="20260406",
                        kis=MagicMock(),
                        rerank=True,
                        top_n=5,
                        strategy_id="krx_vmq",
                    )

        self.assertEqual(result["단축코드"].tolist(), [f"CODE{index}" for index in range(1, 6)])

    def test_fetch_latest_quotes_preserves_existing_market_cap(self):
        df = pd.DataFrame(
            [
                {"단축코드": "005930", "시장구분": "KOSPI", "market_cap": 1.0},
            ]
        )
        with patch(
            "tools.selection_store.fetch_latest_quotes_batch",
            side_effect=lambda df, kis, retry, progress_desc: df.assign(price=100.0),
        ):
            result = _fetch_latest_quotes(df, MagicMock(), retry=1)

        self.assertEqual(result.loc[0, "market_cap"], 1.0)


if __name__ == "__main__":
    unittest.main()
