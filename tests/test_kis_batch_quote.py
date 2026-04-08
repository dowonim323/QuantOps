import os
import sys
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.kis_batch_quote import fetch_latest_quotes_batch


class _FakeItem:
    def __init__(self, payload):
        self.__data__ = payload


class _FakeResponse:
    def __init__(self, output):
        self.output = output


class TestKisBatchQuote(unittest.TestCase):
    def test_fetch_latest_quotes_batch_chunks_and_preserves_existing_market_cap(self):
        df = pd.DataFrame(
            [
                {"단축코드": f"{i:06d}", "시장구분": "KOSPI", "market_cap": float(i + 1)}
                for i in range(31)
            ]
        )
        kis = MagicMock()

        def fake_fetch(path, *, api, params, domain, verbose):
            codes = [params[key] for key in sorted(params) if key.startswith("FID_INPUT_ISCD_")]
            output = [
                _FakeItem(
                    {
                        "inter_shrn_iscd": code,
                        "inter2_prpr": str(index + 1),
                    }
                )
                for index, code in enumerate(codes)
            ]
            return _FakeResponse(output)

        kis.fetch.side_effect = fake_fetch

        result = fetch_latest_quotes_batch(df, kis, retry=1, progress_desc="test")

        self.assertEqual(kis.fetch.call_count, 2)
        self.assertEqual(result.loc[0, "price"], 1.0)
        self.assertEqual(result.loc[0, "market_cap"], 1.0)
        self.assertEqual(result.loc[30, "price"], 1.0)
        self.assertEqual(result.loc[30, "market_cap"], 31.0)

    def test_fetch_latest_quotes_batch_falls_back_for_non_krx(self):
        df = pd.DataFrame(
            [
                {"단축코드": "AAPL", "시장구분": "NASDAQ"},
            ]
        )
        kis = MagicMock()
        fake_quote = MagicMock(price=123.0, market_cap=456.0)

        with patch("tools.kis_batch_quote.fetch_quote", return_value=fake_quote) as fetch_quote_mock:
            result = fetch_latest_quotes_batch(df, kis, retry=1, progress_desc="test")

        kis.fetch.assert_not_called()
        fetch_quote_mock.assert_called_once()
        self.assertEqual(result.loc[0, "price"], 123.0)
        self.assertEqual(result.loc[0, "market_cap"], 456.0)

    def test_fetch_latest_quotes_batch_uses_kis_market_cap_for_krx_without_fallback(self):
        df = pd.DataFrame(
            [
                {"단축코드": "005930", "시장구분": "KOSPI"},
            ]
        )
        kis = MagicMock()
        kis.fetch.return_value = _FakeResponse(
            [_FakeItem({"inter_shrn_iscd": "005930", "inter2_prpr": "1000"})]
        )
        fake_quote = MagicMock(price=1000.0, market_cap=999.0)

        with patch("tools.kis_batch_quote.fetch_quote", return_value=fake_quote) as fetch_quote_mock:
            result = fetch_latest_quotes_batch(df, kis, retry=1, progress_desc="test")

        kis.fetch.assert_called_once()
        fetch_quote_mock.assert_called_once()
        self.assertEqual(result.loc[0, "price"], 1000.0)
        self.assertEqual(result.loc[0, "market_cap"], 999.0)

    def test_fetch_latest_quotes_batch_uses_kis_market_cap_for_nan_krx_fallback(self):
        df = pd.DataFrame(
            [
                {"단축코드": "005930", "시장구분": "KOSPI", "market_cap": pd.NA},
            ]
        )
        kis = MagicMock()
        kis.fetch.return_value = _FakeResponse(
            [_FakeItem({"inter_shrn_iscd": "005930", "inter2_prpr": "1000"})]
        )
        fake_quote = MagicMock(price=1000.0, market_cap=777.0)

        with patch("tools.kis_batch_quote.fetch_quote", return_value=fake_quote) as fetch_quote_mock:
            result = fetch_latest_quotes_batch(df, kis, retry=1, progress_desc="test")

        fetch_quote_mock.assert_called_once()
        self.assertEqual(result.loc[0, "market_cap"], 777.0)

    def test_fetch_latest_quotes_batch_falls_back_for_nan_saved_market_cap(self):
        df = pd.DataFrame(
            [
                {"단축코드": "005930", "시장구분": "KOSPI", "market_cap": pd.NA},
            ]
        )
        kis = MagicMock()
        kis.fetch.return_value = _FakeResponse(
            [_FakeItem({"inter_shrn_iscd": "005930", "inter2_prpr": "1000"})]
        )
        fake_quote = MagicMock(price=1000.0, market_cap=555.0)

        with patch("tools.kis_batch_quote.fetch_quote", return_value=fake_quote) as fetch_quote_mock:
            result = fetch_latest_quotes_batch(df, kis, retry=1, progress_desc="test")

        fetch_quote_mock.assert_called_once()
        self.assertEqual(result.loc[0, "market_cap"], 555.0)

    def test_fetch_latest_quotes_batch_falls_back_when_batch_price_missing(self):
        df = pd.DataFrame(
            [
                {"단축코드": "005930", "시장구분": "KOSPI", "market_cap": 321.0},
            ]
        )
        kis = MagicMock()
        kis.fetch.return_value = _FakeResponse(
            [_FakeItem({"inter_shrn_iscd": "005930", "inter2_prpr": ""})]
        )
        fake_quote = MagicMock(price=1000.0, market_cap=654.0)

        with patch("tools.kis_batch_quote.fetch_quote", return_value=fake_quote) as fetch_quote_mock:
            result = fetch_latest_quotes_batch(df, kis, retry=1, progress_desc="test")

        fetch_quote_mock.assert_called_once()
        self.assertEqual(result.loc[0, "price"], 1000.0)
        self.assertEqual(result.loc[0, "market_cap"], 654.0)

    def test_fetch_latest_quotes_batch_uses_kis_market_cap_when_batch_call_fails(self):
        df = pd.DataFrame(
            [
                {"단축코드": "005930", "시장구분": "KOSPI", "market_cap": 321.0},
            ]
        )
        kis = MagicMock()
        fake_quote = MagicMock(price=1000.0, market_cap=654.0)

        with patch("tools.kis_batch_quote.retry_with_backoff") as retry_mock, patch(
            "tools.kis_batch_quote.fetch_quote",
            return_value=fake_quote,
        ) as fetch_quote_mock:
            retry_mock.side_effect = [
                (False, None),
                (True, {"단축코드": "005930", "price": 1000.0, "market_cap": 654.0}),
            ]
            result = fetch_latest_quotes_batch(df, kis, retry=1, progress_desc="test")

        fetch_quote_mock.assert_not_called()
        self.assertEqual(result.loc[0, "price"], 1000.0)
        self.assertEqual(result.loc[0, "market_cap"], 654.0)


if __name__ == "__main__":
    unittest.main()
