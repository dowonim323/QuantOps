import os
import sys
import unittest
from unittest.mock import MagicMock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pykis.api.stock.status import KisDomesticTradingStatus, domestic_trading_status


class TestDomesticTradingStatus(unittest.TestCase):
    def test_domestic_trading_status_uses_inquire_price_2_endpoint(self):
        kis = MagicMock()
        expected = KisDomesticTradingStatus("101400")
        kis.fetch.return_value = expected

        result = domestic_trading_status(kis, "101400")

        self.assertIs(result, expected)

        args = kis.fetch.call_args.args
        kwargs = kis.fetch.call_args.kwargs

        self.assertEqual(args[0], "/uapi/domestic-stock/v1/quotations/inquire-price-2")
        self.assertEqual(kwargs["api"], "FHPST01010000")
        self.assertEqual(
            kwargs["params"],
            {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": "101400",
            },
        )
        self.assertEqual(kwargs["domain"], "real")
        self.assertIsInstance(kwargs["response_type"], KisDomesticTradingStatus)
        self.assertEqual(kwargs["response_type"].symbol, "101400")


if __name__ == "__main__":
    unittest.main()
