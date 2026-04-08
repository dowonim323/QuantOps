import os
import sys
import unittest

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.quant_utils import _resolve_period_labels


class TestQuantPeriodResolution(unittest.TestCase):
    def test_resolve_period_labels_skips_sparse_latest_common_quarter(self):
        ratio_q = pd.DataFrame(
            [[100.0, 110.0]],
            index=pd.Index(["EPS"]),
            columns=pd.Index(["2025/12", "2026/03"]),
        )
        income_q = pd.DataFrame(
            [[10.0, 12.0]],
            index=pd.Index(["영업이익"]),
            columns=pd.Index(["2025/12", "2026/03"]),
        )
        balance_q = pd.DataFrame(
            [[1000.0, None]],
            index=pd.Index(["자산총계"]),
            columns=pd.Index(["2025/12", "2026/03"]),
        )
        cashflow_q = pd.DataFrame(
            [[20.0, None]],
            index=pd.Index(["영업활동으로인한현금흐름"]),
            columns=pd.Index(["2025/12", "2026/03"]),
        )

        recent, prev_quarter, prev_year = _resolve_period_labels(
            ratio_q,
            income_q,
            balance_q,
            cashflow_q,
        )

        self.assertEqual(recent, "2025/12")
        self.assertEqual(prev_quarter, "2025/09")
        self.assertEqual(prev_year, "2024/12")


if __name__ == "__main__":
    unittest.main()
