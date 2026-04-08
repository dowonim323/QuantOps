import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tools.financial_db as financial_db_module
from tools.financial_db import FinancialDBReader, get_stock_selection_db_path, update_db


class TestFinancialDBReader(unittest.TestCase):
    def test_prefetch_caches_quarter_statements(self):
        sample = pd.DataFrame(
            [[1.0]],
            index=pd.Index(["EPS"], name="metric"),
            columns=pd.Index(["2024/12"]),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            db_map = {
                ("ratio", "quarter"): Path(temp_dir) / "ratio_q.db",
                ("income", "quarter"): Path(temp_dir) / "income_q.db",
                ("balance", "quarter"): Path(temp_dir) / "balance_q.db",
                ("cashflow", "quarter"): Path(temp_dir) / "cashflow_q.db",
            }

            with patch("tools.financial_db.GROUP_DB_MAP", db_map):
                for report_type in ["ratio", "income", "balance", "cashflow"]:
                    update_db(report_type, "quarter", "005930", sample)

            real_read = financial_db_module._read_table_frame
            with patch("tools.financial_db._read_table_frame", wraps=real_read) as read_mock:
                with FinancialDBReader(db_map) as reader:
                    reader.prefetch_quarter_statements(["005930"])
                    reader.load_quarter_statements("005930")

            self.assertEqual(read_mock.call_count, 4)

    def test_get_stock_selection_db_path_resolves_vmq_strategy_db(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_dir = Path(temp_dir) / "db"
            quant_dir = db_dir / "quant"
            legacy_db_path = quant_dir / "stock_selection.db"
            strategy_db_path = quant_dir / "stock_selection_krx_vmq.db"
            legacy_db_path.parent.mkdir(parents=True, exist_ok=True)
            legacy_db_path.write_text("legacy-selection-db", encoding="utf-8")

            with patch("tools.financial_db.DB_DIR", db_dir), patch(
                "tools.financial_db.QUANT_DATA_DIR", quant_dir,
            ), patch("tools.financial_db.STOCK_SELECTION_DB_PATH", legacy_db_path):
                resolved_db_path = get_stock_selection_db_path("krx_vmq")

            self.assertEqual(resolved_db_path, strategy_db_path)
            self.assertTrue(legacy_db_path.exists())
            self.assertFalse(strategy_db_path.exists())

    def test_get_stock_selection_db_path_keeps_default_selection_db(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_dir = Path(temp_dir) / "db"
            quant_dir = db_dir / "quant"
            legacy_db_path = quant_dir / "stock_selection.db"
            legacy_db_path.parent.mkdir(parents=True, exist_ok=True)
            legacy_db_path.write_text("legacy-selection-db", encoding="utf-8")

            with patch("tools.financial_db.DB_DIR", db_dir), patch(
                "tools.financial_db.QUANT_DATA_DIR", quant_dir,
            ), patch("tools.financial_db.STOCK_SELECTION_DB_PATH", legacy_db_path):
                resolved_db_path = get_stock_selection_db_path()

            self.assertEqual(resolved_db_path, legacy_db_path)
            self.assertTrue(legacy_db_path.exists())
            self.assertEqual(
                legacy_db_path.read_text(encoding="utf-8"),
                "legacy-selection-db",
            )


if __name__ == "__main__":
    unittest.main()
