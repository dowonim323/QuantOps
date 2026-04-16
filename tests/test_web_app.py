import os
import sqlite3
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from werkzeug.security import generate_password_hash

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("DASHBOARD_PASSWORD_HASH", generate_password_hash("secret-password"))
os.environ.setdefault("DASHBOARD_SECRET_KEY", "test-dashboard-secret")

import web.app as web_app


class TestWebApp(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        web_app.LOGIN_ATTEMPTS.clear()
        web_app.app.config["TESTING"] = True
        self.client = web_app.app.test_client()

        self.account_db_patcher = patch.object(
            web_app,
            "_resolve_account_db_path",
            side_effect=self._resolve_account_db_path,
        )
        self.selection_db_patcher = patch.object(
            web_app,
            "get_stock_selection_db_path",
            side_effect=self._resolve_selection_db_path,
        )
        self.account_db_patcher.start()
        self.selection_db_patcher.start()
        self.addCleanup(self.account_db_patcher.stop)
        self.addCleanup(self.selection_db_patcher.stop)

        self._create_account_db(
            account_id="krx_vmq",
            asset_rows=[("2026-04-09", 1_000_000.0, 1_120_000.0, 55_000.0, 25_000.0)],
            performance_rows=[
                (
                    "2026-04-09",
                    "005930",
                    "Samsung Electronics",
                    100_000.0,
                    120_000.0,
                    10,
                    10_000.0,
                    12_000.0,
                    0.0,
                    0,
                ),
            ],
            order_rows=[("2026-04-09", "09:10:00", "buy", "Samsung Electronics", 12_000.0, 10, 10)],
        )
        self._create_account_db(
            account_id="krx_us_core4",
            asset_rows=[("2026-04-09", 2_000_000.0, 2_050_000.0, 75_000.0, 0.0)],
            performance_rows=[
                (
                    "2026-04-09",
                    "AAPL",
                    "Apple",
                    200_000.0,
                    210_000.0,
                    5,
                    40_000.0,
                    42_000.0,
                    0.0,
                    0,
                ),
            ],
            order_rows=[("2026-04-09", "09:20:00", "buy", "Apple", 42_000.0, 5, 5)],
        )
        self._create_selection_db(
            strategy_id="krx_vmq",
            table_name="20260409",
            rows=[
                (1, "005930", "Samsung Electronics", 1, 2, 3),
            ],
        )

    def _resolve_account_db_path(self, account_id):
        return Path(self.temp_dir.name) / f"account_{account_id}.db"

    def _resolve_selection_db_path(self, strategy_id=None):
        suffix = strategy_id or "default"
        return Path(self.temp_dir.name) / f"selection_{suffix}.db"

    def _create_account_db(self, account_id, asset_rows, performance_rows, order_rows):
        db_path = self._resolve_account_db_path(account_id)
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "CREATE TABLE daily_assets (date TEXT, initial_asset REAL, final_asset REAL, deposit_d2 REAL, transfer_amount REAL)",
            )
            conn.executemany(
                "INSERT INTO daily_assets VALUES (?, ?, ?, ?, ?)",
                asset_rows,
            )
            conn.execute(
                "CREATE TABLE daily_stock_performance (date TEXT, symbol TEXT, name TEXT, invested_amount REAL, current_value REAL, quantity REAL, average_price REAL, current_price REAL, sell_amount REAL, sell_quantity REAL)",
            )
            conn.executemany(
                "INSERT INTO daily_stock_performance VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                performance_rows,
            )
            conn.execute(
                "CREATE TABLE daily_orders (date TEXT, time TEXT, type TEXT, name TEXT, price REAL, qty REAL, executed_qty REAL)",
            )
            conn.executemany(
                "INSERT INTO daily_orders VALUES (?, ?, ?, ?, ?, ?, ?)",
                order_rows,
            )

    def _replace_asset_rows(self, account_id, asset_rows):
        db_path = self._resolve_account_db_path(account_id)
        with sqlite3.connect(db_path) as conn:
            conn.execute("DELETE FROM daily_assets")
            conn.executemany(
                "INSERT INTO daily_assets VALUES (?, ?, ?, ?, ?)",
                asset_rows,
            )

    def _create_selection_db(self, strategy_id, table_name, rows):
        db_path = self._resolve_selection_db_path(strategy_id)
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                f"CREATE TABLE '{table_name}' (rank_total INTEGER, 단축코드 TEXT, 한글명 TEXT, rank_value INTEGER, rank_momentum INTEGER, rank_quality INTEGER)",
            )
            conn.executemany(
                f"INSERT INTO '{table_name}' VALUES (?, ?, ?, ?, ?, ?)",
                rows,
            )

    def test_strategies_endpoint_returns_strategy_options(self):
        response = self.client.get("/api/strategies")

        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        strategy_ids = [strategy["strategy_id"] for strategy in data["strategies"]]

        self.assertEqual(strategy_ids[0], "all")
        self.assertIn("krx_vmq", strategy_ids)
        self.assertIn("krx_us_core4", strategy_ids)
        self.assertEqual(data["accounts"], [{"account_id": "all", "display_name": "All Accounts"}])

    def test_accounts_endpoint_returns_real_accounts_when_authenticated(self):
        login_response = self.client.post(
            "/login",
            json={"password": "secret-password"},
        )
        self.assertEqual(login_response.status_code, 200)

        response = self.client.get("/api/accounts")

        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        account_ids = [account["account_id"] for account in data["accounts"]]

        self.assertEqual(account_ids[0], "all")
        self.assertIn("krx_vmq", account_ids)
        self.assertIn("krx_us_core4", account_ids)

    def test_guest_assets_mask_sensitive_values(self):
        response = self.client.get("/api/assets?strategy_id=krx_vmq")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(len(payload), 1)
        row = payload[0]

        self.assertEqual(row["strategy_id"], "krx_vmq")
        self.assertNotIn("account_id", row)
        self.assertEqual(row["initial_asset"], 0)
        self.assertEqual(row["final_asset"], 0)
        self.assertEqual(row["deposit_d2"], 0)
        self.assertEqual(row["transfer_amount"], 0)

    def test_guest_performance_masks_amounts_and_account_metadata(self):
        response = self.client.get("/api/performance?account_id=krx_vmq")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(len(payload), 1)
        row = payload[0]

        self.assertEqual(row["strategy_id"], "krx_vmq")
        self.assertNotIn("account_id", row)
        self.assertNotIn("account_display_name", row)
        self.assertEqual(row["invested_amount"], 0)
        self.assertEqual(row["current_value"], 0)
        self.assertEqual(row["quantity"], 0)
        self.assertIn("return_rate", row)
        self.assertIn("weight", row)

    def test_selection_endpoints_return_strategy_grouped_payload(self):
        dates_response = self.client.get("/api/selection/dates?strategy_id=all")
        self.assertEqual(dates_response.status_code, 200)
        self.assertEqual(dates_response.get_json(), ["20260409"])

        response = self.client.get("/api/selection/20260409?strategy_id=all")
        self.assertEqual(response.status_code, 200)

        payload = response.get_json()
        strategies = {item["strategy_id"]: item for item in payload["strategies"]}

        self.assertIn("krx_vmq", strategies)
        self.assertIn("krx_us_core4", strategies)
        self.assertTrue(strategies["krx_vmq"]["available"])
        self.assertEqual(len(strategies["krx_vmq"]["rows"]), 1)
        self.assertFalse(strategies["krx_us_core4"]["requires_selection"])
        self.assertEqual(strategies["krx_us_core4"]["rows"], [])

    def test_unknown_strategy_scope_returns_bad_request(self):
        response = self.client.get("/api/performance?strategy_id=unknown")

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "Unknown strategy_id: unknown")

    def test_strategy_assets_aggregate_multiple_accounts_with_same_strategy(self):
        duplicate_account = SimpleNamespace(
            account_id="krx_vmq_sub",
            display_name="KRX VMQ Sub Account",
            strategy_id="krx_vmq",
        )
        self._create_account_db(
            account_id="krx_vmq_sub",
            asset_rows=[("2026-04-09", 300_000.0, 360_000.0, 12_000.0, 5_000.0)],
            performance_rows=[],
            order_rows=[],
        )

        with patch.dict(
            web_app.STRATEGY_ACCOUNT_MAP,
            {"krx_vmq": [web_app.ACCOUNT_MAP["krx_vmq"], duplicate_account]},
            clear=False,
        ):
            login_response = self.client.post(
                "/login",
                json={"password": "secret-password"},
            )
            self.assertEqual(login_response.status_code, 200)

            response = self.client.get("/api/assets?strategy_id=krx_vmq")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["initial_asset"], 1_300_000.0)
        self.assertEqual(payload[0]["final_asset"], 1_480_000.0)
        self.assertEqual(payload[0]["deposit_d2"], 67_000.0)
        self.assertEqual(payload[0]["transfer_amount"], 30_000.0)

    def test_all_strategy_assets_ignore_new_account_inception_in_cumulative_return(self):
        self._replace_asset_rows(
            "krx_vmq",
            [
                ("2026-04-09", 1_000_000.0, 1_000_000.0, 0.0, 0.0),
                ("2026-04-10", 1_000_000.0, 1_100_000.0, 0.0, 0.0),
            ],
        )
        self._replace_asset_rows(
            "krx_us_core4",
            [("2026-04-10", 2_000_000.0, 2_000_000.0, 0.0, 0.0)],
        )

        login_response = self.client.post(
            "/login",
            json={"password": "secret-password"},
        )
        self.assertEqual(login_response.status_code, 200)

        response = self.client.get("/api/assets?strategy_id=all")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual([item["date"] for item in payload], ["2026-04-09", "2026-04-10"])
        self.assertAlmostEqual(payload[0]["cumulative_return"], 0.0)
        self.assertAlmostEqual(payload[1]["daily_return"], 0.1)
        self.assertAlmostEqual(payload[1]["cumulative_return"], 10.0)

    def test_all_strategy_analytics_ignore_new_account_inception_in_daily_return(self):
        self._replace_asset_rows(
            "krx_vmq",
            [
                ("2026-04-09", 1_000_000.0, 1_000_000.0, 0.0, 0.0),
                ("2026-04-10", 1_000_000.0, 1_100_000.0, 0.0, 0.0),
            ],
        )
        self._replace_asset_rows(
            "krx_us_core4",
            [("2026-04-10", 2_000_000.0, 2_000_000.0, 0.0, 0.0)],
        )

        login_response = self.client.post(
            "/login",
            json={"password": "secret-password"},
        )
        self.assertEqual(login_response.status_code, 200)

        response = self.client.get("/api/analytics?strategy_id=all")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        daily_returns = {item["date"]: item for item in payload["daily_returns"]}

        self.assertAlmostEqual(daily_returns["2026-04-09"]["daily_return"], 0.0)
        self.assertAlmostEqual(daily_returns["2026-04-10"]["daily_return"], 10.0)
        self.assertAlmostEqual(daily_returns["2026-04-10"]["daily_profit"], 100_000.0)
        self.assertEqual(len(payload["monthly_returns"]), 1)
        self.assertEqual(payload["monthly_returns"][0]["month"], "2026-04")
        self.assertAlmostEqual(payload["monthly_returns"][0]["return"], 10.0)


if __name__ == "__main__":
    unittest.main()
