import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
from flask_login import (
    LoginManager,
    UserMixin,
    current_user,
    login_required,
    login_user,
    logout_user,
)
from werkzeug.security import check_password_hash

WEB_DIR = Path(__file__).resolve().parent
load_dotenv(WEB_DIR / ".env")

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from strategies import get_strategy_definition
from tools.financial_db import get_stock_selection_db_path
from tools.trading_profiles import (
    get_enabled_accounts,
    get_unique_strategies,
)

logger = logging.getLogger(__name__)


def _truthy_env(var_name: str) -> bool:
    return os.getenv(var_name, "").strip().lower() in {"1", "true", "yes", "on"}


app = Flask(__name__)

_dashboard_password_hash = os.getenv("DASHBOARD_PASSWORD_HASH")
_dashboard_secret_key = os.getenv("DASHBOARD_SECRET_KEY") or _dashboard_password_hash
if not _dashboard_secret_key:
    raise RuntimeError(
        "DASHBOARD_SECRET_KEY or DASHBOARD_PASSWORD_HASH must be configured in web/.env.",
    )

app.secret_key = _dashboard_secret_key
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=_truthy_env("DASHBOARD_COOKIE_SECURE"),
)

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"


class UnknownStrategyScopeError(ValueError):
    pass


class User(UserMixin):
    def __init__(self, user_id: str):
        self.id = user_id


@login_manager.user_loader
def load_user(user_id):
    return User(user_id)


DB_PATH = BASE_DIR / "db" / "account" / "daily_assets.db"
ACCOUNT_DB_DIR = BASE_DIR / "db" / "account"

ENABLED_ACCOUNTS = tuple(get_enabled_accounts())
ACCOUNT_MAP = {account.account_id: account for account in ENABLED_ACCOUNTS}
ENABLED_STRATEGIES = tuple(get_unique_strategies(list(ENABLED_ACCOUNTS)))
STRATEGY_MAP = {strategy.strategy_id: strategy for strategy in ENABLED_STRATEGIES}
STRATEGY_ACCOUNT_MAP = {
    strategy.strategy_id: [
        account
        for account in ENABLED_ACCOUNTS
        if account.strategy_id == strategy.strategy_id
    ]
    for strategy in ENABLED_STRATEGIES
}

LOGIN_WINDOW = timedelta(minutes=5)
MAX_LOGIN_ATTEMPTS = 5
LOGIN_ATTEMPTS: dict[str, list[datetime]] = {}


def _get_client_identifier() -> str:
    return request.remote_addr or "unknown"


def _prune_login_attempts(client_id: str, now: datetime) -> list[datetime]:
    attempts = [
        attempt
        for attempt in LOGIN_ATTEMPTS.get(client_id, [])
        if now - attempt < LOGIN_WINDOW
    ]
    if attempts:
        LOGIN_ATTEMPTS[client_id] = attempts
    else:
        LOGIN_ATTEMPTS.pop(client_id, None)
    return attempts


def _is_login_rate_limited(client_id: str) -> bool:
    return len(_prune_login_attempts(client_id, datetime.utcnow())) >= MAX_LOGIN_ATTEMPTS


def _record_failed_login_attempt(client_id: str) -> None:
    now = datetime.utcnow()
    attempts = _prune_login_attempts(client_id, now)
    attempts.append(now)
    LOGIN_ATTEMPTS[client_id] = attempts


def _clear_login_attempts(client_id: str) -> None:
    LOGIN_ATTEMPTS.pop(client_id, None)


def _build_strategy_options() -> list[dict[str, object]]:
    strategy_options: list[dict[str, object]] = [
        {
            "strategy_id": "all",
            "display_name": "All Strategies",
            "requires_selection": any(
                get_strategy_definition(strategy.strategy_id).requires_selection
                for strategy in ENABLED_STRATEGIES
            ),
        },
    ]

    for strategy in ENABLED_STRATEGIES:
        strategy_def = get_strategy_definition(strategy.strategy_id)
        strategy_options.append(
            {
                "strategy_id": strategy.strategy_id,
                "display_name": strategy.display_name,
                "requires_selection": strategy_def.requires_selection,
            },
        )

    return strategy_options


def _resolve_strategy_id(raw_strategy_id):
    if raw_strategy_id in (None, "", "all"):
        return "all"

    strategy_id = str(raw_strategy_id)
    if strategy_id in STRATEGY_MAP:
        return strategy_id

    if strategy_id in ACCOUNT_MAP:
        return ACCOUNT_MAP[strategy_id].strategy_id

    raise UnknownStrategyScopeError(f"Unknown strategy_id: {strategy_id}")


def _get_selected_strategy_id():
    raw_strategy_id = request.args.get("strategy_id")
    if raw_strategy_id is None:
        raw_strategy_id = request.args.get("account_id", "all")
    return _resolve_strategy_id(raw_strategy_id)


def _iter_strategy_accounts(selected_strategy_id):
    if selected_strategy_id == "all":
        return list(ENABLED_ACCOUNTS)
    return list(STRATEGY_ACCOUNT_MAP.get(selected_strategy_id, []))


def _should_aggregate_asset_scope(selected_strategy_id: str) -> bool:
    return selected_strategy_id == "all" or len(_iter_strategy_accounts(selected_strategy_id)) > 1


def _resolve_account_db_path(account_id):
    if account_id in (None, "", "default", "krx_vmq"):
        return DB_PATH

    return ACCOUNT_DB_DIR / f"daily_assets_{account_id}.db"


def get_db_connection(account_id=None):
    conn = sqlite3.connect(_resolve_account_db_path(account_id))
    conn.row_factory = sqlite3.Row
    return conn


def _serialize_account_row(row, account, *, include_internal_account_id=False):
    item = dict(row)
    item["strategy_id"] = account.strategy_id
    item["strategy_display_name"] = STRATEGY_MAP[account.strategy_id].display_name
    if include_internal_account_id:
        item["_account_id"] = account.account_id
    if current_user.is_authenticated:
        item["account_id"] = account.account_id
        item["account_display_name"] = account.display_name
    return item


def _fetch_strategy_rows(query, *, selected_strategy_id, params=(), include_internal_account_id=False):
    rows = []
    for account in _iter_strategy_accounts(selected_strategy_id):
        db_path = _resolve_account_db_path(account.account_id)
        if not db_path.exists():
            continue

        with get_db_connection(account.account_id) as conn:
            fetched = conn.execute(query, params).fetchall()

        for row in fetched:
            rows.append(
                _serialize_account_row(
                    row,
                    account,
                    include_internal_account_id=include_internal_account_id,
                )
            )

    return rows


def _aggregate_asset_rows(rows):
    grouped = {}
    for row in rows:
        date_key = row["date"]
        if date_key not in grouped:
            grouped[date_key] = {
                "date": date_key,
                "initial_asset": 0.0,
                "final_asset": 0.0,
                "deposit_d2": 0.0,
                "transfer_amount": 0.0,
            }

        grouped_row = grouped[date_key]
        for key in ("initial_asset", "final_asset", "deposit_d2", "transfer_amount"):
            grouped_row[key] += row.get(key, 0.0) or 0.0

    return [grouped[key] for key in sorted(grouped.keys())]


def _build_aggregated_daily_metrics(rows):
    account_histories = {}

    for row in rows:
        account_id = row.get("_account_id")
        final_asset = row.get("final_asset")
        if not account_id or final_asset is None or final_asset <= 0:
            continue

        account_histories.setdefault(account_id, []).append(dict(row))

    aggregated = {}
    for history in account_histories.values():
        history.sort(key=lambda item: item["date"])
        prev_final_asset = None

        for row in history:
            curr_final_asset = row.get("final_asset", 0.0) or 0.0
            date_key = row["date"]
            metrics = aggregated.setdefault(date_key, {"profit": 0.0, "base": 0.0})

            if prev_final_asset is None:
                prev_final_asset = curr_final_asset
                continue

            transfer_amount = row.get("transfer_amount", 0.0) or 0.0
            base = prev_final_asset + transfer_amount
            profit = curr_final_asset - base

            metrics["profit"] += profit
            if base > 0:
                metrics["base"] += base

            prev_final_asset = curr_final_asset

    return aggregated


def _list_selection_dates_for_strategy(strategy_id: str) -> list[str]:
    strategy_def = get_strategy_definition(strategy_id)
    if not strategy_def.requires_selection:
        return []

    db_path = get_stock_selection_db_path(strategy_id)
    if not db_path.exists():
        return []

    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name DESC",
        )
        tables = [row[0] for row in cursor.fetchall()]

    return sorted(
        [table for table in tables if table.isdigit() and len(table) == 8],
        reverse=True,
    )


def _build_selection_section(strategy_id: str, table_date: str) -> dict[str, object]:
    strategy = STRATEGY_MAP[strategy_id]
    strategy_def = get_strategy_definition(strategy_id)
    section: dict[str, object] = {
        "strategy_id": strategy_id,
        "strategy_display_name": strategy.display_name,
        "requires_selection": strategy_def.requires_selection,
        "available": False,
        "rows": [],
    }

    if not strategy_def.requires_selection:
        return section

    db_path = get_stock_selection_db_path(strategy_id)
    if not db_path.exists():
        return section

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_date,),
        )
        if cursor.fetchone() is None:
            return section

        table_columns = {
            row[1]
            for row in conn.execute(f"PRAGMA table_info('{table_date}')").fetchall()
        }
        order_clause = "ORDER BY rank_total" if "rank_total" in table_columns else ""
        rows = conn.execute(
            f"SELECT * FROM '{table_date}' {order_clause} LIMIT ?",
            (strategy.selection_top_n,),
        ).fetchall()

    section["available"] = True
    if not rows:
        return section

    columns = [
        "rank_total",
        "단축코드",
        "한글명",
        "rank_value",
        "rank_momentum",
        "rank_quality",
    ]
    for factor in ["1/per", "1/pbr", "gp/a"]:
        if factor in table_columns:
            columns.append(factor)

    available_columns = [column for column in columns if column in table_columns]
    section["rows"] = [
        {
            column: row[column] if row[column] is not None else 0
            for column in available_columns
        }
        for row in rows
    ]
    return section


@app.errorhandler(UnknownStrategyScopeError)
def handle_unknown_scope(error):
    return jsonify({"error": str(error)}), 400


@app.route("/login", methods=["POST"])
def login():
    client_id = _get_client_identifier()
    if _is_login_rate_limited(client_id):
        return (
            jsonify(
                {
                    "success": False,
                    "message": "Too many login attempts. Please try again later.",
                },
            ),
            429,
        )

    data = request.get_json(silent=True) or {}
    password = data.get("password", "")
    password_hash = os.getenv("DASHBOARD_PASSWORD_HASH")

    if password_hash and password and check_password_hash(password_hash, password):
        _clear_login_attempts(client_id)
        user = User(user_id="admin")
        login_user(user)
        return jsonify({"success": True})

    _record_failed_login_attempt(client_id)
    return jsonify({"success": False, "message": "Invalid password"}), 401


@app.route("/logout", methods=["POST"])
@login_required
def logout():
    logout_user()
    return jsonify({"success": True})


@app.route("/api/auth/status")
def auth_status():
    return jsonify({"authenticated": current_user.is_authenticated})


@app.route("/api/strategies")
@app.route("/api/accounts")
def get_strategies():
    strategies = _build_strategy_options()
    compat_accounts = [{"account_id": "all", "display_name": "All Accounts"}]
    if current_user.is_authenticated:
        compat_accounts.extend(
            {
                "account_id": account.account_id,
                "display_name": account.display_name,
            }
            for account in ENABLED_ACCOUNTS
        )
    return jsonify({"strategies": strategies, "accounts": compat_accounts})


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/assets")
def get_assets():
    selected_strategy_id = _get_selected_strategy_id()
    aggregate_scope = _should_aggregate_asset_scope(selected_strategy_id)
    assets = _fetch_strategy_rows(
        "SELECT * FROM daily_assets ORDER BY date",
        selected_strategy_id=selected_strategy_id,
        include_internal_account_id=aggregate_scope,
    )
    aggregated_daily_metrics = None
    if aggregate_scope:
        aggregated_daily_metrics = _build_aggregated_daily_metrics(assets)
        assets = _aggregate_asset_rows(assets)

    data = []
    for row in assets:
        if row["final_asset"] is not None and row["final_asset"] > 0:
            data.append(dict(row))

    cumulative_index = 1.0
    if data:
        data[0]["daily_return"] = 0.0
        data[0]["cumulative_return"] = 0.0

    for index in range(1, len(data)):
        if aggregated_daily_metrics is None:
            prev = data[index - 1]["final_asset"] or 0.0
            curr = data[index]["final_asset"] or 0.0
            transfer = data[index].get("transfer_amount", 0.0) or 0.0

            base = prev + transfer
            profit = curr - base
        else:
            metrics = aggregated_daily_metrics.get(data[index]["date"], {})
            base = metrics.get("base", 0.0)
            profit = metrics.get("profit", 0.0)

        daily_return = (profit / base) if base > 0 else 0.0
        cumulative_index *= 1 + daily_return

        data[index]["daily_return"] = daily_return
        data[index]["cumulative_return"] = (cumulative_index - 1.0) * 100

    if not current_user.is_authenticated:
        for item in data:
            item["initial_asset"] = 0
            item["final_asset"] = 0
            item["deposit_d2"] = 0
            item["transfer_amount"] = 0

    return jsonify(data)


@app.route("/api/performance")
def get_performance():
    selected_strategy_id = _get_selected_strategy_id()
    performance_rows = _fetch_strategy_rows(
        "SELECT * FROM daily_stock_performance ORDER BY date, symbol",
        selected_strategy_id=selected_strategy_id,
    )
    asset_rows = _fetch_strategy_rows(
        "SELECT date, final_asset FROM daily_assets WHERE final_asset IS NOT NULL",
        selected_strategy_id=selected_strategy_id,
    )

    asset_map = {}
    for row in asset_rows:
        asset_map[row["date"]] = asset_map.get(row["date"], 0.0) + (row["final_asset"] or 0.0)

    if not current_user.is_authenticated:
        for item in performance_rows:
            invested = item.get("invested_amount", 0) or 0
            current_value = item.get("current_value", 0) or 0
            sell_amount = item.get("sell_amount", 0) or 0
            row_date = item.get("date")

            if invested > 0:
                item["return_rate"] = ((current_value + sell_amount) - invested) / invested * 100
            else:
                item["return_rate"] = 0

            total_asset = asset_map.get(row_date, 0)
            if total_asset > 0:
                item["weight"] = (current_value / total_asset) * 100
            else:
                item["weight"] = 0

            item["invested_amount"] = 0
            item["current_value"] = 0
            item["quantity"] = 0
            item["average_price"] = 0
            item["current_price"] = 0
            item["sell_amount"] = 0
            item["sell_quantity"] = 0

    return jsonify(performance_rows)


@app.route("/api/orders")
def get_orders():
    selected_strategy_id = _get_selected_strategy_id()
    data = _fetch_strategy_rows(
        "SELECT * FROM daily_orders ORDER BY date DESC, time DESC",
        selected_strategy_id=selected_strategy_id,
    )
    data.sort(key=lambda row: (row["date"], row["time"]), reverse=True)

    if not current_user.is_authenticated:
        for item in data:
            item["price"] = 0
            item["qty"] = 0
            item["executed_qty"] = 0

    return jsonify(data)


@app.route("/api/analytics")
def get_analytics():
    selected_strategy_id = _get_selected_strategy_id()
    aggregate_scope = _should_aggregate_asset_scope(selected_strategy_id)
    assets = _fetch_strategy_rows(
        "SELECT * FROM daily_assets ORDER BY date ASC",
        selected_strategy_id=selected_strategy_id,
        include_internal_account_id=aggregate_scope,
    )
    aggregated_daily_metrics = None
    if aggregate_scope:
        aggregated_daily_metrics = _build_aggregated_daily_metrics(assets)
        assets = _aggregate_asset_rows(assets)

    data = []
    for row in assets:
        if row["final_asset"] is not None and row["final_asset"] > 0:
            data.append(dict(row))

    if not data:
        return jsonify({})

    for index in range(1, len(data)):
        if aggregated_daily_metrics is None:
            prev = data[index - 1]["final_asset"] or 0.0
            curr = data[index]["final_asset"] or 0.0
            transfer = data[index].get("transfer_amount", 0.0) or 0.0

            base = prev + transfer
            profit = curr - base
        else:
            metrics = aggregated_daily_metrics.get(data[index]["date"], {})
            base = metrics.get("base", 0.0)
            profit = metrics.get("profit", 0.0)

        data[index]["daily_return"] = (profit / base) if base > 0 else 0.0
        data[index]["daily_profit"] = profit

    data[0]["daily_return"] = 0.0
    data[0]["daily_profit"] = 0.0

    if not current_user.is_authenticated:
        for item in data:
            item["initial_asset"] = 0
            item["final_asset"] = 0
            item["deposit_d2"] = 0
            item["daily_profit"] = 0
            item["transfer_amount"] = 0

    cumulative_index = 1.0
    peak_index = 1.0
    mdd = 0.0
    drawdowns = [{"date": data[0]["date"], "drawdown": 0.0}]

    for index in range(1, len(data)):
        daily_return = data[index].get("daily_return", 0.0)
        cumulative_index *= 1 + daily_return

        if cumulative_index > peak_index:
            peak_index = cumulative_index

        drawdown = (
            (cumulative_index - peak_index) / peak_index
            if peak_index > 0
            else 0.0
        )

        drawdowns.append({"date": data[index]["date"], "drawdown": drawdown * 100})
        if drawdown < mdd:
            mdd = drawdown

    daily_returns = [
        item["daily_return"]
        for item in data
        if item["date"] != data[0]["date"]
    ]
    if daily_returns:
        wins = [item for item in daily_returns if item > 0]
        win_rate = len(wins) / len(daily_returns) * 100
        best_day = max(daily_returns) * 100
        worst_day = min(daily_returns) * 100

        mean_return = sum(daily_returns) / len(daily_returns)
        variance = sum((item - mean_return) ** 2 for item in daily_returns) / len(daily_returns)
        volatility = (variance ** 0.5) * 100
    else:
        win_rate = 0
        best_day = 0
        worst_day = 0
        volatility = 0

    monthly_returns_map = {}
    for day in data:
        month_key = day["date"][:7]
        if month_key not in monthly_returns_map:
            monthly_returns_map[month_key] = 1.0
        monthly_returns_map[month_key] *= 1 + day.get("daily_return", 0.0)

    monthly_returns = [
        {"month": month, "return": (monthly_returns_map[month] - 1.0) * 100}
        for month in sorted(monthly_returns_map.keys())
    ]

    return jsonify(
        {
            "mdd": mdd * 100,
            "win_rate": win_rate,
            "best_day": best_day,
            "worst_day": worst_day,
            "volatility": volatility,
            "mdd_history": drawdowns,
            "monthly_returns": monthly_returns,
            "daily_returns": [
                {
                    "date": item["date"],
                    "daily_return": item.get("daily_return", 0.0) * 100,
                    "daily_profit": item.get("daily_profit", 0.0),
                }
                for item in data
            ],
        },
    )


@app.route("/api/selection/dates")
@app.route("/api/recommendations/dates")
def get_selection_dates():
    selected_strategy_id = _get_selected_strategy_id()
    strategy_ids = (
        [strategy.strategy_id for strategy in ENABLED_STRATEGIES]
        if selected_strategy_id == "all"
        else [selected_strategy_id]
    )

    dates = set()
    for strategy_id in strategy_ids:
        dates.update(_list_selection_dates_for_strategy(strategy_id))

    return jsonify(sorted(dates, reverse=True))


@app.route("/api/selection/<date>")
@app.route("/api/recommendations/<date>")
def get_selection(date):
    selected_strategy_id = _get_selected_strategy_id()
    strategy_ids = (
        [strategy.strategy_id for strategy in ENABLED_STRATEGIES]
        if selected_strategy_id == "all"
        else [selected_strategy_id]
    )

    try:
        sections = [
            _build_selection_section(strategy_id, date)
            for strategy_id in strategy_ids
        ]
    except Exception:
        logger.exception("Failed to load selection data. strategy_id=%s date=%s", selected_strategy_id, date)
        return jsonify({"error": "Failed to load selection data."}), 500

    return jsonify(
        {
            "selected_strategy_id": selected_strategy_id,
            "strategies": sections,
        },
    )


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        debug=_truthy_env("FLASK_DEBUG"),
        port=15000,
    )
