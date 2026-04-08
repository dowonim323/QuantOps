import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
DB_DIR = BASE_DIR / "db" / "account"
DB_PATH = DB_DIR / "daily_assets.db"

_db_initialized: set[Path] = set()


def _normalize_account_id(account_id: str | None) -> str:
    if account_id is None:
        return "krx_vmq"

    normalized = "".join(
        char if char.isalnum() or char in {"_", "-"} else "_"
        for char in str(account_id).lower()
    ).strip("_")
    return normalized or "krx_vmq"


def _resolve_db_path(account_id: str | None = None) -> Path:
    normalized = _normalize_account_id(account_id)
    if normalized in {"default", "krx_vmq"}:
        return DB_PATH

    return DB_DIR / f"daily_assets_{normalized}.db"


def _init_db(account_id: str | None = None) -> None:
    """Initialize database tables. Called only once per process."""
    db_path = _resolve_db_path(account_id)
    if db_path in _db_initialized:
        return

    DB_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path, timeout=30.0) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_assets (
                date TEXT PRIMARY KEY,
                initial_asset REAL,
                final_asset REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_orders (
                order_number TEXT PRIMARY KEY,
                date TEXT,
                time TEXT,
                type TEXT,
                name TEXT,
                qty INTEGER,
                executed_qty INTEGER,
                price REAL,
                status TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_stock_performance (
                date TEXT,
                symbol TEXT,
                name TEXT,
                invested_amount REAL,
                current_value REAL,
                realized_profit REAL,
                sell_amount REAL DEFAULT 0.0,
                quantity INTEGER DEFAULT 0,
                PRIMARY KEY (date, symbol)
            )
        """)

        # Add sell_amount column if not exists (migration)
        try:
            conn.execute("ALTER TABLE daily_stock_performance ADD COLUMN sell_amount REAL DEFAULT 0.0")
        except sqlite3.OperationalError:
            pass # Column likely already exists

        # Add quantity column if not exists (migration)
        try:
            conn.execute("ALTER TABLE daily_stock_performance ADD COLUMN quantity INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass

        # Add deposit_d2 column to daily_assets if not exists (migration)
        try:
            conn.execute("ALTER TABLE daily_assets ADD COLUMN deposit_d2 REAL DEFAULT 0.0")
        except sqlite3.OperationalError:
            pass

        # Add transfer_amount column to daily_assets if not exists (migration)
        try:
            conn.execute("ALTER TABLE daily_assets ADD COLUMN transfer_amount REAL DEFAULT 0.0")
        except sqlite3.OperationalError:
            pass

        conn.execute("""
            CREATE TABLE IF NOT EXISTS unfilled_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                time TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                unfilled_qty INTEGER,
                current_value REAL,
                target_value REAL,
                context TEXT,
                resolved INTEGER DEFAULT 0
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS strategy_runtime_state (
                strategy_id TEXT PRIMARY KEY,
                stage INTEGER NOT NULL DEFAULT 0,
                last_signal_date TEXT,
                last_rsi REAL,
                last_rebalance_date TEXT
            )
        """)

        try:
            conn.execute("ALTER TABLE strategy_runtime_state ADD COLUMN last_rebalance_date TEXT")
        except sqlite3.OperationalError:
            pass

    _db_initialized.add(db_path)


def get_db_connection(account_id: str | None = None):
    return sqlite3.connect(_resolve_db_path(account_id), timeout=30.0)


def get_daily_asset(
    target_date: date | None = None,
    *,
    account_id: str | None = None,
) -> tuple[float | None, float | None, float]:
    """
    특정 날짜의 (장초 평가금, 장후 평가금, 입출금액)을 반환합니다.
    날짜가 없으면 오늘 날짜를 사용합니다.
    """
    if target_date is None:
        target_date = date.today()
        
    _init_db(account_id)
    
    with get_db_connection(account_id) as conn:
        cursor = conn.execute(
            "SELECT initial_asset, final_asset, transfer_amount FROM daily_assets WHERE date = ?",
            (target_date.strftime("%Y-%m-%d"),)
        )
        row = cursor.fetchone()
        
    if row:
        return row[0], row[1], (row[2] or 0.0)
    return None, None, 0.0

def save_initial_asset(
    asset_value: float,
    deposit_d2: float = 0.0,
    transfer_amount: float = 0.0,
    target_date: date | None = None,
    *,
    account_id: str | None = None,
):
    """
    Save the initial asset value for the day.
    If a record already exists for the date, it updates the initial_asset.
    """
    if target_date is None:
        target_date = date.today()
    
    _init_db(account_id)
    date_str = target_date.strftime("%Y-%m-%d")
    
    with get_db_connection(account_id) as conn:
        conn.execute("""
            INSERT INTO daily_assets (date, initial_asset, deposit_d2, transfer_amount)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                initial_asset = excluded.initial_asset,
                deposit_d2 = excluded.deposit_d2,
                transfer_amount = excluded.transfer_amount
        """, (date_str, asset_value, deposit_d2, transfer_amount))

def save_final_asset(
    asset_value: float,
    deposit_d2: float = 0.0,
    target_date: date | None = None,
    *,
    account_id: str | None = None,
):
    """
    Save the final asset value for the day.
    If a record already exists for the date, it updates the final_asset.
    """
    if target_date is None:
        target_date = date.today()
    
    _init_db(account_id)
    date_str = target_date.strftime("%Y-%m-%d")
    
    with get_db_connection(account_id) as conn:
        conn.execute("""
            INSERT INTO daily_assets (date, final_asset, deposit_d2)
            VALUES (?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                final_asset = excluded.final_asset,
                deposit_d2 = excluded.deposit_d2
        """, (date_str, asset_value, deposit_d2))

def save_daily_orders(
    orders: list[dict],
    target_date: date | None = None,
    *,
    account_id: str | None = None,
) -> None:
    """
    일별 주문 내역을 저장합니다.
    orders: dict 리스트 (order_number, time, type, name, qty, executed_qty, price, status)
    """
    if target_date is None:
        target_date = date.today()
        
    _init_db(account_id)
    date_str = target_date.strftime("%Y-%m-%d")
    
    with get_db_connection(account_id) as conn:
        for order in orders:
            conn.execute("""
                INSERT INTO daily_orders (order_number, date, time, type, name, qty, executed_qty, price, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_number) DO UPDATE SET
                    executed_qty = excluded.executed_qty,
                    status = excluded.status
            """, (
                order["order_number"],
                date_str,
                order["time"],
                order["type"],
                order["name"],
                order["qty"],
                order["executed_qty"],
                order["price"],
                order["status"]
            ))

def get_previous_final_asset(
    target_date: date | None = None,
    *,
    account_id: str | None = None,
) -> tuple[float | None, float | None]:
    """
    target_date 이전의 가장 최근 (final_asset, deposit_d2)를 반환합니다.
    """
    if target_date is None:
        target_date = date.today()
        
    _init_db(account_id)
    
    with get_db_connection(account_id) as conn:
        cursor = conn.execute(
            "SELECT final_asset, deposit_d2 FROM daily_assets WHERE date < ? AND final_asset IS NOT NULL ORDER BY date DESC LIMIT 1",
            (target_date.strftime("%Y-%m-%d"),)
        )
        row = cursor.fetchone()
        
    if row:
        return row[0], (row[1] or 0.0)
    return None, None

def save_stock_performance(
    performances: list[dict],
    target_date: date | None = None,
    *,
    account_id: str | None = None,
) -> None:
    """
    일별 종목 성과를 저장합니다.
    performances: dict 리스트 (symbol, name, invested_amount, current_value, realized_profit)
    """
    if target_date is None:
        target_date = date.today()
        
    _init_db(account_id)
    date_str = target_date.strftime("%Y-%m-%d")
    
    with get_db_connection(account_id) as conn:
        for perf in performances:
            conn.execute("""
                INSERT INTO daily_stock_performance (date, symbol, name, invested_amount, current_value, realized_profit, sell_amount, quantity)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, symbol) DO UPDATE SET
                    name = excluded.name,
                    invested_amount = excluded.invested_amount,
                    current_value = excluded.current_value,
                    realized_profit = excluded.realized_profit,
                    sell_amount = excluded.sell_amount,
                    quantity = excluded.quantity
            """, (
                date_str,
                perf["symbol"],
                perf["name"],
                perf["invested_amount"],
                perf["current_value"],
                perf["realized_profit"],
                perf.get("sell_amount", 0.0),
                perf.get("quantity", 0)
            ))

def get_latest_stock_performance(
    target_date: date | None = None,
    *,
    account_id: str | None = None,
) -> dict[str, dict]:
    """
    target_date 이전의 가장 최근 종목별 성과를 반환합니다.
    Returns: {symbol: {invested_amount, sell_amount, ...}}
    """
    if target_date is None:
        target_date = date.today()
        
    _init_db(account_id)
    
    with sqlite3.connect(_resolve_db_path(account_id)) as conn:
        cursor = conn.execute(
            "SELECT MAX(date) FROM daily_stock_performance WHERE date < ?",
            (target_date.strftime("%Y-%m-%d"),)
        )
        last_date = cursor.fetchone()[0]
        
        if not last_date:
            return {}
            
        cursor = conn.execute(
            "SELECT symbol, name, invested_amount, current_value, realized_profit, sell_amount FROM daily_stock_performance WHERE date = ?",
            (last_date,)
        )
        
        result = {}
        for row in cursor.fetchall():
            result[row[0]] = {
                "symbol": row[0],
                "name": row[1],
                "invested_amount": row[2],
                "current_value": row[3],
                "realized_profit": row[4],
                "sell_amount": row[5]
            }
            
        return result


def save_unfilled_orders(
    unfilled: dict[str, Any],
    side: str,
    order_type: str,
    context: str = "",
    target_date: date | None = None,
    *,
    account_id: str | None = None,
) -> None:
    from datetime import datetime
    
    if target_date is None:
        target_date = date.today()
        
    _init_db(account_id)
    date_str = target_date.strftime("%Y-%m-%d")
    time_str = datetime.now().strftime("%H:%M:%S")
    
    with get_db_connection(account_id) as conn:
        for symbol, info in unfilled.items():
            if order_type == "qty":
                unfilled_qty = int(info) if isinstance(info, (int, float)) else None
                current_value = None
                target_value = None
            else:
                unfilled_qty = None
                current_value = info.get("current") if isinstance(info, dict) else None
                target_value = info.get("target") if isinstance(info, dict) else None
            
            conn.execute("""
                INSERT INTO unfilled_orders (date, time, symbol, side, order_type, unfilled_qty, current_value, target_value, context)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (date_str, time_str, symbol, side, order_type, unfilled_qty, current_value, target_value, context))


def get_unresolved_unfilled_orders(
    target_date: date | None = None,
    *,
    account_id: str | None = None,
) -> list[dict]:
    if target_date is None:
        target_date = date.today()
        
    _init_db(account_id)
    date_str = target_date.strftime("%Y-%m-%d")
    
    with get_db_connection(account_id) as conn:
        cursor = conn.execute(
            "SELECT id, time, symbol, side, order_type, unfilled_qty, current_value, target_value, context FROM unfilled_orders WHERE date = ? AND resolved = 0",
            (date_str,)
        )
        
        results = []
        for row in cursor.fetchall():
            results.append({
                "id": row[0],
                "time": row[1],
                "symbol": row[2],
                "side": row[3],
                "order_type": row[4],
                "unfilled_qty": row[5],
                "current_value": row[6],
                "target_value": row[7],
                "context": row[8],
            })
        
        return results


def mark_unfilled_order_resolved(order_id: int, *, account_id: str | None = None) -> None:
    _init_db(account_id)
    with get_db_connection(account_id) as conn:
        conn.execute("UPDATE unfilled_orders SET resolved = 1 WHERE id = ?", (order_id,))


def load_strategy_runtime_state(
    strategy_id: str,
    *,
    account_id: str,
) -> dict[str, Any]:
    _init_db(account_id)
    with get_db_connection(account_id) as conn:
        cursor = conn.execute(
            "SELECT stage, last_signal_date, last_rsi, last_rebalance_date FROM strategy_runtime_state WHERE strategy_id = ?",
            (strategy_id,),
        )
        row = cursor.fetchone()

    if row is None:
        return {
            "stage": 0,
            "last_signal_date": None,
            "last_rsi": None,
            "last_rebalance_date": None,
        }

    return {
        "stage": int(row[0]),
        "last_signal_date": row[1],
        "last_rsi": float(row[2]) if row[2] is not None else None,
        "last_rebalance_date": row[3],
    }


def save_strategy_runtime_state(
    strategy_id: str,
    stage: int,
    *,
    account_id: str,
    last_signal_date: str | None = None,
    last_rsi: float | None = None,
    last_rebalance_date: str | None = None,
) -> None:
    _init_db(account_id)
    with get_db_connection(account_id) as conn:
        conn.execute(
            """
            INSERT INTO strategy_runtime_state (strategy_id, stage, last_signal_date, last_rsi, last_rebalance_date)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(strategy_id) DO UPDATE SET
                stage = excluded.stage,
                last_signal_date = excluded.last_signal_date,
                last_rsi = excluded.last_rsi,
                last_rebalance_date = excluded.last_rebalance_date
            """,
            (strategy_id, stage, last_signal_date, last_rsi, last_rebalance_date),
        )
