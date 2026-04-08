from __future__ import annotations

import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Mapping, Tuple, cast

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DB_DIR = BASE_DIR / "db"
DB_HISTORY_DIR = BASE_DIR / "db_history"
DB_HISTORY_FIN_DIR = DB_HISTORY_DIR / "financial_data"
DB_HISTORY_QUANT_DIR = DB_HISTORY_DIR / "quant"
FINANCIAL_DATA_DIR = DB_DIR / "financial_data"
QUANT_DATA_DIR = DB_DIR / "quant"


def _resolve_financial_db_path(filename: str) -> Path:
    FINANCIAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    legacy_path = DB_DIR / filename
    new_path = FINANCIAL_DATA_DIR / filename

    if legacy_path.exists() and not new_path.exists():
        new_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(legacy_path, new_path)

    return new_path


def _resolve_quant_db_path(filename: str) -> Path:
    QUANT_DATA_DIR.mkdir(parents=True, exist_ok=True)
    legacy_path = DB_DIR / filename
    new_path = QUANT_DATA_DIR / filename

    if legacy_path.exists() and not new_path.exists():
        shutil.move(legacy_path, new_path)

    return new_path

GroupKey = Tuple[str, str]

GROUP_DB_MAP: Dict[GroupKey, Path] = {
    ("ratio", "year"): _resolve_financial_db_path("financial_ratio_year.db"),
    ("ratio", "quarter"): _resolve_financial_db_path("financial_ratio_quarter.db"),
    ("income", "year"): _resolve_financial_db_path("income_statement_year.db"),
    ("income", "quarter"): _resolve_financial_db_path("income_statement_quarter.db"),
    ("balance", "year"): _resolve_financial_db_path("balance_sheet_year.db"),
    ("balance", "quarter"): _resolve_financial_db_path("balance_sheet_quarter.db"),
    ("cashflow", "year"): _resolve_financial_db_path("cash_flow_sheet_year.db"),
    ("cashflow", "quarter"): _resolve_financial_db_path("cash_flow_sheet_quarter.db"),
}

STOCK_SELECTION_DB_PATH = _resolve_quant_db_path("stock_selection.db")


def get_financial_db_path(report_type: str, period: str) -> Path:
    group_key = (report_type, period)
    if group_key not in GROUP_DB_MAP:
        raise ValueError(f"알 수 없는 그룹: {group_key}")

    return GROUP_DB_MAP[group_key]


def get_stock_selection_db_path(strategy_id: str | None = None) -> Path:
    if strategy_id in (None, "", "default"):
        return STOCK_SELECTION_DB_PATH

    return _resolve_quant_db_path(f"stock_selection_{strategy_id}.db")


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _qualified_table_name(code: str, schema: str | None = None) -> str:
    table_name = _quote_identifier(code)
    if schema is None:
        return table_name

    return f'{_quote_identifier(schema)}.{table_name}'


def ensure_table(
    conn: sqlite3.Connection,
    code: str,
    schema: str | None = None,
) -> None:
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {_qualified_table_name(code, schema)} "
        "(metric TEXT PRIMARY KEY)"
    )


def ensure_columns(
    conn: sqlite3.Connection,
    code: str,
    columns: Iterable[str],
    schema: str | None = None,
) -> None:
    column_list = list(columns)
    if not column_list:
        return

    existing_columns = {
        row[1]
        for row in conn.execute(
            f"PRAGMA {'' if schema is None else f'{_quote_identifier(schema)}.'}table_info({_quote_identifier(code)})"
        )
    }

    for column in column_list:
        if column in existing_columns:
            continue
        conn.execute(
            f"ALTER TABLE {_qualified_table_name(code, schema)} "
            f"ADD COLUMN {_quote_identifier(column)} REAL"
        )
        existing_columns.add(column)


def _ensure_history_structure() -> None:
    DB_HISTORY_FIN_DIR.mkdir(parents=True, exist_ok=True)
    DB_HISTORY_QUANT_DIR.mkdir(parents=True, exist_ok=True)
    for path in DB_HISTORY_DIR.iterdir():
        if path in {DB_HISTORY_FIN_DIR, DB_HISTORY_QUANT_DIR} or not path.is_dir():
            continue
        destination = DB_HISTORY_FIN_DIR / path.name
        if destination.exists():
            continue
        shutil.move(path, destination)


def _resolved_backup_dir(history_root: Path, timestamp: str) -> Path:
    candidate = history_root / timestamp
    if not candidate.exists():
        return candidate

    suffix = 1
    while True:
        deduped = history_root / f"{timestamp}_{suffix}"
        if not deduped.exists():
            return deduped
        suffix += 1


def _prune_history_entries(history_root: Path, limit: int) -> None:
    if limit <= 0 or not history_root.exists():
        return

    history_dirs = sorted(
        [path for path in history_root.iterdir() if path.is_dir()],
        key=lambda path: path.name,
    )

    overflow = len(history_dirs) - limit
    if overflow <= 0:
        return

    for directory in history_dirs[:overflow]:
        shutil.rmtree(directory, ignore_errors=True)


def _backup_db_group(
    source_dir: Path,
    history_root: Path,
    history_limit: int,
) -> Path | None:
    _ensure_history_structure()
    source_dir.mkdir(parents=True, exist_ok=True)
    db_files = sorted(source_dir.glob("*.db"))

    if not db_files:
        _prune_history_entries(history_root, history_limit)
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = _resolved_backup_dir(history_root, timestamp)
    backup_dir.mkdir(parents=True, exist_ok=False)

    for db_file in db_files:
        destination = backup_dir / db_file.name
        shutil.copy2(db_file, destination)

    _prune_history_entries(history_root, history_limit)
    return backup_dir


def backup_databases(history_limit: int = 30) -> Path | None:
    return _backup_db_group(FINANCIAL_DATA_DIR, DB_HISTORY_FIN_DIR, history_limit)


def backup_quant_databases(history_limit: int = 30) -> Path | None:
    return _backup_db_group(QUANT_DATA_DIR, DB_HISTORY_QUANT_DIR, history_limit)


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized.index = normalized.index.astype(str)
    normalized.columns = normalized.columns.astype(str)
    return normalized


def _build_row_parameters(df: pd.DataFrame) -> list[tuple[object, ...]]:
    row_parameters: list[tuple[object, ...]] = []

    for metric, row in df.iterrows():
        values: list[object] = [metric]
        for value in row.tolist():
            if value is None or pd.isna(value):
                values.append(None)
            else:
                values.append(float(value))
        row_parameters.append(tuple(values))

    return row_parameters


def _write_dataframe(
    conn: sqlite3.Connection,
    report_type: str,
    period: str,
    code: str,
    df: pd.DataFrame,
    *,
    drop_missing_metrics: bool = False,
    schema: str | None = None,
) -> None:
    normalized = _normalize_dataframe(df)
    if normalized.empty:
        return

    ensure_table(conn, code, schema=schema)
    ensure_columns(conn, code, normalized.columns, schema=schema)

    qualified_table = _qualified_table_name(code, schema)

    if drop_missing_metrics:
        placeholders = ", ".join("?" for _ in normalized.index)
        if placeholders:
            conn.execute(
                f"DELETE FROM {qualified_table} WHERE metric NOT IN ({placeholders})",
                tuple(normalized.index),
            )

    column_identifiers = [_quote_identifier(column) for column in normalized.columns]
    insert_columns = ", ".join([_quote_identifier("metric"), *column_identifiers])
    value_placeholders = ", ".join("?" for _ in range(len(normalized.columns) + 1))
    update_assignments = ", ".join(
        f"{column_identifier} = excluded.{column_identifier}"
        for column_identifier in column_identifiers
    )
    row_parameters = _build_row_parameters(normalized)

    conn.executemany(
        f"INSERT INTO {qualified_table} ({insert_columns}) "
        f"VALUES ({value_placeholders}) "
        f"ON CONFLICT(metric) DO UPDATE SET {update_assignments}",
        row_parameters,
    )


class FinancialDBBatchWriter:
    def __init__(
        self,
        db_map: Mapping[GroupKey, Path] | None = None,
    ) -> None:
        self._db_map = dict(db_map or GROUP_DB_MAP)
        self._aliases: dict[GroupKey, str] = {}
        self._conn = sqlite3.connect(":memory:", isolation_level=None)
        self._savepoint_index = 0

        for index, (group_key, db_path) in enumerate(self._db_map.items()):
            db_path.parent.mkdir(parents=True, exist_ok=True)
            alias = f"financial_{index}"
            self._conn.execute(
                f"ATTACH DATABASE ? AS {_quote_identifier(alias)}",
                (str(db_path),),
            )
            self._aliases[group_key] = alias

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> FinancialDBBatchWriter:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def write_symbol_reports(
        self,
        reports: Iterable[tuple[str, str, str, pd.DataFrame]],
        *,
        drop_missing_metrics: bool = False,
    ) -> None:
        report_list = list(reports)
        if not report_list:
            return

        self._savepoint_index += 1
        savepoint_name = f"symbol_{self._savepoint_index}"
        quoted_savepoint = _quote_identifier(savepoint_name)
        self._conn.execute(f"SAVEPOINT {quoted_savepoint}")

        try:
            for report_type, period, code, df in report_list:
                group_key = cast(GroupKey, (report_type, period))
                alias = self._aliases[group_key]
                _write_dataframe(
                    self._conn,
                    report_type,
                    period,
                    code,
                    df,
                    drop_missing_metrics=drop_missing_metrics,
                    schema=alias,
                )
        except Exception:
            self._conn.execute(f"ROLLBACK TO SAVEPOINT {quoted_savepoint}")
            self._conn.execute(f"RELEASE SAVEPOINT {quoted_savepoint}")
            raise

        self._conn.execute(f"RELEASE SAVEPOINT {quoted_savepoint}")


def update_db(
    report_type: str,
    period: str,
    code: str,
    df: pd.DataFrame,
    *,
    drop_missing_metrics: bool = False,
) -> None:
    db_path = get_financial_db_path(report_type, period)

    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        _write_dataframe(
            conn,
            report_type,
            period,
            code,
            df,
            drop_missing_metrics=drop_missing_metrics,
        )
        conn.commit()


def _read_table_frame(conn: sqlite3.Connection, code: str) -> pd.DataFrame:
    return cast(
        pd.DataFrame,
        pd.read_sql_query(
            f'SELECT * FROM "{code}"',
            conn,
            index_col="metric",
        ),
    )


def _normalize_loaded_frame(df: pd.DataFrame) -> pd.DataFrame:
    converted = df.apply(pd.to_numeric, errors="coerce")
    if isinstance(converted, pd.Series):
        converted = converted.to_frame()
    normalized = cast(pd.DataFrame, converted)
    normalized.index = normalized.index.astype(str)
    normalized.columns = normalized.columns.astype(str)
    return normalized


class FinancialDBReader:
    def __init__(self, db_map: Mapping[GroupKey, Path] | None = None) -> None:
        self._db_map = dict(db_map or GROUP_DB_MAP)
        self._connections: dict[GroupKey, sqlite3.Connection] = {}
        self._cache: dict[tuple[str, str, str], pd.DataFrame] = {}

    def __enter__(self) -> FinancialDBReader:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def close(self) -> None:
        for conn in self._connections.values():
            conn.close()
        self._connections.clear()
        self._cache.clear()

    def _get_connection(self, report_type: str, period: str) -> sqlite3.Connection | None:
        group_key = cast(GroupKey, (report_type, period))
        db_path = self._db_map[group_key]
        if not db_path.exists():
            return None

        conn = self._connections.get(group_key)
        if conn is None:
            conn = sqlite3.connect(db_path)
            self._connections[group_key] = conn
        return conn

    def load_db(self, report_type: str, period: str, code: str) -> pd.DataFrame:
        cache_key = (report_type, period, code)
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached.copy()

        conn = self._get_connection(report_type, period)
        if conn is None:
            result = pd.DataFrame()
            self._cache[cache_key] = result
            return result.copy()

        try:
            df = _read_table_frame(conn, code)
        except (pd.errors.DatabaseError, sqlite3.OperationalError):
            result = pd.DataFrame()
            self._cache[cache_key] = result
            return result.copy()

        normalized = _normalize_loaded_frame(df)
        self._cache[cache_key] = normalized
        return normalized.copy()

    def load_quarter_statements(self, code: str) -> tuple[pd.DataFrame, ...]:
        return (
            self.load_db("ratio", "quarter", code),
            self.load_db("income", "quarter", code),
            self.load_db("balance", "quarter", code),
            self.load_db("cashflow", "quarter", code),
        )

    def prefetch_quarter_statements(self, codes: Iterable[str]) -> None:
        for code in dict.fromkeys(str(code) for code in codes):
            self.load_quarter_statements(code)


def load_db(report_type: str, period: str, code: str) -> pd.DataFrame:
    db_path = get_financial_db_path(report_type, period)
    if not db_path.exists():
        return pd.DataFrame()

    with sqlite3.connect(db_path) as conn:
        try:
            df = _read_table_frame(conn, code)
        except (pd.errors.DatabaseError, sqlite3.OperationalError):
            return pd.DataFrame()

    return _normalize_loaded_frame(df)
