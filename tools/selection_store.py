from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime
from typing import Any, Optional, Union

import pandas as pd
from pykis import PyKis
from pykis.api.stock.quote import quote as fetch_quote

from tools.financial_db import DB_DIR, STOCK_SELECTION_DB_PATH
from tools.quant_utils import (
    MOMENTUM_METRICS,
    QUALITY_METRICS,
    VALUE_METRICS,
    apply_custom_selection_filters,
    apply_risk_filters,
    apply_smallcap_filter,
    get_rank,
)
from .time_utils import today_kst

from tqdm import tqdm

logger = logging.getLogger(__name__)

_REQUIRED_VALUE_COLUMNS = {"eps", "bps", "sps", "cps"}
_REQUIRED_DELTA_COLUMNS = {
    "delta_oper_income_q",
    "delta_oper_income_y",
    "delta_earnings_q",
    "delta_earnings_y",
}

def _normalize_selection_table_name(
    table_date: Union[str, date, datetime, None],
) -> str:
    """날짜 입력을 YYYYMMDD 형식으로 정규화."""
    if table_date is None:
        normalized_date = today_kst()
    elif isinstance(table_date, datetime):
        normalized_date = table_date.date()
    elif isinstance(table_date, date):
        normalized_date = table_date
    else:
        parsed = pd.to_datetime(table_date, errors="raise")
        if pd.isna(parsed):
            raise ValueError("날짜를 해석할 수 없습니다.")
        normalized_date = parsed.date()

    return normalized_date.strftime("%Y%m%d")


def save_stock_selection(
    df: pd.DataFrame,
    table_date: Union[str, date, datetime, None] = None,
) -> None:
    """종목 선정 스냅샷을 날짜별 테이블에 저장합니다."""
    required_columns = {"단축코드", "한글명"}
    if not required_columns.issubset(df.columns):
        missing = ", ".join(sorted(required_columns - set(df.columns)))
        raise KeyError(f"다음 컬럼이 필요합니다: {missing}")

    table_name = _normalize_selection_table_name(table_date)
    DB_DIR.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(STOCK_SELECTION_DB_PATH) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)


_ALLOWED_MARKETS = {
    "KRX",
    "KOSPI",
    "KOSDAQ",
    "KONEX",
    "NASDAQ",
    "NYSE",
    "AMEX",
    "TYO",
    "HKEX",
    "HNX",
    "HSX",
    "SSE",
    "SZSE",
}


def _resolve_market_type(raw_market: Any) -> str:
    """시장 정보를 PyKis가 이해하는 MARKET_TYPE 문자열로 변환."""
    if raw_market is None:
        return "KRX"

    normalized = str(raw_market).upper()
    if normalized not in _ALLOWED_MARKETS:
        logger.debug("알 수 없는 시장구분 %s, KRX로 대체합니다.", raw_market)
        return "KRX"

    if normalized in {"KOSPI", "KOSDAQ", "KONEX"}:
        return "KRX"

    return normalized


def _fetch_latest_quotes(
    df: pd.DataFrame,
    kis: PyKis,
    retry: int = 5,
) -> pd.DataFrame:
    """종목별 최신 가격과 시가총액을 수집해 병합합니다."""
    records: list[dict[str, Any]] = []

    unique_codes = (
        df.loc[:, ["단축코드", "시장구분"]]
        if "시장구분" in df.columns
        else df.loc[:, ["단축코드"]].assign(시장구분=None)
    )

    for _, row in tqdm(unique_codes.drop_duplicates(subset=["단축코드"]).iterrows(), total=unique_codes["단축코드"].nunique(), desc="Fetching Quotes"):
        code = row["단축코드"]
        market = _resolve_market_type(row.get("시장구분"))
        price = None
        market_cap = None

        for attempt in range(retry):
            try:
                quote = fetch_quote(kis, symbol=code, market=market)
                raw_price = getattr(quote, "price", None)
                raw_cap = getattr(quote, "market_cap", None)
                price = float(raw_price) if raw_price is not None else None
                market_cap = float(raw_cap) if raw_cap is not None else None
                break
            except Exception as exc:  # noqa: BLE001
                if attempt == retry - 1:
                    tqdm.write(f"시세 조회 실패 ({code}/{market}): {exc}")
                continue

        records.append({"단축코드": code, "price": price, "market_cap": market_cap})

    df_quotes = pd.DataFrame(records)
    merged = df.merge(df_quotes, on="단축코드", how="left")
    return merged


def _recalculate_dynamic_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """가격에 따라 변하는 지표를 최신 시세 기준으로 다시 계산합니다."""
    df_out = df.copy()

    for column, source in {
        "1/per": "eps",
        "1/pbr": "bps",
        "1/psr": "sps",
        "1/pcr": "cps",
    }.items():
        numerator = df_out[source]
        denominator = df_out["price"]
        mask = (
            pd.notna(numerator)
            & pd.notna(denominator)
            & (denominator != 0)
        )
        df_out.loc[mask, column] = 4 * numerator[mask] / denominator[mask]
        df_out.loc[~mask, column] = pd.NA

    for column, source in {
        "poir_q": "delta_oper_income_q",
        "poir_y": "delta_oper_income_y",
        "peir_q": "delta_earnings_q",
        "peir_y": "delta_earnings_y",
    }.items():
        numerator = df_out[source]
        denominator = df_out["market_cap"]
        mask = (
            pd.notna(numerator)
            & pd.notna(denominator)
            & (denominator != 0)
        )
        df_out.loc[mask, column] = numerator[mask] / denominator[mask]
        df_out.loc[~mask, column] = pd.NA

    return df_out


def _has_required_columns(df: pd.DataFrame) -> bool:
    required = {"단축코드", "한글명"} | _REQUIRED_VALUE_COLUMNS | _REQUIRED_DELTA_COLUMNS
    missing = required - set(df.columns)
    if missing:
        logger.warning("재계산에 필요한 컬럼이 부족합니다: %s", ", ".join(sorted(missing)))
        return False
    return True


def _trim_result(df: pd.DataFrame, top_n: Optional[int]) -> pd.DataFrame:
    if top_n is None:
        return df.reset_index(drop=True)
    return df.head(top_n).reset_index(drop=True)


def load_stock_selection(
    table_date: Union[str, date, datetime, None] = None,
    *,
    kis: PyKis,
    rerank: bool = True,
    top_n: Optional[int] = 20,
) -> pd.DataFrame:
    """저장된 날짜별 종목 선정을 DataFrame으로 읽어옵니다."""
    if not STOCK_SELECTION_DB_PATH.exists():
        raise KeyError("저장된 종목 선정 테이블이 없습니다.")

    with sqlite3.connect(STOCK_SELECTION_DB_PATH) as conn:
        if table_date is None:
            tables = [
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            ]
            if not tables:
                raise KeyError("저장된 종목 선정 테이블이 없습니다.")
            table_name = max(tables)
        else:
            table_name = _normalize_selection_table_name(table_date)
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            )
            if cursor.fetchone() is None:
                raise KeyError(f"{table_name} 테이블이 존재하지 않습니다.")

        df = pd.read_sql_query(
            f"SELECT * FROM '{table_name}'",
            conn,
        )

    if df.empty:
        return _trim_result(df, top_n)

    if not _has_required_columns(df):
        return _trim_result(df, top_n)

    df_for_rank = df.copy()

    if rerank:
        if kis is None:
            raise ValueError("kis 인스턴스가 필요합니다.")
        
        # 재랭킹 시에는 DB에 저장된 과거 가격/시가총액 정보를 제거하고 새로 조회
        cols_to_drop = [c for c in ["price", "market_cap"] if c in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
            
        df_with_quotes = _fetch_latest_quotes(df, kis)
        if df_with_quotes["price"].isna().all():
            logger.warning("가격 정보를 불러오지 못해 저장된 데이터를 그대로 사용합니다.")
        else:
            df_for_rank = _recalculate_dynamic_metrics(df_with_quotes)

    df_after_risk = apply_risk_filters(df_for_rank)
    if df_after_risk.empty:
        return df_after_risk

    df_pre_rank = apply_smallcap_filter(df_after_risk)
    if df_pre_rank.empty:
        return df_pre_rank

    df_ranked = get_rank(
        df_pre_rank,
        value_metrics=VALUE_METRICS,
        momentum_metrics=MOMENTUM_METRICS,
        quality_metrics=QUALITY_METRICS,
    )

    df_post_custom = apply_custom_selection_filters(df_ranked)
    if df_post_custom.empty:
        return df_post_custom

    df_ranked = df_post_custom.sort_values("rank_total").reset_index(drop=True)
    return _trim_result(df_ranked, top_n)


__all__ = ["save_stock_selection", "load_stock_selection"]

