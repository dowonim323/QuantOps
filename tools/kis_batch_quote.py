from __future__ import annotations

from typing import Any, Iterable

import pandas as pd
from pykis import MARKET_TYPE, PyKis
from pykis.api.stock.quote import quote as fetch_quote
from tqdm import tqdm

from .retry import retry_with_backoff

_BATCH_SIZE = 30
_KRX_MARKETS = {None, "KRX", "KOSPI", "KOSDAQ", "KONEX"}
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


def _chunked(values: list[str], size: int) -> Iterable[list[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _normalize_market_type(raw_market: Any) -> MARKET_TYPE:
    if raw_market is None:
        return "KRX"

    normalized = str(raw_market).upper()
    if normalized not in _ALLOWED_MARKETS:
        return "KRX"

    if normalized in {"KRX", "KOSPI", "KOSDAQ", "KONEX"}:
        return "KRX"

    return normalized  # type: ignore[return-value]


def _is_krx_market(raw_market: Any) -> bool:
    return raw_market is None or str(raw_market).upper() in _KRX_MARKETS


def _to_optional_float(value: Any) -> float | None:
    if value is None:
        return None

    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        return float(text)
    except ValueError:
        return None


def _build_multi_quote_params(codes: list[str]) -> dict[str, str]:
    params: dict[str, str] = {}
    for index, code in enumerate(codes, start=1):
        params[f"FID_COND_MRKT_DIV_CODE_{index}"] = "J"
        params[f"FID_INPUT_ISCD_{index}"] = code
    return params


def _fetch_multi_quote_chunk(kis: PyKis, codes: list[str]) -> dict[str, float | None]:
    response = kis.fetch(
        "/uapi/domestic-stock/v1/quotations/intstock-multprice",
        api="FHKST11300006",
        params=_build_multi_quote_params(codes),
        domain="real",
        verbose=False,
    )
    output = getattr(response, "output", [])
    prices: dict[str, float | None] = {}

    for item in output:
        payload = getattr(item, "__data__", item)
        code = str(payload.get("inter_shrn_iscd", "")).strip()
        if not code:
            continue
        prices[code] = _to_optional_float(payload.get("inter2_prpr"))

    return prices


def _fetch_single_quote_record(
    kis: PyKis,
    code: str,
    market: MARKET_TYPE,
    fallback_market_cap: float | None,
) -> dict[str, Any]:
    quote = fetch_quote(kis, symbol=code, market=market)
    price = _to_optional_float(getattr(quote, "price", None))
    market_cap = _to_optional_float(getattr(quote, "market_cap", None))
    if market_cap is None:
        market_cap = fallback_market_cap

    return {
        "단축코드": code,
        "price": price,
        "market_cap": market_cap,
    }


def _batch_record_from_row(row: pd.Series, price: float | None) -> dict[str, Any]:
    market_cap = _to_optional_float(row.get("_fallback_market_cap"))

    return {
        "단축코드": str(row["단축코드"]),
        "price": price,
        "market_cap": market_cap,
    }


def fetch_latest_quotes_batch(
    df: pd.DataFrame,
    kis: PyKis,
    *,
    retry: int = 5,
    progress_desc: str = "Fetching Quotes",
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    fallback_market_cap = (
        pd.Series(df["market_cap"], index=df.index)
        if "market_cap" in df.columns
        else None
    )
    base_df = df.drop(columns=[column for column in ["price", "market_cap"] if column in df.columns]).copy()

    columns = ["단축코드"]
    if "시장구분" in base_df.columns:
        columns.append("시장구분")
    if fallback_market_cap is not None:
        base_df = base_df.copy()
        base_df["_fallback_market_cap"] = fallback_market_cap
        columns.append("_fallback_market_cap")

    unique_rows = base_df.loc[:, columns].drop_duplicates(subset=["단축코드"]).reset_index(drop=True)
    records: list[dict[str, Any]] = []

    with tqdm(total=len(unique_rows), desc=progress_desc) as progress:
        krx_rows = unique_rows[unique_rows.get("시장구분").map(_is_krx_market) if "시장구분" in unique_rows.columns else pd.Series([True] * len(unique_rows))]

        for chunk_codes in _chunked(krx_rows["단축코드"].astype(str).tolist(), _BATCH_SIZE):
            chunk_rows = krx_rows[krx_rows["단축코드"].astype(str).isin(chunk_codes)]

            success, price_map = retry_with_backoff(
                lambda codes=chunk_codes: _fetch_multi_quote_chunk(kis, codes),
                max_retries=retry,
                initial_delay=0.5,
                max_delay=5.0,
                backoff_factor=1.5,
                context=f"KIS multi quote ({len(chunk_codes)} symbols)",
            )

            if success and price_map is not None:
                for _, row in chunk_rows.iterrows():
                    code = str(row["단축코드"])
                    if code in price_map:
                        record = _batch_record_from_row(row, price_map[code])
                        fallback_cap = _to_optional_float(row.get("_fallback_market_cap"))
                        if record["price"] is None:
                            market = _normalize_market_type(row.get("시장구분"))
                            fallback_cap = _to_optional_float(row.get("_fallback_market_cap"))
                            success_single, fallback_record = retry_with_backoff(
                                lambda c=code, m=market, cap=fallback_cap: _fetch_single_quote_record(kis, c, m, cap),
                                max_retries=retry,
                                initial_delay=0.5,
                                max_delay=5.0,
                                backoff_factor=1.5,
                                context=f"Fallback single quote ({code})",
                            )
                            record = (
                                fallback_record
                                if success_single and fallback_record is not None
                                else {"단축코드": code, "price": None, "market_cap": fallback_cap}
                            )
                        elif record["market_cap"] is None and fallback_cap is not None:
                            record["market_cap"] = fallback_cap
                        elif record["market_cap"] is None:
                            market = _normalize_market_type(row.get("시장구분"))
                            success_single, fallback_record = retry_with_backoff(
                                lambda c=code, m=market: _fetch_single_quote_record(kis, c, m, None),
                                max_retries=retry,
                                initial_delay=0.5,
                                max_delay=5.0,
                                backoff_factor=1.5,
                                context=f"Fallback single quote ({code})",
                            )
                            record = (
                                fallback_record
                                if success_single and fallback_record is not None
                                else {"단축코드": code, "price": None, "market_cap": None}
                            )
                        records.append(record)
                    else:
                        market = _normalize_market_type(row.get("시장구분"))
                        fallback_cap = _to_optional_float(row.get("_fallback_market_cap"))
                        success_single, fallback_record = retry_with_backoff(
                            lambda c=code, m=market, cap=fallback_cap: _fetch_single_quote_record(kis, c, m, cap),
                            max_retries=retry,
                            initial_delay=0.5,
                            max_delay=5.0,
                            backoff_factor=1.5,
                            context=f"Fallback single quote ({code})",
                        )
                        records.append(
                            fallback_record
                            if success_single and fallback_record is not None
                            else {"단축코드": code, "price": None, "market_cap": None}
                        )
            else:
                for _, row in chunk_rows.iterrows():
                    code = str(row["단축코드"])
                    market = _normalize_market_type(row.get("시장구분"))
                    fallback_cap = _to_optional_float(row.get("_fallback_market_cap"))
                    success_single, fallback_record = retry_with_backoff(
                        lambda c=code, m=market, cap=fallback_cap: _fetch_single_quote_record(kis, c, m, cap),
                        max_retries=retry,
                        initial_delay=0.5,
                        max_delay=5.0,
                        backoff_factor=1.5,
                        context=f"Fallback single quote ({code})",
                    )
                    records.append(
                        fallback_record
                        if success_single and fallback_record is not None
                        else {"단축코드": code, "price": None, "market_cap": fallback_cap}
                    )

            progress.update(len(chunk_rows))

        non_krx_rows = unique_rows[~unique_rows.index.isin(krx_rows.index)]
        for _, row in non_krx_rows.iterrows():
            code = str(row["단축코드"])
            market = _normalize_market_type(row.get("시장구분"))
            fallback_cap = _to_optional_float(row.get("_fallback_market_cap"))
            success_single, fallback_record = retry_with_backoff(
                lambda c=code, m=market, cap=fallback_cap: _fetch_single_quote_record(kis, c, m, cap),
                max_retries=retry,
                initial_delay=0.5,
                max_delay=5.0,
                backoff_factor=1.5,
                context=f"Single quote ({code})",
            )
            records.append(
                fallback_record
                if success_single and fallback_record is not None
                else {"단축코드": code, "price": None, "market_cap": None}
            )
            progress.update(1)

    df_quotes = pd.DataFrame(records)
    merged = base_df.merge(df_quotes, on="단축코드", how="left")

    if fallback_market_cap is not None:
        merged_market_cap = pd.Series(merged["market_cap"], index=merged.index)
        merged["market_cap"] = merged_market_cap.where(
            merged_market_cap.notna(),
            fallback_market_cap,
        )

    return merged


__all__ = ["fetch_latest_quotes_batch"]
