from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, Iterator, Mapping, Optional, Sequence

import numpy as np
import pandas as pd
from tqdm import tqdm

from tools.financial_db import FinancialDBReader, load_db
from tools.kis_batch_quote import fetch_latest_quotes_batch
from tools.krx_ohlcv import KrxOHLCVReader
from .time_utils import today_kst

logger = logging.getLogger(__name__)

FACTOR_COLUMNS = [
    "1/per",
    "1/pbr",
    "1/psr",
    "1/pcr",
    "poir_q",
    "poir_y",
    "peir_q",
    "peir_y",
    "gp/a",
    "asset_shrink",
    "income_to_debt_growth",
    "volatility",
]

VALUE_METRICS: Sequence[str] = ("1/per", "1/pbr", "1/psr", "1/pcr")
MOMENTUM_METRICS: Sequence[str] = ("poir_q", "poir_y", "peir_q", "peir_y")
QUALITY_METRICS: Sequence[str] = (
    "gp/a",
    "income_to_debt_growth",
    "asset_shrink",
    "volatility",
)
SELECTION_FILTER_CONDITIONS: Sequence[Mapping[str, Any]] = (
    {"column": "asset_shrink", "mode": "abs", "value": 0.3, "direction": "up"},
    {"column": "amount", "mode": "abs", "value": 50_000_000, "direction": "down"},
    {"column": "F_score", "mode": "abs", "value": 2, "direction": "down"},
)
PRE_REMOTE_SELECTION_FILTERS: Sequence[Mapping[str, Any]] = (
    {"column": "asset_shrink", "mode": "abs", "value": 0.3, "direction": "up"},
)
AMOUNT_SELECTION_FILTERS: Sequence[Mapping[str, Any]] = (
    {"column": "amount", "mode": "abs", "value": 50_000_000, "direction": "down"},
)

_MAX_STOCK_API_RETRY = 10
_LOOKBACK_DAYS = 365
_QUOTE_LOOKBACK_DAYS = 30
_RANK_WINDOW_MIN_SIZE = 20
_FAILED_STOCK = object()


class LazyStockMap(Mapping[str, Any]):
    def __init__(self, codes: Sequence[str], kis: Any):
        self._codes = tuple(dict.fromkeys(codes))
        self._code_set = set(self._codes)
        self._kis = kis
        self._cache: dict[str, Any] = {}

    def __getitem__(self, key: str) -> Any:
        if key not in self._code_set:
            raise KeyError(key)

        stock = self._resolve_stock(key)
        if stock is _FAILED_STOCK:
            raise KeyError(key)

        return stock

    def __iter__(self) -> Iterator[str]:
        return iter(self._codes)

    def __len__(self) -> int:
        return len(self._codes)

    def get(self, key: str, default: Any = None) -> Any:
        if key not in self._code_set:
            return default

        stock = self._resolve_stock(key)
        if stock is _FAILED_STOCK:
            return default

        return stock

    @property
    def kis(self) -> Any:
        return self._kis

    def _resolve_stock(self, code: str) -> Any:
        cached = self._cache.get(code)
        if cached is not None:
            return cached

        try:
            stock = _retry_stock_call(
                lambda: self._kis.stock(code),
                _MAX_STOCK_API_RETRY,
                f"{code}의 stock 객체 생성에 {_MAX_STOCK_API_RETRY}회 연속 실패했습니다.",
            )
        except RuntimeError as exc:
            logger.warning("%s", exc)
            self._cache[code] = _FAILED_STOCK
            return _FAILED_STOCK

        self._cache[code] = stock
        return stock


def filter_risky(df: pd.DataFrame) -> pd.DataFrame:
    """위험(회피) 조건에 해당하지 않는 종목만 남겨 반환합니다."""
    if df.empty:
        return df.copy()

    mask = pd.Series(True, index=df.index)

    if "거래정지" in df:
        mask &= df["거래정지"].fillna("") != "Y"

    if "정리매매" in df:
        mask &= df["정리매매"].fillna("") != "Y"

    if "관리종목" in df:
        mask &= df["관리종목"].fillna("") != "Y"

    if "시장경고" in df:
        mask &= ~df["시장경고"].astype(str).fillna("").isin(["2", "3"])

    if "경고예고" in df:
        mask &= df["경고예고"].fillna("") != "Y"

    return df.loc[mask].copy()


def _empty_factor_result() -> dict[str, Any]:
    result: dict[str, Any] = {key: None for key in FACTOR_COLUMNS}
    result["eps"] = None
    result["bps"] = None
    result["sps"] = None
    result["cps"] = None
    result["__fscore_eligible"] = False
    return result


def _to_optional_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None

    return float(value)


def _load_quarter_statements(
    code: str,
    reader: FinancialDBReader | None = None,
) -> tuple[pd.DataFrame, ...]:
    """분기 재무제표 데이터를 로드합니다."""
    if reader is not None:
        return reader.load_quarter_statements(code)

    return (
        load_db("ratio", "quarter", code),
        load_db("income", "quarter", code),
        load_db("balance", "quarter", code),
        load_db("cashflow", "quarter", code),
    )


def _has_meaningful_period_data(df: pd.DataFrame, period: str) -> bool:
    if period not in df.columns:
        return False

    return bool(df[period].notna().any())


def _resolve_period_labels(
    ratio_quarter: pd.DataFrame,
    income_quarter: pd.DataFrame,
    balance_quarter: pd.DataFrame,
    cashflow_quarter: pd.DataFrame,
) -> tuple[str, str, str]:
    """공통된 날짜를 찾아 최근, 전분기, 전년동기 라벨을 반환합니다."""
    dates = set(ratio_quarter.columns)
    dates &= set(income_quarter.columns)
    dates &= set(balance_quarter.columns)
    dates &= set(cashflow_quarter.columns)

    if not dates:
        raise ValueError("No common dates found")

    cols_sorted = sorted(dates, key=pd.to_datetime, reverse=True)
    recent_label = next(
        (
            period
            for period in cols_sorted
            if _has_meaningful_period_data(ratio_quarter, period)
            and _has_meaningful_period_data(income_quarter, period)
            and _has_meaningful_period_data(balance_quarter, period)
            and _has_meaningful_period_data(cashflow_quarter, period)
        ),
        None,
    )

    if recent_label is None:
        raise ValueError("No common dates with meaningful data found")

    recent_dt = pd.to_datetime(recent_label, format="%Y/%m")
    prev_quarter = (recent_dt - pd.DateOffset(months=3)).strftime("%Y/%m")
    prev_year = (recent_dt - pd.DateOffset(years=1)).strftime("%Y/%m")
    recent = recent_dt.strftime("%Y/%m")

    return recent, prev_quarter, prev_year


def _retry_stock_call(func, attempts: int, error_message: str) -> Any:
    """주식 API 호출을 재시도합니다."""
    for attempt in range(attempts):
        try:
            return func()
        except Exception as exc:
            if attempt == attempts - 1:
                raise RuntimeError(error_message) from exc


def _signed_log(value: float) -> float:
    """부호를 유지한 로그 변환을 수행합니다."""
    return np.sign(value) * np.log1p(np.abs(value))


def _compute_volatility(stock: Any, code: str) -> tuple[float, bool]:
    """주가 변동성을 계산합니다."""
    one_year_ago = today_kst() - timedelta(days=_LOOKBACK_DAYS)
    chart = _retry_stock_call(
        lambda: stock.daily_chart(start=one_year_ago, period="week"),
        _MAX_STOCK_API_RETRY,
        f"{code}의 차트 데이터를 10회 연속 불러오기에 실패하였습니다.",
    )

    df_chart = pd.DataFrame(
        {
            "time": [bar.time for bar in chart.bars],
            "close_stock": [bar.close for bar in chart.bars],
        }
    ).dropna(subset=["close_stock"])

    if df_chart.shape[0] < 2:
        return np.nan, False

    df_chart["ret_stock"] = df_chart["close_stock"].pct_change().astype(float)
    df_chart = df_chart.dropna(subset=["ret_stock"])

    if df_chart.shape[0] < 1:
        return np.nan, False

    volatility = df_chart["ret_stock"].std()
    return float(-volatility), True


def _fetch_paidin_events(stock: Any, code: str) -> list[Any]:
    """유상증자 이벤트를 조회합니다."""
    end = today_kst()
    start = end - timedelta(days=_LOOKBACK_DAYS)
    return _retry_stock_call(
        lambda: stock.paidin_capin(
            start=start.strftime("%Y%m%d"),
            end=end.strftime("%Y%m%d"),
        ),
        _MAX_STOCK_API_RETRY,
        f"{code}의 차트 데이터를 10회 연속 불러오기에 실패하였습니다.",
    )


def _fetch_paidin_event_symbols(kis: Any) -> set[str] | None:
    end = today_kst()
    start = end - timedelta(days=_LOOKBACK_DAYS)
    try:
        events = kis.paidin_capin(
            symbol="",
            start=start.strftime("%Y%m%d"),
            end=end.strftime("%Y%m%d"),
            max_pages=100,
        )
    except Exception:
        return None

    return {
        str(item.symbol)
        for item in events
        if getattr(item, "symbol", None)
    }


def _calculate_valuation_factors(
    price: float | None,
    eps: float | None,
    bps: float | None,
    sps: float | None,
    cps: float | None,
) -> dict[str, float | None]:
    """가치 지표(PER, PBR, PSR, PCR의 역수)를 계산합니다."""
    if price in (0, None):
        return {"1/per": None, "1/pbr": None, "1/psr": None, "1/pcr": None}

    return {
        "1/per": 4 * eps / price if eps is not None else None,
        "1/pbr": 4 * bps / price if bps is not None else None,
        "1/psr": 4 * sps / price if sps is not None else None,
        "1/pcr": 4 * cps / price if cps is not None else None,
    }


def _calculate_momentum_factors(
    market_cap: float | None,
    income_quarter: pd.DataFrame,
    recent: str,
    prev_quarter: str,
    prev_year: str,
) -> dict[str, float | None]:
    """모멘텀 지표(이익 성장률 등)를 계산합니다."""
    
    def get_val(df, idx, col):
        return df.loc[idx, col] if idx in df.index and col in df.columns else None

    op_recent = get_val(income_quarter, "영업이익", recent)
    op_quarter = get_val(income_quarter, "영업이익", prev_quarter)
    op_year = get_val(income_quarter, "영업이익", prev_year)

    net_recent = get_val(income_quarter, "당기순이익", recent)
    net_quarter = get_val(income_quarter, "당기순이익", prev_quarter)
    net_year = get_val(income_quarter, "당기순이익", prev_year)

    delta_op_q = op_recent - op_quarter if op_recent is not None and op_quarter is not None else None
    delta_op_y = op_recent - op_year if op_recent is not None and op_year is not None else None
    delta_net_q = net_recent - net_quarter if net_recent is not None and net_quarter is not None else None
    delta_net_y = net_recent - net_year if net_recent is not None and net_year is not None else None

    if market_cap in (0, None):
        return {
            "poir_q": None, "poir_y": None, "peir_q": None, "peir_y": None,
            "delta_oper_income_q": delta_op_q, "delta_oper_income_y": delta_op_y,
            "delta_earnings_q": delta_net_q, "delta_earnings_y": delta_net_y,
        }

    return {
        "poir_q": delta_op_q / market_cap if delta_op_q is not None else None,
        "poir_y": delta_op_y / market_cap if delta_op_y is not None else None,
        "peir_q": delta_net_q / market_cap if delta_net_q is not None else None,
        "peir_y": delta_net_y / market_cap if delta_net_y is not None else None,
        "delta_oper_income_q": delta_op_q,
        "delta_oper_income_y": delta_op_y,
        "delta_earnings_q": delta_net_q,
        "delta_earnings_y": delta_net_y,
    }


def _calculate_quality_factors(
    income_quarter: pd.DataFrame,
    balance_quarter: pd.DataFrame,
    recent: str,
    prev_year: str,
) -> dict[str, float | None]:
    """퀄리티 지표(GP/A, 자산증감, 이익/부채 성장 괴리)를 계산합니다."""
    
    def get_val(df, idx, col, default=None):
        return df.loc[idx, col] if idx in df.index and col in df.columns else default

    # GP/A
    gross_profit = get_val(income_quarter, "매출총이익", recent)
    if gross_profit is None:
        gross_profit = get_val(income_quarter, "영업이익", recent)

    asset_recent = get_val(balance_quarter, "자산총계", recent) or get_val(balance_quarter, "자산", recent)
    asset_year = get_val(balance_quarter, "자산총계", prev_year) or get_val(balance_quarter, "자산", prev_year)

    gp_a = gross_profit / asset_recent if asset_recent not in (0, None) and gross_profit is not None else None

    # Asset Shrink
    asset_growth = (
        asset_recent / asset_year - 1
        if asset_recent is not None and asset_year not in (None, 0)
        else None
    )
    asset_shrink = -asset_growth if asset_growth is not None else None

    # Income to Debt Growth
    op_recent = get_val(income_quarter, "영업이익", recent, 0)
    op_year = get_val(income_quarter, "영업이익", prev_year, 0)

    short_debt_recent = get_val(balance_quarter, "단기차입금", recent, 0)
    long_debt_recent = get_val(balance_quarter, "장기차입금", recent, 0)
    debt_recent = (short_debt_recent or 0) + (long_debt_recent or 0)

    short_debt_year = get_val(balance_quarter, "단기차입금", prev_year, 0)
    long_debt_year = get_val(balance_quarter, "장기차입금", prev_year, 0)
    debt_year = (short_debt_year or 0) + (long_debt_year or 0)

    income_to_debt_recent = _signed_log(1 + op_recent) - _signed_log(1 + debt_recent)
    income_to_debt_year = _signed_log(1 + op_year) - _signed_log(1 + debt_year)
    
    return {
        "gp/a": gp_a,
        "asset_shrink": asset_shrink,
        "income_to_debt_growth": income_to_debt_recent - income_to_debt_year,
    }


def _calculate_f_score(
    income_quarter: pd.DataFrame,
    cashflow_quarter: pd.DataFrame,
    recent: str,
    *,
    has_paidin_event: bool,
) -> int:
    """F-Score를 계산합니다."""
    net_income = income_quarter.loc["당기순이익", recent] if "당기순이익" in income_quarter.index else 0
    operating_cash_flow = (
        cashflow_quarter.loc["영업활동으로인한현금흐름", recent]
        if "영업활동으로인한현금흐름" in cashflow_quarter.index
        else 0
    )
    return int(net_income > 0) + int(operating_cash_flow > 0) + int(not has_paidin_event)


def _calculate_f_score_for_row(
    row: pd.Series,
    stocks: Mapping[str, Any],
    reader: FinancialDBReader | None = None,
    paidin_event_symbols: set[str] | None = None,
) -> int | None:
    eligible = row.get("__fscore_eligible", False)
    if not isinstance(eligible, (bool, np.bool_)) or not bool(eligible):
        return None

    code = str(row["단축코드"])
    ratio_q, income_q, balance_q, cashflow_q = _load_quarter_statements(code, reader=reader)
    recent, _, _ = _resolve_period_labels(ratio_q, income_q, balance_q, cashflow_q)

    if paidin_event_symbols is not None:
        has_paidin_event = code in paidin_event_symbols
    else:
        stock = stocks[code]
        has_paidin_event = len(_fetch_paidin_events(stock, code)) > 0

    return _calculate_f_score(
        income_q,
        cashflow_q,
        recent,
        has_paidin_event=has_paidin_event,
    )


def _calculate_quant_factors_for_row(
    row: pd.Series,
    stocks: Mapping[str, Any],
    reader: FinancialDBReader | None = None,
    volatility_map: Mapping[str, tuple[float, bool]] | None = None,
) -> dict[str, Any]:
    """단일 종목에 대한 모든 퀀트 팩터를 계산합니다."""
    code = str(row["단축코드"])
    stock = stocks[code]
    raw_price = row.get("price")
    raw_market_cap = row.get("market_cap")
    price = _to_optional_float(raw_price)
    market_cap = _to_optional_float(raw_market_cap)

    if volatility_map is not None and code in volatility_map:
        volatility, has_volatility = volatility_map[code]
    else:
        volatility, has_volatility = _compute_volatility(stock, code)
    if not has_volatility:
        result = _empty_factor_result()
        result["volatility"] = volatility
        return result

    ratio_q, income_q, balance_q, cashflow_q = _load_quarter_statements(code, reader=reader)
    recent, prev_q, prev_y = _resolve_period_labels(ratio_q, income_q, balance_q, cashflow_q)

    # Basic Metrics
    eps = ratio_q.loc["EPS", recent] if "EPS" in ratio_q.index else None
    bps = ratio_q.loc["BPS", recent] if "BPS" in ratio_q.index else None
    sps = ratio_q.loc["SPS", recent] if "SPS" in ratio_q.index else None
    cps = ratio_q.loc["CPS", recent] if "CPS" in ratio_q.index else None

    # Calculate Factors
    valuation = _calculate_valuation_factors(price, eps, bps, sps, cps)
    momentum = _calculate_momentum_factors(market_cap, income_q, recent, prev_q, prev_y)
    quality = _calculate_quality_factors(income_q, balance_q, recent, prev_y)

    return {
        **valuation,
        **momentum,
        **quality,
        "volatility": volatility,
        "__fscore_eligible": True,
        "eps": eps,
        "bps": bps,
        "sps": sps,
        "cps": cps,
    }


def get_quant_factors(
    df: pd.DataFrame,
    stocks: Mapping[str, Any],
    reader: FinancialDBReader | None = None,
    volatility_map: Mapping[str, tuple[float, bool]] | None = None,
) -> pd.DataFrame:
    """DataFrame의 각 종목에 대해 퀀트 팩터를 계산하여 추가합니다."""
    df_out = df.copy()
    results = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="팩터 계산"):
        try:
            results.append(
                _calculate_quant_factors_for_row(
                    row,
                    stocks,
                    reader=reader,
                    volatility_map=volatility_map,
                )
            )
        except Exception as exc:
            # print(f"Error calculating factors for {row.get('단축코드')}: {exc}")
            results.append(_empty_factor_result())

    metrics = pd.DataFrame(results, index=df_out.index)
    df_out[metrics.columns] = metrics
    return df_out


def get_f_scores(
    df: pd.DataFrame,
    stocks: Mapping[str, Any],
    reader: FinancialDBReader | None = None,
    paidin_event_symbols: set[str] | None = None,
) -> pd.DataFrame:
    df_out = df.copy()
    scores = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="F-Score 계산"):
        try:
            scores.append(
                _calculate_f_score_for_row(
                    row,
                    stocks,
                    reader=reader,
                    paidin_event_symbols=paidin_event_symbols,
                )
            )
        except Exception:
            scores.append(None)

    df_out["F_score"] = scores
    return df_out


def filter_stocks(
    df: pd.DataFrame,
    conditions: Optional[Sequence[Mapping[str, Any]]] = None,
    match: str = "any",
) -> pd.DataFrame:
    """사용자 정의 조건에 따라 종목을 필터링합니다."""
    if "단축코드" not in df.columns or not conditions:
        return df.copy()

    match_mode = match.lower()
    if match_mode not in {"any", "all"}:
        raise ValueError("match 파라미터는 'any' 또는 'all'이어야 합니다.")

    masks = []
    for cond in conditions:
        column = cond.get("column")
        mode = cond.get("mode", "abs")
        value = cond.get("value")
        direction = cond.get("direction", "up")

        if column not in df.columns or value is None:
            continue

        series = df[column]

        if mode == "abs":
            if direction == "up":
                mask = series >= value
            elif direction == "down":
                mask = series <= value
            else:
                continue
        elif mode == "rel":
            value = min(max(value, 0), 1)
            if direction == "up":
                cutoff = series.quantile(1 - value)
                mask = series >= cutoff
            elif direction == "down":
                cutoff = series.quantile(value)
                mask = series <= cutoff
            else:
                continue
        else:
            continue

        masks.append(mask.fillna(False))

    if not masks:
        return df.copy()

    mask_df = pd.concat(masks, axis=1)
    combined_mask = mask_df.all(axis=1) if match_mode == "all" else mask_df.any(axis=1)
    combined_mask = pd.Series(combined_mask, index=df.index).fillna(False)

    filtered = df.loc[~combined_mask].copy()
    return filtered.reset_index(drop=True)


def get_rank(
    df: pd.DataFrame,
    value_metrics: Sequence[str],
    momentum_metrics: Sequence[str],
    quality_metrics: Sequence[str],
) -> pd.DataFrame:
    """밸류/모멘텀/퀄리티 지표를 기반으로 종합 랭킹을 계산합니다."""
    df_out = df.copy()
    metric_groups = {
        "value": value_metrics,
        "momentum": momentum_metrics,
        "quality": quality_metrics,
    }

    temp_cols = []
    all_metric_rank_cols = []

    for group_name, metrics in metric_groups.items():
        if not metrics:
            continue

        metric_rank_cols = []
        for metric in metrics:
            if metric not in df_out.columns:
                raise KeyError(f"지표 '{metric}'가 데이터프레임에 없습니다.")

            rank_col = f"__rank_{group_name}_{metric}"
            df_out[rank_col] = df_out[metric].rank(
                method="min",
                ascending=False,
                na_option="bottom",
            )
            metric_rank_cols.append(rank_col)

        if not metric_rank_cols:
            continue

        group_score_col = f"__group_score_{group_name}"
        df_out[group_score_col] = df_out[metric_rank_cols].mean(axis=1, skipna=True)
        group_rank_col = f"rank_{group_name}"
        df_out[group_rank_col] = df_out[group_score_col].rank(
            method="min",
            ascending=True,
            na_option="bottom",
        )

        temp_cols.extend(metric_rank_cols)
        temp_cols.append(group_score_col)
        all_metric_rank_cols.extend(metric_rank_cols)

    if all_metric_rank_cols:
        total_score = pd.Series(
            df_out[all_metric_rank_cols].mean(axis=1, skipna=True),
            index=df_out.index,
        )
        df_out["rank_total"] = total_score.rank(
            method="min",
            ascending=True,
            na_option="bottom",
        )
    else:
        df_out["rank_total"] = np.nan

    if temp_cols:
        df_out = df_out.drop(columns=temp_cols)

    df_out = df_out.sort_values("rank_total").reset_index(drop=True)
    return df_out


def get_stock_quote(df: pd.DataFrame, stocks: Mapping[str, Any]) -> pd.DataFrame:
    kis = getattr(stocks, "kis", None)
    if kis is not None:
        return fetch_latest_quotes_batch(
            df.drop(columns=[column for column in ["market_cap"] if column in df.columns]),
            kis,
            retry=_MAX_STOCK_API_RETRY,
            progress_desc="종목 시세 조회",
        )

    df_out = df.copy()
    results = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="종목 시세 조회"):
        stock_code = str(row["단축코드"])
        stock = stocks.get(stock_code)

        if stock is None:
            results.append({"price": None, "market_cap": None})
            continue

        try:
            quote = _retry_stock_call(
                stock.quote,
                _MAX_STOCK_API_RETRY,
                f"{stock_code}의 시세 데이터를 {_MAX_STOCK_API_RETRY}회 연속 불러오기에 실패하였습니다.",
            )
            price = float(quote.price)
            market_cap = float(quote.market_cap)
        except RuntimeError as exc:
            # print(exc)
            price = None
            market_cap = None

        results.append(
            {
                "price": price,
                "market_cap": market_cap,
            }
        )

    metrics = pd.DataFrame(results, index=df_out.index)
    df_out[metrics.columns] = metrics
    return df_out


def get_average_amount(
    df: pd.DataFrame,
    stocks: Mapping[str, Any],
    krx_reader: KrxOHLCVReader | None = None,
) -> pd.DataFrame:
    df_out = df.copy()
    results = []
    amount_map: dict[str, int | None] = {}

    if krx_reader is not None:
        amount_map = krx_reader.compute_amounts(
            df_out["단축코드"].astype(str).tolist(),
            end_day=today_kst() - timedelta(days=1),
            lookback_days=_QUOTE_LOOKBACK_DAYS,
        )

    for _, row in tqdm(df.iterrows(), total=len(df), desc="거래대금 조회"):
        stock_code = str(row["단축코드"])
        if stock_code in amount_map and amount_map[stock_code] is not None:
            results.append({"amount": amount_map[stock_code]})
            continue

        stock = stocks.get(stock_code)

        if stock is None:
            results.append({"amount": None})
            continue

        stock_obj = stock

        try:
            today = today_kst()
            start_date = today - timedelta(days=_QUOTE_LOOKBACK_DAYS)
            end_date = today - timedelta(days=1)
            chart = _retry_stock_call(
                lambda start=start_date, end=end_date: stock_obj.daily_chart(
                    start=start,
                    end=end,
                    period="day",
                ),
                _MAX_STOCK_API_RETRY,
                f"{stock_code}의 차트 데이터를 {_MAX_STOCK_API_RETRY}회 연속 불러오기에 실패하였습니다.",
            )
            bars = getattr(chart, "bars", None)
            if bars:
                amount = int(sum(bar.amount for bar in bars) / len(bars))
            else:
                amount = None
        except RuntimeError as exc:
            # print(exc)
            amount = None

        results.append({"amount": amount})

    metrics = pd.DataFrame(results, index=df_out.index)
    df_out[metrics.columns] = metrics
    return df_out


def _evaluate_ranked_candidates(
    df_ranked: pd.DataFrame,
    stocks: Mapping[str, Any],
    *,
    top_n: int,
    include_full_data: bool,
    reader: FinancialDBReader | None = None,
    krx_reader: KrxOHLCVReader | None = None,
) -> pd.DataFrame:
    if df_ranked.empty:
        return df_ranked.copy()

    df_base = filter_stocks(df_ranked, PRE_REMOTE_SELECTION_FILTERS)
    if df_base.empty:
        return df_base

    window_size = max(top_n, _RANK_WINDOW_MIN_SIZE)
    paidin_event_symbols: set[str] | None = None
    paidin_event_symbols_loaded = False
    survivors: list[pd.DataFrame] = []
    survivor_count = 0
    kis = getattr(stocks, "kis", None)

    for start in range(0, len(df_base), window_size):
        df_window = df_base.iloc[start : start + window_size].copy()
        if df_window.empty:
            continue

        df_with_amount = get_average_amount(df_window, stocks, krx_reader=krx_reader)
        df_amount_survivors = filter_stocks(df_with_amount, AMOUNT_SELECTION_FILTERS)
        if df_amount_survivors.empty:
            if not include_full_data and survivor_count >= top_n:
                break
            continue

        if not paidin_event_symbols_loaded and kis is not None:
            paidin_event_symbols = _fetch_paidin_event_symbols(kis)
            paidin_event_symbols_loaded = True

        df_with_f_score = get_f_scores(
            df_amount_survivors,
            stocks,
            reader=reader,
            paidin_event_symbols=paidin_event_symbols,
        )
        df_filtered = apply_custom_selection_filters(df_with_f_score)
        if not df_filtered.empty:
            survivors.append(df_filtered)
            survivor_count += len(df_filtered)

        if not include_full_data and survivor_count >= top_n:
            break

    if not survivors:
        return df_base.iloc[0:0].copy()

    return pd.concat(survivors, ignore_index=True)


def apply_risk_filters(df: pd.DataFrame) -> pd.DataFrame:
    """위험(회피) 조건을 충족하지 않는 종목만 남깁니다."""
    if df.empty:
        return df.copy()

    df_filtered = filter_risky(df)
    if df_filtered.empty:
        return df_filtered

    return df_filtered.reset_index(drop=True)


def apply_smallcap_filter(df: pd.DataFrame) -> pd.DataFrame:
    """시가총액 하위 구간만 남겨 소형주 중심으로 필터링합니다."""
    if df.empty:
        return df.copy()

    if "market_cap" not in df.columns:
        return df.reset_index(drop=True)

    market_caps = df["market_cap"].dropna()
    if market_caps.empty:
        return df.reset_index(drop=True)

    threshold = market_caps.quantile(0.2)
    df_smallcap = df.loc[df["market_cap"] <= threshold].copy()
    return df_smallcap.reset_index(drop=True)


def apply_custom_selection_filters(df: pd.DataFrame) -> pd.DataFrame:
    """사용자 정의 조건(커스텀)으로 최종 필터링합니다."""
    if df.empty:
        return df.copy()

    df_filtered = filter_stocks(df, SELECTION_FILTER_CONDITIONS)
    return df_filtered.reset_index(drop=True)


def create_stock_objects(df: pd.DataFrame, kis: Any) -> Mapping[str, Any]:
    """DataFrame의 '단축코드' 컬럼을 기반으로 KIS stock 객체를 생성합니다."""
    unique_codes = (
        df["단축코드"].dropna().astype(str).drop_duplicates().tolist()
        if "단축코드" in df.columns
        else []
    )
    return LazyStockMap(unique_codes, kis)


def select_stocks(
    df_codes: pd.DataFrame,
    stocks: Mapping[str, Any],
    top_n: int = 20,
    include_full_data: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]:
    """위험 종목을 제거하고 퀀트 지표 기반 랭킹을 적용해 우선 종목을 선정한다.

    Args:
        df_codes: 코드 마스터 데이터프레임.
        stocks: KIS stock 객체 매핑.
        top_n: 반환할 최종 종목 수.
        include_full_data: True일 경우 최종 후보 전체를 함께 반환.

    Returns:
        기본적으로 단축코드/한글명으로 구성된 상위 top_n DataFrame을 반환합니다.
        include_full_data=True이면 (상위 top_n DataFrame, 필터링 및 랭킹 정보가
        포함된 전체 DataFrame)의 튜플을 반환합니다.
    """
    def _return_result(df: pd.DataFrame) -> pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]:
        if include_full_data:
            return df, df
        return df

    if df_codes.empty:
        return _return_result(df_codes.reindex(columns=["단축코드", "한글명"]))

    if top_n <= 0:
        raise ValueError("top_n은 1 이상이어야 합니다.")

    df_candidates = apply_risk_filters(df_codes)
    if df_candidates.empty:
        return _return_result(df_candidates.reindex(columns=["단축코드", "한글명"]))

    df_with_quotes = get_stock_quote(df_candidates, stocks)

    if df_with_quotes.empty:
        return _return_result(df_codes.reindex(columns=["단축코드", "한글명"]))

    df_smallcap = apply_smallcap_filter(df_with_quotes)
    if df_smallcap.empty:
        return _return_result(df_smallcap.reindex(columns=["단축코드", "한글명"]))

    with FinancialDBReader() as db_reader:
        db_reader.prefetch_quarter_statements(df_smallcap["단축코드"].astype(str).tolist())
        df_with_factors = get_quant_factors(
            df_smallcap,
            stocks,
            reader=db_reader,
        ).reset_index(drop=True)

        df_ranked = get_rank(
            df_with_factors,
            value_metrics=VALUE_METRICS,
            momentum_metrics=MOMENTUM_METRICS,
            quality_metrics=QUALITY_METRICS,
        )

        df_after_custom = _evaluate_ranked_candidates(
            df_ranked,
            stocks,
            top_n=top_n,
            include_full_data=include_full_data,
            reader=db_reader,
        )

    if df_after_custom.empty:
        return _return_result(df_after_custom.reindex(columns=["단축코드", "한글명"]))

    helper_columns = [column for column in df_after_custom.columns if column.startswith("__")]
    df_filtered = df_after_custom.drop(columns=helper_columns, errors="ignore").sort_values("rank_total").reset_index(drop=True)
    selection_columns = ("단축코드", "한글명")
    df_top = df_filtered.loc[:, selection_columns].head(top_n).reset_index(drop=True)

    if include_full_data:
        return df_top, df_filtered

    return df_top


__all__ = [
    "filter_risky",
    "get_quant_factors",
    "filter_stocks",
    "get_rank",
    "get_stock_quote",
    "get_average_amount",
    "get_f_scores",
    "create_stock_objects",
    "LazyStockMap",
    "select_stocks",
    "VALUE_METRICS",
    "MOMENTUM_METRICS",
    "QUALITY_METRICS",
    "SELECTION_FILTER_CONDITIONS",
    "apply_risk_filters",
    "apply_smallcap_filter",
    "apply_custom_selection_filters",
]
