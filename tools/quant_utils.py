from __future__ import annotations

from datetime import timedelta
from typing import Any, Mapping, Optional, Sequence

import numpy as np
import pandas as pd
from tqdm import tqdm

from tools.financial_db import load_db
from .time_utils import today_kst

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
    "F_score",
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

_MAX_STOCK_API_RETRY = 10
_LOOKBACK_DAYS = 365
_QUOTE_LOOKBACK_DAYS = 30


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
        mask &= ~df["시장경고"].astype(str).fillna("").isin({"2", "3"})

    if "경고예고" in df:
        mask &= df["경고예고"].fillna("") != "Y"

    return df.loc[mask].copy()


def _empty_factor_result() -> dict[str, Any]:
    return {key: None for key in FACTOR_COLUMNS}


def _load_quarter_statements(code: str) -> tuple[pd.DataFrame, ...]:
    """분기 재무제표 데이터를 로드합니다."""
    return (
        load_db("ratio", "quarter", code),
        load_db("income", "quarter", code),
        load_db("balance", "quarter", code),
        load_db("cashflow", "quarter", code),
    )


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

    cols_sorted = sorted(dates, key=pd.to_datetime)
    dates_sorted = pd.to_datetime(cols_sorted, format="%Y/%m")

    recent_dt = dates_sorted[-1]
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

    return -df_chart["ret_stock"].std(), True


def _fetch_paidin_events(stock: Any, code: str) -> list[Any]:
    """유상증자 이벤트를 조회합니다."""
    end = today_kst()
    start = end - timedelta(days=_LOOKBACK_DAYS)
    return _retry_stock_call(
        lambda: stock.paidin_capin(start=start, end=end),
        _MAX_STOCK_API_RETRY,
        f"{code}의 차트 데이터를 10회 연속 불러오기에 실패하였습니다.",
    )


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

    delta_op_q = op_recent - op_quarter if None not in (op_recent, op_quarter) else None
    delta_op_y = op_recent - op_year if None not in (op_recent, op_year) else None
    delta_net_q = net_recent - net_quarter if None not in (net_recent, net_quarter) else None
    delta_net_y = net_recent - net_year if None not in (net_recent, net_year) else None

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
    stock: Any,
    code: str,
    recent: str,
) -> int:
    """F-Score를 계산합니다."""
    net_income = income_quarter.loc["당기순이익", recent] if "당기순이익" in income_quarter.index else 0
    operating_cash_flow = (
        cashflow_quarter.loc["영업활동으로인한현금흐름", recent]
        if "영업활동으로인한현금흐름" in cashflow_quarter.index
        else 0
    )
    events = _fetch_paidin_events(stock, code)

    return int(net_income > 0) + int(operating_cash_flow > 0) + int(len(events) == 0)


def _calculate_quant_factors_for_row(row: pd.Series, stocks: Mapping[str, Any]) -> dict[str, Any]:
    """단일 종목에 대한 모든 퀀트 팩터를 계산합니다."""
    code = row["단축코드"]
    stock = stocks[code]
    price = row["price"]
    market_cap = row["market_cap"]

    ratio_q, income_q, balance_q, cashflow_q = _load_quarter_statements(code)
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
    
    volatility, has_volatility = _compute_volatility(stock, code)
    if not has_volatility:
        return {"volatility": volatility}

    f_score = _calculate_f_score(income_q, cashflow_q, stock, code, recent)

    return {
        **valuation,
        **momentum,
        **quality,
        "volatility": volatility,
        "F_score": f_score,
        "eps": eps,
        "bps": bps,
        "sps": sps,
        "cps": cps,
    }


def get_quant_factors(df: pd.DataFrame, stocks: Mapping[str, Any]) -> pd.DataFrame:
    """DataFrame의 각 종목에 대해 퀀트 팩터를 계산하여 추가합니다."""
    df_out = df.copy()
    results = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="팩터 계산"):
        try:
            results.append(_calculate_quant_factors_for_row(row, stocks))
        except Exception as exc:
            # print(f"Error calculating factors for {row.get('단축코드')}: {exc}")
            results.append(_empty_factor_result())

    metrics = pd.DataFrame(results, index=df_out.index)
    df_out[metrics.columns] = metrics
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
    combined_mask = combined_mask.fillna(False)

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
        total_score = df_out[all_metric_rank_cols].mean(axis=1, skipna=True)
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
    """KIS stock 객체를 활용하여 현재가, 시가총액, 최근 거래대금 평균을 수집합니다."""
    df_out = df.copy()
    results = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="종목 시세 조회"):
        stock_code = row["단축코드"]
        stock = stocks.get(stock_code)

        if stock is None:
            # print(f"{stock_code} 종목 객체가 stocks 매핑에 없습니다.")
            results.append({"price": None, "market_cap": None, "amount": None})
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

        try:
            today = today_kst()
            start_date = today - timedelta(days=_QUOTE_LOOKBACK_DAYS)
            end_date = today - timedelta(days=1)
            chart = _retry_stock_call(
                lambda start=start_date, end=end_date: stock.daily_chart(
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

        results.append(
            {
                "price": price,
                "market_cap": market_cap,
                "amount": amount,
            }
        )

    metrics = pd.DataFrame(results, index=df_out.index)
    df_out[metrics.columns] = metrics
    return df_out


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
    stocks = {}

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Stock 객체 생성"):
        stock_code = row["단축코드"]

        if stock_code in stocks:
            continue

        try:
            stocks[stock_code] = _retry_stock_call(
                lambda code=stock_code: kis.stock(code),
                _MAX_STOCK_API_RETRY,
                f"{stock_code}의 stock 객체 생성에 {_MAX_STOCK_API_RETRY}회 연속 실패했습니다.",
            )
        except RuntimeError as exc:
            print(exc)

    return stocks


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
    if df_codes.empty:
        return df_codes.reindex(columns=["단축코드", "한글명"])

    if top_n <= 0:
        raise ValueError("top_n은 1 이상이어야 합니다.")

    df_with_quotes = get_stock_quote(df_codes, stocks)

    if df_with_quotes.empty:
        return df_codes.reindex(columns=["단축코드", "한글명"])

    df_with_factors = get_quant_factors(df_with_quotes, stocks).reset_index(drop=True)

    df_after_risk = apply_risk_filters(df_with_factors)
    if df_after_risk.empty:
        return df_after_risk.reindex(columns=["단축코드", "한글명"])

    df_pre_rank = apply_smallcap_filter(df_after_risk)
    if df_pre_rank.empty:
        return df_pre_rank.reindex(columns=["단축코드", "한글명"])

    df_ranked = get_rank(
        df_pre_rank,
        value_metrics=VALUE_METRICS,
        momentum_metrics=MOMENTUM_METRICS,
        quality_metrics=QUALITY_METRICS,
    )

    df_after_custom = apply_custom_selection_filters(df_ranked)

    if df_after_custom.empty:
        return df_after_custom.reindex(columns=["단축코드", "한글명"])

    df_filtered = df_after_custom.sort_values("rank_total").reset_index(drop=True)
    selection_columns = ("단축코드", "한글명")
    df_top = df_filtered.loc[:, selection_columns].head(top_n).reset_index(drop=True)

    if include_full_data:
        return df_top, df_with_factors

    return df_top


__all__ = [
    "filter_risky",
    "get_quant_factors",
    "filter_stocks",
    "get_rank",
    "get_stock_quote",
    "create_stock_objects",
    "select_stocks",
    "VALUE_METRICS",
    "MOMENTUM_METRICS",
    "QUALITY_METRICS",
    "SELECTION_FILTER_CONDITIONS",
    "apply_risk_filters",
    "apply_smallcap_filter",
    "apply_custom_selection_filters",
]
