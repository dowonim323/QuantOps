from __future__ import annotations

import argparse
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence, cast
from unittest.mock import patch

import numpy as np
import pandas as pd
from pykis import KisAuth, PyKis

from tools.financial_db import FinancialDBReader
from tools.market_master import get_kospi_kosdaq_master_dataframe
from tools.quant_utils import (
    AMOUNT_SELECTION_FILTERS,
    FACTOR_COLUMNS,
    MOMENTUM_METRICS,
    PRE_REMOTE_SELECTION_FILTERS,
    QUALITY_METRICS,
    SELECTION_FILTER_CONDITIONS,
    VALUE_METRICS,
    _RANK_WINDOW_MIN_SIZE,
    _fetch_paidin_event_symbols,
    apply_custom_selection_filters,
    apply_risk_filters,
    apply_smallcap_filter,
    create_stock_objects,
    filter_stocks,
    get_average_amount,
    get_f_scores,
    get_quant_factors,
    get_rank,
    get_stock_quote,
)
from tools.time_utils import today_kst
from tools.trading_profiles import (
    get_enabled_accounts,
    get_primary_selection_account,
    get_strategy_profile,
    resolve_secret_path,
)


BASE_DIR = Path(__file__).resolve().parent.parent
LOGGER = logging.getLogger(__name__)
_CODE_COLUMNS = ["단축코드", "한글명"]


@dataclass(frozen=True)
class WindowSummary:
    index: int
    start_rank: int
    end_rank: int
    window_size: int
    amount_survivors: int
    final_survivors: int


def _parse_args() -> argparse.Namespace:
    accounts = get_enabled_accounts()
    account = get_primary_selection_account(accounts)
    strategy = get_strategy_profile(account.strategy_id)

    parser = argparse.ArgumentParser(
        description="VMQ 종목 선정 0건 원인 추적용 디버그 스크립트",
    )
    parser.add_argument(
        "--date",
        help="today_kst()를 덮어쓸 기준 일자 (YYYYMMDD)",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=strategy.selection_top_n,
        help="최종 선정 수 (기본값: VMQ 전략 설정값)",
    )
    parser.add_argument(
        "--secret-path",
        help="KIS 시크릿 파일 경로. 기본값은 VMQ 계정 설정",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="로그 레벨",
    )
    parser.add_argument(
        "--skip-quote-refresh",
        action="store_true",
        help="시세 재조회 단계를 건너뛰고 코드 마스터의 기존 시가총액으로 late-stage만 확인합니다.",
    )
    parser.add_argument(
        "--fast-volatility",
        action="store_true",
        help="변동성 계산을 고정값으로 대체해 late-stage 디버깅 속도를 높입니다.",
    )
    parser.add_argument(
        "--assume-high-amount",
        action="store_true",
        help="거래대금을 모두 통과한 것으로 간주하고 F-score/custom filter만 빠르게 확인합니다.",
    )
    parser.add_argument(
        "--skip-paidin-events",
        action="store_true",
        help="유상증자 조회를 건너뛰고 paid-in 이벤트가 없다고 가정합니다.",
    )
    parser.add_argument(
        "--skip-factor-stage",
        action="store_true",
        help="팩터/랭킹 재계산을 건너뛰고 F-score 생존 여부만 빠르게 확인합니다.",
    )
    return parser.parse_args()


def _parse_date(raw: str | None) -> date | None:
    if raw is None:
        return None

    return datetime.strptime(raw, "%Y%m%d").date()


@contextmanager
def _selection_date_context(selection_date: date | None) -> Iterator[None]:
    if selection_date is None:
        yield
        return

    with patch("tools.quant_utils.today_kst", return_value=selection_date):
        yield


def _format_change(before: int, after: int) -> str:
    return f"{before} -> {after} ({after - before:+d})"


def _log_stage(label: str, before: int | None, df: pd.DataFrame, **details: object) -> None:
    prefix = label
    if before is None:
        message = f"{prefix}: {len(df)}"
    else:
        message = f"{prefix}: {_format_change(before, len(df))}"

    if details:
        detail_text = ", ".join(f"{key}={value}" for key, value in details.items())
        message = f"{message} | {detail_text}"

    LOGGER.info(message)


def _condition_mask(df: pd.DataFrame, condition: Mapping[str, object]) -> pd.Series:
    column = condition.get("column")
    value = condition.get("value")
    mode = str(condition.get("mode", "abs"))
    direction = str(condition.get("direction", "up"))

    if column not in df.columns or value is None:
        return pd.Series(False, index=df.index)

    series = cast(pd.Series, df[str(column)])
    if mode == "abs":
        if direction == "up":
            return cast(pd.Series, series.ge(value).fillna(False))
        if direction == "down":
            return cast(pd.Series, series.le(value).fillna(False))
        return pd.Series(False, index=df.index)

    if mode == "rel":
        percentile = min(max(float(cast(float, value)), 0), 1)
        if direction == "up":
            cutoff = series.quantile(1 - percentile)
            return cast(pd.Series, series.ge(cutoff).fillna(False))
        if direction == "down":
            cutoff = series.quantile(percentile)
            return cast(pd.Series, series.le(cutoff).fillna(False))

    return pd.Series(False, index=df.index)


def _log_filter_details(
    label: str,
    df: pd.DataFrame,
    conditions: Sequence[Mapping[str, object]],
) -> None:
    if df.empty:
        LOGGER.info("%s: input empty", label)
        return

    masks: list[pd.Series] = []
    for condition in conditions:
        mask = _condition_mask(df, condition)
        masks.append(mask)
        LOGGER.info(
            "%s condition column=%s direction=%s value=%s matched=%s",
            label,
            condition.get("column"),
            condition.get("direction"),
            condition.get("value"),
            int(mask.sum()),
        )

    if masks:
        combined = cast(pd.Series, pd.concat(masks, axis=1).any(axis=1))
        LOGGER.info("%s combined matched=%s", label, int(combined.sum()))


def _log_non_null_counts(label: str, df: pd.DataFrame, columns: list[str]) -> None:
    counts = {
        column: int(df[column].notna().sum())
        for column in columns
        if column in df.columns
    }
    LOGGER.info("%s non-null counts: %s", label, counts)


def _sample_codes(df: pd.DataFrame, limit: int = 5) -> list[str]:
    if "단축코드" not in df.columns:
        return []
    return df["단축코드"].astype(str).head(limit).tolist()


def _non_null_count(df: pd.DataFrame, column: str) -> int:
    if column not in df.columns:
        return 0
    return int(cast(pd.Series, df[column]).notna().sum())


def _debug_vmq_selection(
    secret_path: Path,
    top_n: int,
    selection_date: date | None,
    skip_quote_refresh: bool,
    fast_volatility: bool,
    assume_high_amount: bool,
    skip_paidin_events: bool,
    skip_factor_stage: bool,
) -> None:
    code_dir = BASE_DIR / "codes"
    accounts = get_enabled_accounts()
    account = get_primary_selection_account(accounts)

    LOGGER.info(
        "VMQ debug start: date=%s, top_n=%s, secret=%s, account=%s",
        (selection_date or today_kst()).strftime("%Y%m%d"),
        top_n,
        secret_path.name,
        account.account_id,
    )

    kis = PyKis(KisAuth.load(secret_path), keep_token=True)
    df_codes = get_kospi_kosdaq_master_dataframe(str(code_dir))
    _log_stage("base universe", None, df_codes)

    if df_codes.empty:
        return

    stocks = create_stock_objects(df_codes, kis)

    df_candidates = apply_risk_filters(df_codes)
    _log_stage("after risk filters", len(df_codes), df_candidates)
    if df_candidates.empty:
        return

    if skip_quote_refresh:
        df_with_quotes = df_candidates.copy()
        _log_stage(
            "after quote enrichment (skipped)",
            len(df_candidates),
            df_with_quotes,
            market_cap_non_null=_non_null_count(df_with_quotes, "market_cap"),
        )
    else:
        df_with_quotes = get_stock_quote(df_candidates, stocks)
        _log_stage(
            "after quote enrichment",
            len(df_candidates),
            df_with_quotes,
            price_non_null=_non_null_count(df_with_quotes, "price"),
            market_cap_non_null=_non_null_count(df_with_quotes, "market_cap"),
        )
    if df_with_quotes.empty:
        return

    df_smallcap = apply_smallcap_filter(df_with_quotes)
    _log_stage("after smallcap filter", len(df_with_quotes), df_smallcap)
    if df_smallcap.empty:
        return

    with FinancialDBReader() as db_reader:
        db_reader.prefetch_quarter_statements(df_smallcap["단축코드"].astype(str).tolist())
        if skip_factor_stage:
            df_with_factors = df_smallcap.assign(
                asset_shrink=np.nan,
                __fscore_eligible=True,
            ).reset_index(drop=True)
            _log_stage(
                "after factor calculation (skipped)",
                len(df_smallcap),
                df_with_factors,
            )
            df_ranked = df_with_factors.assign(
                rank_total=range(1, len(df_with_factors) + 1),
            )
            _log_stage(
                "after ranking (synthetic)",
                len(df_with_factors),
                df_ranked,
                rank_total_non_null=int(df_ranked["rank_total"].notna().sum()),
            )
        else:
            volatility_map = None
            if fast_volatility:
                volatility_map = {
                    code: (0.0, True)
                    for code in df_smallcap["단축코드"].astype(str).tolist()
                }

            df_with_factors = get_quant_factors(
                df_smallcap,
                stocks,
                reader=db_reader,
                volatility_map=volatility_map,
            ).reset_index(drop=True)
            _log_stage("after factor calculation", len(df_smallcap), df_with_factors)
            _log_non_null_counts("factor coverage", df_with_factors, FACTOR_COLUMNS)

            df_ranked = get_rank(
                df_with_factors,
                value_metrics=VALUE_METRICS,
                momentum_metrics=MOMENTUM_METRICS,
                quality_metrics=QUALITY_METRICS,
            )
            _log_stage(
                "after ranking",
                len(df_with_factors),
                df_ranked,
                rank_total_non_null=int(df_ranked["rank_total"].notna().sum()),
            )

        df_base = filter_stocks(df_ranked, PRE_REMOTE_SELECTION_FILTERS)
        _log_filter_details(
            "pre-remote filters",
            df_ranked,
            list(PRE_REMOTE_SELECTION_FILTERS),
        )
        _log_stage("after pre-remote filters", len(df_ranked), df_base)
        if df_base.empty:
            return

        kis_obj = getattr(stocks, "kis", None)
        paidin_event_symbols: set[str] | None = set() if skip_paidin_events else None
        paidin_event_symbols_loaded = skip_paidin_events
        window_size = max(top_n, _RANK_WINDOW_MIN_SIZE)
        survivors: list[pd.DataFrame] = []
        summaries: list[WindowSummary] = []

        for index, start in enumerate(range(0, len(df_base), window_size), start=1):
            df_window = df_base.iloc[start : start + window_size].copy()
            if df_window.empty:
                continue

            if assume_high_amount:
                df_with_amount = df_window.assign(amount=100_000_000.0)
                df_amount_survivors = df_with_amount.copy()
            else:
                df_with_amount = get_average_amount(df_window, stocks)
                df_amount_survivors = filter_stocks(df_with_amount, AMOUNT_SELECTION_FILTERS)

            LOGGER.info(
                "window %02d ranks %d-%d size=%d amount_non_null=%d amount_survivors=%d sample=%s",
                index,
                start + 1,
                start + len(df_window),
                len(df_window),
                _non_null_count(df_with_amount, "amount"),
                len(df_amount_survivors),
                _sample_codes(df_window),
            )
            _log_filter_details(
                f"window {index:02d} amount filters",
                df_with_amount,
                list(AMOUNT_SELECTION_FILTERS),
            )

            if df_amount_survivors.empty:
                summaries.append(
                    WindowSummary(
                        index=index,
                        start_rank=start + 1,
                        end_rank=start + len(df_window),
                        window_size=len(df_window),
                        amount_survivors=0,
                        final_survivors=0,
                    )
                )
                continue

            if not paidin_event_symbols_loaded and kis_obj is not None:
                paidin_event_symbols = _fetch_paidin_event_symbols(kis_obj)
                paidin_event_symbols_loaded = True

            df_with_f_score = get_f_scores(
                df_amount_survivors,
                stocks,
                reader=db_reader,
                paidin_event_symbols=paidin_event_symbols,
            )
            df_filtered = apply_custom_selection_filters(df_with_f_score)
            _log_filter_details(
                f"window {index:02d} custom filters",
                df_with_f_score,
                list(SELECTION_FILTER_CONDITIONS),
            )
            LOGGER.info(
                "window %02d fscore_non_null=%d final_survivors=%d final_sample=%s",
                index,
                _non_null_count(df_with_f_score, "F_score"),
                len(df_filtered),
                _sample_codes(df_filtered),
            )

            summaries.append(
                WindowSummary(
                    index=index,
                    start_rank=start + 1,
                    end_rank=start + len(df_window),
                    window_size=len(df_window),
                    amount_survivors=len(df_amount_survivors),
                    final_survivors=len(df_filtered),
                )
            )

            if not df_filtered.empty:
                survivors.append(df_filtered)

        windows_with_amount = sum(summary.amount_survivors > 0 for summary in summaries)
        windows_with_final = sum(summary.final_survivors > 0 for summary in summaries)
        LOGGER.info(
            "window summary: total=%d, amount_survivor_windows=%d, final_survivor_windows=%d",
            len(summaries),
            windows_with_amount,
            windows_with_final,
        )

        if not survivors:
            LOGGER.info(
                "final result: 0 survivors after late-stage evaluation; last non-zero stage was after pre-remote filters (%d rows)",
                len(df_base),
            )
            return

        df_final = pd.concat(survivors, ignore_index=True)
        helper_columns = [
            column for column in df_final.columns if column.startswith("__")
        ]
        df_final = df_final.drop(columns=helper_columns, errors="ignore")
        df_final = df_final.sort_values("rank_total").reset_index(drop=True)
        df_top = df_final.loc[:, _CODE_COLUMNS].head(top_n).reset_index(drop=True)

        LOGGER.info(
            "final result: survivors=%d, selected=%d, selected_codes=%s",
            len(df_final),
            len(df_top),
            _sample_codes(df_top, limit=top_n),
        )


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    selection_date = _parse_date(args.date)
    accounts = get_enabled_accounts()
    account = get_primary_selection_account(accounts)
    secret_path = (
        Path(args.secret_path)
        if args.secret_path
        else resolve_secret_path(BASE_DIR, account)
    )

    with _selection_date_context(selection_date):
        _debug_vmq_selection(
            secret_path,
            args.top_n,
            selection_date,
            args.skip_quote_refresh,
            args.fast_volatility,
            args.assume_high_amount,
            args.skip_paidin_events,
            args.skip_factor_stage,
        )


if __name__ == "__main__":
    main()
