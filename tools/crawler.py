from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Iterable, Literal, Mapping

import pandas as pd
import requests
from requests import Session
from requests.exceptions import RequestException

from .retry import retry_with_backoff

logger = logging.getLogger(__name__)

ReportType = Literal["ratio", "income", "balance", "cashflow"]
ReportPeriod = Literal["quarter", "year"]

_DEFAULT_TIMEOUT_SECONDS = 15.0
_ENC_PARAM_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"encparam\s*:\s*'([^']+)'") ,
    re.compile(r'encparam\s*:\s*"([^\"]+)"'),
    re.compile(r"['\"]encparam['\"]\s*:\s*['\"]([^'\"]+)['\"]"),
)
_PERIOD_LABEL_PATTERN = re.compile(r"(\d{4}/\d{2})")
_ESTIMATED_PERIOD_PATTERN = re.compile(r"\(E\)")
_INVALID_SYMBOL_MARKERS: tuple[str, ...] = (
    "올바른 종목이 아닙니다.",
    "menuType=block",
)
_BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}
_XHR_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}
_RATIO_METRIC_ORDER: tuple[str, ...] = (
    "EPS",
    "BPS",
    "CPS",
    "SPS",
    "PER",
    "PBR",
    "PCR",
    "PSR",
    "EV/EBITDA",
    "DPS",
    "현금배당수익률",
    "현금배당성향(%)",
)


class RetryableError(Exception):
    pass


class UnsupportedWiseReportSymbol(Exception):
    pass


@dataclass(frozen=True)
class _WiseReportRequestConfig:
    page_path: str
    api_path: str
    report_id: str


@dataclass(frozen=True)
class _WiseReportPageContext:
    page_url: str
    api_url: str
    encparam: str


_REQUEST_CONFIG: dict[ReportType, _WiseReportRequestConfig] = {
    "ratio": _WiseReportRequestConfig(
        page_path="c1040001.aspx",
        api_path="cF4002.aspx",
        report_id="5",
    ),
    "income": _WiseReportRequestConfig(
        page_path="c1030001.aspx",
        api_path="cF3002.aspx",
        report_id="0",
    ),
    "balance": _WiseReportRequestConfig(
        page_path="c1030001.aspx",
        api_path="cF3002.aspx",
        report_id="1",
    ),
    "cashflow": _WiseReportRequestConfig(
        page_path="c1030001.aspx",
        api_path="cF3002.aspx",
        report_id="2",
    ),
}


def _normalize_metric_name(metric: str) -> str:
    normalized = metric.replace(" ", "")
    normalized = re.sub(r"^\.+", "", normalized)
    normalized = normalized.replace("[구.", "[구")
    return normalized.strip()


def _extract_period_fields(raw_labels: Iterable[Any]) -> list[tuple[int, str]]:
    fields: list[tuple[int, str]] = []

    for field_index, raw_label in enumerate(raw_labels, start=1):
        text = str(raw_label)
        if "QoQ" in text or "YoY" in text:
            continue

        if _ESTIMATED_PERIOD_PATTERN.search(text):
            continue

        match = _PERIOD_LABEL_PATTERN.search(text)
        if not match:
            continue

        fields.append((field_index, match.group(1)))

    return fields


def _coerce_numeric(value: Any) -> float | None:
    if value is None or value == "":
        return None

    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None

    if pd.isna(numeric_value):
        return None

    return numeric_value


def _extract_encparam(html: str) -> str | None:
    for pattern in _ENC_PARAM_PATTERNS:
        match = pattern.search(html)
        if match is not None:
            return match.group(1)

    return None


def _should_keep_row(report_type: ReportType, row: Mapping[str, Any]) -> bool:
    metric_name = _normalize_metric_name(str(row.get("ACC_NM", "")))
    level = int(row.get("LVL", 0) or 0)

    if report_type == "ratio":
        return metric_name in _RATIO_METRIC_ORDER

    if report_type == "income":
        return level == 1

    return level in {1, 3}


def _build_dataframe_from_payload(
    report_type: ReportType,
    payload: Mapping[str, Any],
) -> pd.DataFrame:
    period_fields = _extract_period_fields(payload.get("YYMM", []))
    period_labels = [label for _, label in period_fields]
    if not period_labels:
        raise RetryableError("WiseReport payload does not contain period labels.")

    raw_rows = payload.get("DATA")
    if not isinstance(raw_rows, list):
        raise RetryableError("WiseReport payload does not contain DATA rows.")

    ordered_rows: list[tuple[str, list[float | None]]] = []
    seen_metrics: set[str] = set()

    for raw_row in raw_rows:
        if not isinstance(raw_row, Mapping):
            continue

        if not _should_keep_row(report_type, raw_row):
            continue

        metric_name = _normalize_metric_name(str(raw_row.get("ACC_NM", "")))
        if not metric_name or metric_name in seen_metrics:
            continue

        values = [
            _coerce_numeric(raw_row.get(f"DATA{field_index}"))
            for field_index, _label in period_fields
        ]

        ordered_rows.append((metric_name, values))
        seen_metrics.add(metric_name)

    if report_type == "ratio":
        row_map = {metric: values for metric, values in ordered_rows}
        empty_row: list[float | None] = [None] * len(period_labels)
        ordered_rows = [
            (metric, row_map.get(metric, empty_row.copy()))
            for metric in _RATIO_METRIC_ORDER
        ]

    if not ordered_rows:
        raise RetryableError(f"No usable rows found for report type '{report_type}'.")

    index = [metric for metric, _ in ordered_rows]
    values = [row_values for _, row_values in ordered_rows]
    df = pd.DataFrame(
        values,
        index=pd.Index(index),
        columns=pd.Index(period_labels),
    )
    df.index.name = "metric"
    return df


class FinancialCrawler:
    def __init__(
        self,
        session: Session | None = None,
        timeout: float = _DEFAULT_TIMEOUT_SECONDS,
    ):
        self.session = session or requests.Session()
        self.session.headers.update(_BASE_HEADERS)
        self.timeout = timeout
        self._page_context_cache: dict[tuple[str, str], _WiseReportPageContext] = {}

    def close(self) -> None:
        self.session.close()

    def _request_text(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        max_retries: int,
        context: str,
    ) -> str:
        def _fetch() -> str:
            response = self.session.get(
                url,
                params=params,
                headers=headers,
                timeout=self.timeout,
            )
            response.raise_for_status()

            text = response.text
            if not text.strip():
                raise RetryableError(f"Empty response from {url}")

            return text

        success, result = retry_with_backoff(
            _fetch,
            max_retries=max_retries,
            initial_delay=0.2,
            max_delay=2.0,
            backoff_factor=1.5,
            context=context,
            exceptions=(RequestException, RetryableError),
        )

        if not success or result is None:
            raise RetryableError(context or f"Request failed for {url}")

        return result

    def _load_page_context(
        self,
        code: str,
        report_type: ReportType,
        *,
        max_retries: int,
        force_refresh: bool = False,
    ) -> _WiseReportPageContext:
        config = _REQUEST_CONFIG[report_type]
        cache_key = (config.page_path, code)

        if force_refresh:
            self._page_context_cache.pop(cache_key, None)

        cached = self._page_context_cache.get(cache_key)
        if cached is not None:
            return cached

        page_url = f"https://navercomp.wisereport.co.kr/v2/company/{config.page_path}?cmp_cd={code}&cn="
        def _fetch_context() -> _WiseReportPageContext:
            html = self._request_text(
                page_url,
                max_retries=1,
                context=f"Load WiseReport page ({report_type}:{code})",
            )

            if any(marker in html for marker in _INVALID_SYMBOL_MARKERS):
                raise UnsupportedWiseReportSymbol(
                    f"WiseReport does not support symbol {code}"
                )

            encparam = _extract_encparam(html)
            if encparam is None:
                raise RetryableError(f"encparam not found for {report_type}:{code}")

            return _WiseReportPageContext(
                page_url=page_url,
                api_url=f"https://navercomp.wisereport.co.kr/v2/company/{config.api_path}",
                encparam=encparam,
            )

        success, context = retry_with_backoff(
            _fetch_context,
            max_retries=max_retries,
            initial_delay=0.2,
            max_delay=2.0,
            backoff_factor=1.5,
            context=f"Resolve WiseReport page context ({report_type}:{code})",
            exceptions=(RequestException, RetryableError),
        )
        if not success or context is None:
            raise RetryableError(f"encparam not found for {report_type}:{code}")

        self._page_context_cache[cache_key] = context
        return context

    def _fetch_payload(
        self,
        code: str,
        report_type: ReportType,
        period: ReportPeriod,
        *,
        max_retries: int,
        force_refresh: bool = False,
    ) -> Mapping[str, Any]:
        config = _REQUEST_CONFIG[report_type]
        page_context = self._load_page_context(
            code,
            report_type,
            max_retries=max_retries,
            force_refresh=force_refresh,
        )
        params = {
            "cmp_cd": code,
            "frq": "0",
            "rpt": config.report_id,
            "finGubun": "MAIN",
            "frqTyp": "0" if period == "year" else "1",
            "cn": "",
            "encparam": page_context.encparam,
        }
        headers = {
            **_XHR_HEADERS,
            "Referer": page_context.page_url,
        }

        def _fetch() -> Mapping[str, Any]:
            text = self._request_text(
                page_context.api_url,
                params=params,
                headers=headers,
                max_retries=1,
                context=f"Fetch WiseReport payload ({report_type}:{period}:{code})",
            )
            try:
                payload = json.loads(text)
            except json.JSONDecodeError as exc:
                raise RetryableError(
                    f"Invalid WiseReport JSON for {report_type}:{period}:{code}"
                ) from exc

            if not isinstance(payload, dict):
                raise RetryableError(
                    f"Unexpected WiseReport payload type for {report_type}:{period}:{code}"
                )

            return payload

        success, payload = retry_with_backoff(
            _fetch,
            max_retries=max_retries,
            initial_delay=0.2,
            max_delay=2.0,
            backoff_factor=1.5,
            context=f"Fetch WiseReport JSON ({report_type}:{period}:{code})",
            exceptions=(RetryableError,),
        )

        if not success or payload is None:
            raise RetryableError(
                f"Failed to load WiseReport JSON for {report_type}:{period}:{code}"
            )

        return payload

    def crawl(
        self,
        code: str,
        report_type: ReportType = "ratio",
        period: ReportPeriod = "quarter",
        max_retries: int = 3,
    ) -> pd.DataFrame:
        def _load_dataframe(*, force_refresh: bool) -> pd.DataFrame:
            payload = self._fetch_payload(
                code,
                report_type,
                period,
                max_retries=max_retries,
                force_refresh=force_refresh,
            )
            return _build_dataframe_from_payload(report_type, payload)

        try:
            return _load_dataframe(force_refresh=False)
        except UnsupportedWiseReportSymbol:
            raise
        except RetryableError:
            return _load_dataframe(force_refresh=True)


def crawl_financial_data(
    client_or_code: FinancialCrawler | Session | Any,
    code: str | None = None,
    report_type: ReportType = "ratio",
    period: ReportPeriod = "quarter",
) -> pd.DataFrame:
    if isinstance(client_or_code, FinancialCrawler):
        if code is None:
            raise ValueError("code is required when a crawler instance is provided.")
        return client_or_code.crawl(code, report_type=report_type, period=period)

    session = client_or_code if isinstance(client_or_code, Session) else None
    resolved_code = code if code is not None else str(client_or_code)
    crawler = FinancialCrawler(session=session)

    try:
        return crawler.crawl(resolved_code, report_type=report_type, period=period)
    finally:
        if session is None:
            crawler.close()


__all__ = [
    "FinancialCrawler",
    "RetryableError",
    "UnsupportedWiseReportSymbol",
    "crawl_financial_data",
]
