from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable, Literal, TypeAlias, cast

import pandas as pd
from tqdm import tqdm

from tools.crawler import (
    FinancialCrawler,
    UnsupportedWiseReportSymbol,
)
from tools.financial_db import (
    backup_databases,
    FinancialDBBatchWriter,
)
from tools.market_master import download_code_master, get_kospi_kosdaq_master_dataframe
from tools.notifications import send_notification

BASE_DIR = Path(__file__).resolve().parent.parent

ReportType: TypeAlias = Literal["ratio", "income", "balance", "cashflow"]
ReportPeriod: TypeAlias = Literal["year", "quarter"]
ReportTask = tuple[ReportType, ReportPeriod]

REPORT_TASKS: tuple[ReportTask, ...] = (
    ("ratio", "year"),
    ("ratio", "quarter"),
    ("income", "year"),
    ("income", "quarter"),
    ("balance", "year"),
    ("balance", "quarter"),
    ("cashflow", "year"),
    ("cashflow", "quarter"),
)

DEFAULT_NOTIFICATION_CHANNEL = "financial_crawling"


def _resolve_max_workers(max_workers: int | None) -> int:
    """최대 워커 수를 결정합니다."""
    if max_workers is not None and max_workers > 0:
        return max(1, min(max_workers, 16))

    logical_cores = os.cpu_count() or 1
    return max(1, min(logical_cores, 16))


def _format_elapsed(seconds: float) -> str:
    """경과 시간을 포맷팅합니다."""
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms"

    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds - (hours * 3600) - (minutes * 60)

    parts: list[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes or hours:
        parts.append(f"{minutes}m")
    parts.append(f"{secs:.1f}s")

    return " ".join(parts)


def _short_exception_message(exc: Exception) -> str:
    """예외 메시지를 간략하게 반환합니다."""
    text = str(exc).strip()
    if not text:
        return exc.__class__.__name__
    first_line = text.splitlines()[0].strip()
    return first_line or exc.__class__.__name__


def prepare_code_metadata(code_dir: Path) -> tuple[pd.Series, pd.Series]:
    """종목 코드 메타데이터를 준비합니다."""
    download_code_master(str(code_dir), "kospi")
    download_code_master(str(code_dir), "kosdaq")
    df_codes = get_kospi_kosdaq_master_dataframe(str(code_dir)).copy()

    df_codes["단축코드"] = df_codes["단축코드"].astype(str)
    df_codes["한글명"] = df_codes["한글명"].astype(str)

    codes = cast(pd.Series, df_codes["단축코드"])
    names = cast(pd.Series, df_codes.set_index("단축코드")["한글명"])
    return codes, names


def fetch_reports(
    code: str,
    tasks: Iterable[ReportTask],
    max_retry: int = 10,
) -> tuple[list[tuple[str, str, Any]], list[str]]:
    results: list[tuple[str, str, Any]] = []
    failures: list[str] = []

    crawler = FinancialCrawler()

    try:
        for report_type, period in tasks:
            try:
                data = crawler.crawl(
                    code,
                    report_type,
                    period=period,
                    max_retries=max_retry,
                )
                results.append((report_type, period, data))
            except UnsupportedWiseReportSymbol as exc:
                failures.append(f"skipped ({_short_exception_message(exc)})")
                break
            except Exception as exc:
                short_exc = _short_exception_message(exc)
                failures.append(f"{report_type}:{period} - exception occurred ({short_exc})")
    finally:
        crawler.close()

    return results, failures


def _process_single_code(
    code: str,
    _company_name: str,
    tasks: Iterable[ReportTask],
    max_retry: int = 10,
) -> tuple[list[tuple[str, str, Any]], list[str]]:
    """단일 종목 처리를 위한 작업 함수 (워커에서 실행)."""
    return fetch_reports(code, tasks, max_retry=max_retry)


def _run_parallel(
    codes: Iterable[str],
    names: pd.Series,
    tasks: Iterable[ReportTask],
    resolved_workers: int,
) -> tuple[set[str], list[str], list[tuple[str, str, str, str, Any]]]:
    """병렬로 크롤링 작업을 수행합니다."""
    successful_codes: set[str] = set()
    failures: list[str] = []
    collected_reports: list[tuple[str, str, str, str, Any]] = []

    with ThreadPoolExecutor(max_workers=resolved_workers) as executor:
        future_to_meta = {
            executor.submit(
                _process_single_code,
                code,
                str(names.get(code, "")),
                tasks,
                max_retry=10,
            ): (code, str(names.get(code, "")))
            for code in codes
        }

        total_codes = len(future_to_meta)

        with tqdm(total=total_codes, desc="Crawling", unit="symbols") as progress:
            for future in as_completed(future_to_meta):
                code, company_name = future_to_meta[future]
                try:
                    results, task_failures = future.result()
                except Exception as exc:
                    short_exc = _short_exception_message(exc)
                    failures.append(
                        f"{code} ({company_name}) - error during processing: {short_exc}"
                    )
                else:
                    if task_failures:
                        joined = ", ".join(task_failures)
                        failures.append(f"{code} ({company_name}) - {joined}")
                    else:
                        successful_codes.add(code)
                        for report_type, period, data in results:
                            collected_reports.append(
                                (report_type, period, code, company_name, data)
                            )

                progress.update(1)

    return successful_codes, failures, collected_reports


def _apply_updates(
    collected_reports: Iterable[tuple[str, str, str, str, Any]],
    successful_codes: set[str],
) -> list[str]:
    """수집된 데이터를 DB에 업데이트합니다."""
    collected_list = list(collected_reports)
    failures: list[str] = []
    failed_codes: set[str] = set()
    reports_by_code: dict[str, list[tuple[str, str, str, str, Any]]] = {}

    for report in collected_list:
        reports_by_code.setdefault(report[2], []).append(report)

    with FinancialDBBatchWriter() as batch_writer:
        with tqdm(total=len(collected_list), desc="DB Update", unit="tasks") as progress:
            for code_reports in reports_by_code.values():
                code, company_name = code_reports[0][2], code_reports[0][3]

                try:
                    batch_writer.write_symbol_reports(
                        [
                            (report_type, period, code, data)
                            for report_type, period, _code, _company_name, data in code_reports
                        ],
                        drop_missing_metrics=True,
                    )
                except Exception as exc:
                    short_exc = _short_exception_message(exc)
                    failure_message = (
                        f"{code} ({company_name}) - update failed: {short_exc}"
                    )
                    failures.append(failure_message)
                    failed_codes.add(code)
                finally:
                    progress.update(len(code_reports))

    for code in failed_codes:
        successful_codes.discard(code)

    return failures


def main(
    channel: str = DEFAULT_NOTIFICATION_CHANNEL,
    max_workers: int | None = None,
) -> None:
    """메인 실행 함수."""
    code_dir = BASE_DIR / "codes"

    resolved_workers = _resolve_max_workers(max_workers)

    backup_dir = backup_databases()
    codes, names = prepare_code_metadata(code_dir)

    total = len(codes)
    start_time = time.perf_counter()
    message_lines = [
        "Financial data crawl started.",
    ]
    if backup_dir is not None:
        message_lines.append(f"Database backup created at {backup_dir.name}")
    message_lines.append(
        f"Total symbols: {total}, worker threads: {resolved_workers}"
    )
    send_notification(
        channel,
        "\n".join(message_lines),
        title="Financial Crawl Started",
        markdown=True,
        tags=("chart_with_upwards_trend",),
    )

    successful_codes, crawl_failures, collected_reports = _run_parallel(
        codes,
        names,
        REPORT_TASKS,
        resolved_workers,
    )

    update_failures = _apply_updates(collected_reports, successful_codes)
    failures = [*crawl_failures, *update_failures]
    success_count = len(successful_codes)

    elapsed_seconds = time.perf_counter() - start_time

    summary_lines = [
        "Financial data crawl finished.",
        f"Total symbols: {total}",
        f"Success: {success_count}",
        f"Failures: {len(failures)}",
        f"Worker threads: {resolved_workers}",
        f"Elapsed time: {_format_elapsed(elapsed_seconds)}",
    ]

    if failures:
        summary_lines.append("Failure details:")
        summary_lines.extend(f"- {failure}" for failure in failures)

    send_notification(
        channel,
        "\n".join(summary_lines),
        title="Financial Crawl Finished",
        markdown=True,
        tags=("white_check_mark",) if not failures else ("warning",),
        priority="high" if failures else None,
    )


if __name__ == "__main__":
    main()
