from __future__ import annotations

import json
import logging
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_KRX_URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
_KRX_LOGIN_PAGE = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd"
_KRX_LOGIN_JSP = "https://data.krx.co.kr/contents/MDC/COMS/client/view/login.jsp?site=mdc"
_KRX_LOGIN_URL = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
_KRX_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
}
_KRX_MARKET_MAP = {"KOSPI": "STK", "KOSDAQ": "KSQ"}
_DEFAULT_KRX_CREDENTIALS_PATH = Path(__file__).resolve().parent.parent / "secrets" / "krx_marketplace.json"


def _load_krx_credentials(credentials_path: Path | None = None) -> tuple[str | None, str | None]:
    candidate_path = credentials_path or Path(os.getenv("KRX_LOGIN_FILE", _DEFAULT_KRX_CREDENTIALS_PATH))
    if not candidate_path.exists():
        return None, None

    try:
        payload = json.loads(candidate_path.read_text(encoding="utf-8"))
    except Exception:
        return None, None

    login_id = payload.get("login_id")
    login_pw = payload.get("login_pw")
    return (
        str(login_id) if login_id else None,
        str(login_pw) if login_pw else None,
    )


class KrxOHLCVReader:
    def __init__(
        self,
        session: requests.Session | None = None,
        *,
        login_id: str | None = None,
        login_pw: str | None = None,
        credentials_path: Path | None = None,
    ) -> None:
        self._session = session or requests.Session()
        self._owns_session = session is None
        self._session.headers.update(_KRX_HEADERS)
        self._snapshot_cache: dict[str, pd.DataFrame] = {}
        self._available: bool | None = None
        file_login_id, file_login_pw = _load_krx_credentials(credentials_path)
        self._login_id = login_id or os.getenv("KRX_LOGIN_ID") or file_login_id
        self._login_pw = login_pw or os.getenv("KRX_LOGIN_PW") or file_login_pw
        self._auth_attempted = False
        self._authenticated = False

    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def __enter__(self) -> KrxOHLCVReader:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def login_krx(self) -> bool:
        if not self._login_id or not self._login_pw:
            return False

        headers = {"User-Agent": _KRX_HEADERS["User-Agent"]}
        self._session.get(_KRX_LOGIN_PAGE, headers=headers, timeout=15)
        self._session.get(
            _KRX_LOGIN_JSP,
            headers={
                "User-Agent": _KRX_HEADERS["User-Agent"],
                "Referer": _KRX_LOGIN_PAGE,
            },
            timeout=15,
        )

        payload = {
            "mbrNm": "",
            "telNo": "",
            "di": "",
            "certType": "",
            "mbrId": self._login_id,
            "pw": self._login_pw,
        }
        login_headers = {
            "User-Agent": _KRX_HEADERS["User-Agent"],
            "Referer": _KRX_LOGIN_PAGE,
        }

        response = self._session.post(
            _KRX_LOGIN_URL,
            data=payload,
            headers=login_headers,
            timeout=15,
        )
        data = response.json()
        error_code = data.get("_error_code", "")

        if error_code == "CD011":
            payload["skipDup"] = "Y"
            response = self._session.post(
                _KRX_LOGIN_URL,
                data=payload,
                headers=login_headers,
                timeout=15,
            )
            data = response.json()
            error_code = data.get("_error_code", "")

        return error_code == "CD001"

    def _ensure_authenticated(self) -> None:
        if self._auth_attempted:
            return

        self._auth_attempted = True
        if not self._login_id or not self._login_pw:
            return

        try:
            self._authenticated = self.login_krx()
        except Exception:
            self._authenticated = False

        if not self._authenticated:
            logger.warning("KRX login failed; continuing with unauthenticated KRX requests.")

    def _fetch_market_snapshot(self, trading_day: str, market: str) -> pd.DataFrame:
        self._ensure_authenticated()
        response = self._session.post(
            _KRX_URL,
            data={
                "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
                "trdDd": trading_day,
                "mktId": _KRX_MARKET_MAP[market],
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("OutBlock_1") or payload.get("output") or []
        if not rows:
            return pd.DataFrame(columns=pd.Index(["종가", "거래대금"]))

        df = pd.DataFrame(rows)
        df = df[["ISU_SRT_CD", "TDD_CLSPRC", "ACC_TRDVAL"]]
        df.columns = ["단축코드", "종가", "거래대금"]
        df = df.replace(r"[^-\.\w]", "", regex=True).replace("", "0")
        df["종가"] = pd.to_numeric(df["종가"], errors="coerce")
        df["거래대금"] = pd.to_numeric(df["거래대금"], errors="coerce")
        return df.set_index("단축코드")

    def load_snapshot(self, trading_day: str) -> pd.DataFrame:
        cached = self._snapshot_cache.get(trading_day)
        if cached is not None:
            return cached.copy()

        if self._available is False:
            return pd.DataFrame(columns=pd.Index(["종가", "거래대금"]))

        try:
            frames = [self._fetch_market_snapshot(trading_day, market) for market in ("KOSPI", "KOSDAQ")]
            snapshot = pd.concat(frames, axis=0)
            self._available = True
        except Exception:
            self._available = False
            logger.warning("KRX OHLCV fetch unavailable for %s; falling back to KIS chart path.", trading_day)
            snapshot = pd.DataFrame(columns=pd.Index(["종가", "거래대금"]))

        self._snapshot_cache[trading_day] = snapshot
        return snapshot.copy()

    def _business_days(self, end_day: date, lookback_days: int) -> list[str]:
        start_day = end_day - timedelta(days=lookback_days)
        days = pd.date_range(start=start_day, end=end_day, freq="B")
        return [day.strftime("%Y%m%d") for day in days]

    def compute_amounts(self, codes: Iterable[str], *, end_day: date, lookback_days: int) -> dict[str, int | None]:
        code_list = list(dict.fromkeys(str(code) for code in codes))
        if not code_list:
            return {}

        snapshots = []
        for trading_day in self._business_days(end_day, lookback_days):
            snapshot = self.load_snapshot(trading_day)
            if snapshot.empty:
                continue
            snapshot = snapshot.reindex(code_list)
            snapshot["date"] = trading_day
            snapshots.append(snapshot)

        if not snapshots:
            return {}

        merged = pd.concat(snapshots).reset_index().rename(columns={"index": "단축코드"})
        results: dict[str, int | None] = {}
        for code in code_list:
            series = merged.loc[merged["단축코드"] == code, "거래대금"].dropna()
            results[code] = int(series.mean()) if not series.empty else None
        return results

    def compute_weekly_volatility(self, codes: Iterable[str], *, end_day: date, lookback_days: int) -> dict[str, tuple[float, bool]]:
        code_list = list(dict.fromkeys(str(code) for code in codes))
        if not code_list:
            return {}

        closes: dict[str, list[tuple[pd.Timestamp, float]]] = {code: [] for code in code_list}
        snapshot_found = False
        for trading_day in self._business_days(end_day, lookback_days):
            snapshot = self.load_snapshot(trading_day)
            if snapshot.empty:
                continue
            snapshot_found = True
            dt = pd.to_datetime(trading_day, format="%Y%m%d")
            for code in code_list:
                if code in snapshot.index:
                    close = snapshot.at[code, "종가"]
                    if pd.notna(close):
                        closes[code].append((dt, float(close)))

        if not snapshot_found:
            return {}

        results: dict[str, tuple[float, bool]] = {}
        for code in code_list:
            values = closes[code]
            if len(values) < 2:
                results[code] = (float("nan"), False)
                continue

            series = pd.Series(
                [close for _, close in values],
                index=pd.DatetimeIndex([dt for dt, _ in values]),
            ).sort_index()
            weekly = series.resample("W-FRI").last().dropna()
            returns = weekly.pct_change().dropna()
            if returns.empty:
                results[code] = (float("nan"), False)
                continue

            results[code] = (float(-returns.std()), True)

        return results


__all__ = ["KrxOHLCVReader"]
