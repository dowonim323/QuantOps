from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal

from pykis.api.stock.chart import KisChartBarRepr, KisChartBase
from pykis.api.stock.daily_chart import drop_after
from pykis.api.stock.market import MARKET_TYPE
from pykis.api.stock.quote import (
    STOCK_SIGN_TYPE,
    STOCK_SIGN_TYPE_KOR_MAP,
    STOCK_SIGN_TYPE_MAP,
)
from pykis.responses.dynamic import KisDynamic, KisList, KisObject
from pykis.responses.response import KisResponse, raise_not_found
from pykis.responses.types import KisAny, KisDatetime, KisDecimal, KisInt, KisString
from pykis.utils.timezone import TIMEZONE

if TYPE_CHECKING:
    from pykis.kis import PyKis

__all__ = [
    "INDEX_CODE",
    "INDEX_NAME_MAP",
    "resolve_index_code",
    "KisDomesticIndexSummary",
    "KisDomesticIndexDailyChartBar",
    "KisDomesticIndexDailyChart",
    "domestic_index_daily_chart",
    "index_daily_chart",
    "kospi_index_daily_chart",
    "kosdaq_index_daily_chart",
]

INDEX_CODE = Literal["0001", "1001", "2001"]
"""한국투자증권 업종 지수 코드"""

INDEX_NAME_MAP: dict[INDEX_CODE, str] = {
    "0001": "코스피",
    "1001": "코스닥",
    "2001": "코스피200",
}

_INDEX_ALIAS_MAP: dict[str, INDEX_CODE] = {
    "KOSPI": "0001",
    "코스피": "0001",
    "KOSDAQ": "1001",
    "코스닥": "1001",
    "KOSPI200": "2001",
    "코스피200": "2001",
}


def resolve_index_code(index: str | INDEX_CODE) -> tuple[INDEX_CODE, str]:
    """
    업종 지수 식별자를 코드와 한글명으로 변환합니다.

    Args:
        index (str | INDEX_CODE): 업종 지수 코드 혹은 별칭 (예: 'KOSPI', '코스닥')

    Raises:
        ValueError: 지원하지 않는 지수 코드 또는 별칭인 경우
    """
    if not index:
        raise ValueError("지수 코드를 입력해주세요.")

    normalized = index.strip()

    if normalized in INDEX_NAME_MAP:
        code = normalized  # type: ignore[assignment]
        return code, INDEX_NAME_MAP[code]

    key = normalized.upper()

    if key in _INDEX_ALIAS_MAP:
        code = _INDEX_ALIAS_MAP[key]
        return code, INDEX_NAME_MAP[code]

    if normalized in _INDEX_ALIAS_MAP:
        code = _INDEX_ALIAS_MAP[normalized]
        return code, INDEX_NAME_MAP[code]

    allowed = sorted(set(INDEX_NAME_MAP.keys()) | set(_INDEX_ALIAS_MAP.keys()))
    raise ValueError(
        f"지원하지 않는 지수 코드입니다: {index}. 사용 가능한 값: {', '.join(allowed)}"
    )


def _ensure_decimal(value: Any, default: str = "0") -> Decimal:
    """빈 문자열이나 None 이 넘어올 때 안전하게 Decimal 변환"""
    if value is None:
        value = default

    text = str(value).strip()

    if not text:
        text = default

    return Decimal(text).normalize()


def _sign_or_default(value: Any) -> STOCK_SIGN_TYPE:
    """전일 대비 부호 값이 없을 때 보합으로 처리"""
    key = str(value).strip() if value is not None else ""
    return STOCK_SIGN_TYPE_MAP.get(key, "steady")


def _ensure_datetime(value: Any, format: str = "%Y%m%d") -> datetime | None:
    """빈 문자열이면 None, 그렇지 않으면 지정 포맷으로 datetime 변환"""
    if value is None:
        return None

    text = str(value).strip()

    if not text:
        return None

    return datetime.strptime(text, format).replace(tzinfo=TIMEZONE)


class KisDomesticIndexSummary(KisDynamic):
    """한국투자증권 국내 업종 지수 요약"""

    name: str = KisString["hts_kor_isnm"]
    """지수 한글명"""

    market_code: str = KisString["bstp_cls_code", ""]
    """업종 구분 코드"""

    date: datetime | None = KisAny(_ensure_datetime)["stck_bsop_date", None]
    """기준 일자"""

    price: Decimal = KisAny(_ensure_decimal)["bstp_nmix_prpr", "0"]
    """현재 지수"""

    change: Decimal = KisAny(_ensure_decimal)["bstp_nmix_prdy_vrss", "0"]
    """전일 대비"""

    change_rate: Decimal = KisAny(_ensure_decimal)["bstp_nmix_prdy_ctrt", "0"]
    """전일 대비율 (%)"""

    sign: STOCK_SIGN_TYPE = KisAny(_sign_or_default)["prdy_vrss_sign", "3"]
    """전일 대비 부호"""

    previous_price: Decimal = KisAny(_ensure_decimal)["prdy_nmix", "0"]
    """전일 지수"""

    open: Decimal = KisAny(_ensure_decimal)["bstp_nmix_oprc", "0"]
    """시가"""

    high: Decimal = KisAny(_ensure_decimal)["bstp_nmix_hgpr", "0"]
    """고가"""

    low: Decimal = KisAny(_ensure_decimal)["bstp_nmix_lwpr", "0"]
    """저가"""

    volume: int = KisAny(lambda x: int(_ensure_decimal(x)))["acml_vol", "0"]
    """누적 거래량"""

    amount: Decimal = KisAny(_ensure_decimal)["acml_tr_pbmn", "0"]
    """누적 거래대금"""

    previous_volume: int = KisAny(lambda x: int(_ensure_decimal(x)))["prdy_vol", "0"]
    """전일 거래량"""

    modified: bool = KisAny(lambda x: str(x).upper() == "Y")["mod_yn", "N"]
    """변경 여부"""

    @property
    def sign_name(self) -> str:
        """전일 대비 부호 (한글)"""
        return STOCK_SIGN_TYPE_KOR_MAP[self.sign]


class KisDomesticIndexDailyChartBar(KisChartBarRepr, KisDynamic):
    """한국투자증권 국내 업종 기간별 시세 봉"""

    time: datetime = KisDatetime("%Y%m%d", timezone=TIMEZONE)["stck_bsop_date"]
    """시간 (현지시간)"""

    time_kst: datetime = KisDatetime("%Y%m%d", timezone=TIMEZONE)["stck_bsop_date"]
    """시간 (한국시간)"""

    open: Decimal = KisAny(_ensure_decimal)["bstp_nmix_oprc", "0"]
    """시가"""

    close: Decimal = KisAny(_ensure_decimal)["bstp_nmix_prpr", "0"]
    """종가"""

    high: Decimal = KisAny(_ensure_decimal)["bstp_nmix_hgpr", "0"]
    """고가"""

    low: Decimal = KisAny(_ensure_decimal)["bstp_nmix_lwpr", "0"]
    """저가"""

    volume: int = KisAny(lambda x: int(_ensure_decimal(x)))["acml_vol", "0"]
    """누적 거래량"""

    amount: Decimal = KisAny(_ensure_decimal)["acml_tr_pbmn", "0"]
    """누적 거래대금"""

    change: Decimal = KisAny(_ensure_decimal)["bstp_nmix_prdy_vrss", "0"]
    """전일 대비"""

    change_rate: Decimal = KisAny(_ensure_decimal)["bstp_nmix_prdy_ctrt", "0"]
    """전일 대비율 (%)"""

    sign: STOCK_SIGN_TYPE = KisAny(_sign_or_default)["prdy_vrss_sign", "3"]
    """전일 대비 부호"""

    @property
    def price(self) -> Decimal:
        """현재가 (종가)"""
        return self.close

    @property
    def prev_price(self) -> Decimal:
        """전일 지수"""
        return self.close - self.change

    @property
    def rate(self) -> Decimal:
        """전일 대비율 (%)"""
        if self.change_rate != 0:
            return self.change_rate

        if self.prev_price == 0:
            return Decimal("0")

        return (self.change / self.prev_price) * 100

    @property
    def sign_name(self) -> str:
        """전일 대비 부호 (한글)"""
        return STOCK_SIGN_TYPE_KOR_MAP[self.sign]


class KisDomesticIndexDailyChart(KisResponse, KisChartBase):
    """한국투자증권 국내 업종 기간별 시세"""

    symbol: str
    """지수 코드"""

    market: MARKET_TYPE = "KRX"
    """시장 구분"""

    timezone = TIMEZONE
    """표준 시간대"""

    summary: KisDomesticIndexSummary = KisObject(KisDomesticIndexSummary)["output1"]
    """요약 정보"""

    bars: list[KisDomesticIndexDailyChartBar] = KisList(KisDomesticIndexDailyChartBar)["output2"]
    """차트 (오름차순)"""

    def __init__(self, index_code: INDEX_CODE, index_name: str):
        super().__init__()
        self.symbol = index_code
        self._name: str | None = index_name if index_name else None

    @property
    def name(self) -> str:
        """지수 한글명"""
        if self._name:
            return self._name

        return super().name

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    def __pre_init__(self, data: dict[str, object]):
        super().__pre_init__(data)

        summary_raw = data.get("output1")

        if isinstance(summary_raw, list):
            summary = summary_raw[0] if summary_raw else {}
        elif isinstance(summary_raw, dict):
            summary = summary_raw
        else:
            summary = {}

        bars_raw = data.get("output2")

        if isinstance(bars_raw, list):
            bars = bars_raw
        elif isinstance(bars_raw, dict):
            bars = [bars_raw]
        else:
            bars = []

        if not summary:
            summary = {}

        summary_defaults = {
            "stck_bsop_date": bars[0].get("stck_bsop_date", "") if bars else "",
            "bstp_nmix_prpr": "0",
            "bstp_nmix_prdy_vrss": "0",
            "bstp_nmix_prdy_ctrt": "0",
            "prdy_vrss_sign": "3",
            "prdy_nmix": "0",
            "bstp_nmix_oprc": "0",
            "bstp_nmix_hgpr": "0",
            "bstp_nmix_lwpr": "0",
            "acml_vol": "0",
            "acml_tr_pbmn": "0",
            "prdy_vol": "0",
            "hts_kor_isnm": self._name or self.symbol,
            "bstp_cls_code": "",
            "mod_yn": "N",
        }

        for key, default in summary_defaults.items():
            summary.setdefault(key, default)

        bar_defaults = {
            "stck_bsop_date": summary["stck_bsop_date"],
            "bstp_nmix_oprc": "0",
            "bstp_nmix_prpr": "0",
            "bstp_nmix_hgpr": "0",
            "bstp_nmix_lwpr": "0",
            "acml_vol": "0",
            "acml_tr_pbmn": "0",
            "bstp_nmix_prdy_vrss": "0",
            "bstp_nmix_prdy_ctrt": "0",
            "prdy_vrss_sign": "3",
        }

        for bar in bars:
            for key, default in bar_defaults.items():
                bar.setdefault(key, default)

        if summary.get("bstp_nmix_prpr") in {"", "0"} and bars:
            summary["bstp_nmix_prpr"] = bars[0].get("bstp_nmix_prpr", "0")

        if not bars:
            raise_not_found(
                data,
                "해당 지수의 차트를 조회할 수 없습니다.",
                code=self.symbol,
            )

        if not self.name:
            self.name = str(summary.get("hts_kor_isnm") or self.symbol)

        data["output1"] = summary
        data["output2"] = [item for item in bars if item]


def domestic_index_daily_chart(
    self: "PyKis",
    index: str | INDEX_CODE,
    start: date | timedelta | None = None,
    end: date | None = None,
    period: Literal["day", "week", "month", "year"] = "day",
    *,
    market_division: str = "U",
) -> KisDomesticIndexDailyChart:
    """
    국내 업종 기간별 시세(일/주/월/년)를 조회합니다.

    국내주식업종기간별시세(일/주/월/년)[v1_국내주식-021]

    Args:
        index (str | INDEX_CODE): 지수 코드 또는 별칭 (예: 'KOSPI', '코스닥', '0001')
        start (date | timedelta | None, optional): 조회 시작 일자 또는 기간. Defaults to None.
        end (date | None, optional): 조회 종료 일자. Defaults to 오늘.
        period (Literal["day", "week", "month", "year"], optional): 조회 간격. Defaults to "day".
        market_division (str, optional): 시장 구분 코드. Defaults to "U".

    Raises:
        ValueError: 잘못된 조회 파라미터인 경우
    """
    code, index_name = resolve_index_code(index)

    if not end:
        end = datetime.now(TIMEZONE).date()

    if isinstance(start, datetime):
        start = start.date()

    if isinstance(end, datetime):
        end = end.date()

    if isinstance(start, date) and end and start > end:
        start, end = end, start

    period_code_map: dict[str, tuple[str, timedelta]] = {
        "day": ("D", timedelta(days=1)),
        "week": ("W", timedelta(days=7)),
        "month": ("M", timedelta(days=30)),
        "year": ("Y", timedelta(days=365)),
    }

    if period not in period_code_map:
        raise ValueError("period는 'day', 'week', 'month', 'year' 중 하나여야 합니다.")

    period_code, period_delta = period_code_map[period]
    cursor = end
    chart: KisDomesticIndexDailyChart | None = None

    while True:
        params = {
            "FID_COND_MRKT_DIV_CODE": market_division,
            "FID_INPUT_ISCD": code,
            # FID_INPUT_DATE_1에 영업일이 아닌 날짜(휴일 등)가 전달되면 API 에러가 발생할 수 있습니다.
            # 따라서 시작일을 "00000101"로 고정하여 API 오류를 방지합니다.
            # 데이터는 FID_INPUT_DATE_2(종료일)부터 역순으로 페이징되어 조회되므로,
            # 아래 루프에서 start 날짜 도달 시 즉시 중단되어 불필요한 데이터 조회는 발생하지 않습니다.
            "FID_INPUT_DATE_1": "00000101",
            "FID_INPUT_DATE_2": cursor.strftime("%Y%m%d") if cursor else end.strftime("%Y%m%d"),
            "FID_PERIOD_DIV_CODE": period_code,
        }

        result = self.fetch(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice",
            api="FHKUP03500100",
            params=params,
            response_type=KisDomesticIndexDailyChart(index_code=code, index_name=index_name),
            domain="real",
        )

        if chart is None:
            chart = result
        else:
            chart.bars.extend(result.bars)

        if not result.bars:
            break

        last = result.bars[-1].time.date()

        if cursor and cursor <= last:
            break

        if isinstance(start, timedelta) and chart.bars:
            start = (chart.bars[0].time - start).date()

        if isinstance(start, date) and last <= start:
            break

        cursor = last - period_delta

        if isinstance(start, date) and cursor < start:
            break

    if chart is None:
        raise ValueError("지수 데이터를 조회할 수 없습니다.")

    return drop_after(
        chart,
        start=start if isinstance(start, date) else None,
        end=end,
    )


def index_daily_chart(
    self: "PyKis",
    index: str | INDEX_CODE,
    start: date | timedelta | None = None,
    end: date | None = None,
    period: Literal["day", "week", "month", "year"] = "day",
) -> KisDomesticIndexDailyChart:
    """
    국내 업종 기간별 시세(일/주/월/년)를 조회합니다.

    Args:
        index (str | INDEX_CODE): 지수 코드 또는 별칭 (예: 'KOSPI', '코스닥', '0001')
        start (date | timedelta | None, optional): 조회 시작 일자 또는 기간.
        end (date | None, optional): 조회 종료 일자.
        period (Literal["day", "week", "month", "year"], optional): 조회 간격.
    """
    return domestic_index_daily_chart(
        self,
        index=index,
        start=start,
        end=end,
        period=period,
    )


def kospi_index_daily_chart(
    self: "PyKis",
    start: date | timedelta | None = None,
    end: date | None = None,
    period: Literal["day", "week", "month", "year"] = "day",
) -> KisDomesticIndexDailyChart:
    """
    코스피 지수 기간별 시세를 조회합니다.
    """
    return domestic_index_daily_chart(
        self,
        index="KOSPI",
        start=start,
        end=end,
        period=period,
    )


def kosdaq_index_daily_chart(
    self: "PyKis",
    start: date | timedelta | None = None,
    end: date | None = None,
    period: Literal["day", "week", "month", "year"] = "day",
) -> KisDomesticIndexDailyChart:
    """
    코스닥 지수 기간별 시세를 조회합니다.
    """
    return domestic_index_daily_chart(
        self,
        index="KOSDAQ",
        start=start,
        end=end,
        period=period,
    )

