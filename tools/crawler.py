from __future__ import annotations

import time
from io import StringIO
from typing import Literal

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class RetryableError(Exception):
    """재시도 가능한 예외."""


def get_driver() -> webdriver.Chrome:
    """헤드리스 Chrome WebDriver를 생성하여 반환합니다."""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=options)


class FinancialCrawler:
    """네이버 금융 재무 데이터 크롤러."""

    def __init__(self, driver: webdriver.Chrome):
        self.driver = driver

    def _wait_and_click(self, selector: str, timeout: int = 5) -> None:
        """요소가 클릭 가능해질 때까지 기다린 후 클릭합니다."""
        WebDriverWait(self.driver, timeout).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
        ).click()

    def _wait_for_table(self, timeout: int = 10) -> None:
        """테이블 요소가 로드될 때까지 기다립니다."""
        WebDriverWait(self.driver, timeout).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, "table"))
        )

    def _parse_html_table(self, html: str, period: str) -> pd.DataFrame | None:
        """HTML에서 테이블을 파싱하여 DataFrame으로 반환합니다."""
        if not html:
            raise RetryableError("Empty HTML")

        tables = pd.read_html(StringIO(html))
        if not tables:
            raise RetryableError("No tables found")

        df = tables[-1]

        # 테이블 형태 검증
        if (df.shape[1] == 7 and period == "quarter") or (
            df.shape[1] == 9 and period == "year"
        ):
            return df

        return None

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame의 컬럼명과 인덱스를 정리합니다."""
        df = df.copy()
        df["항목"] = df["항목"].str.replace("펼치기", "", regex=False).str.strip()
        
        # 불필요한 접미사 제거
        for suffix in [
            "(IFRS연결)",
            "(IFRS별도)",
            "(GAAP연결)",
            "(GAAP개별)",
            "연간컨센서스보기",
        ]:
            df.columns = [col.replace(suffix, "").strip() for col in df.columns]

        # 예측치, QoQ, YoY 등 제외
        df = df[
            [
                col
                for col in df.columns
                if all(x not in col for x in ["(E)", "(QoQ)", "(YoY)"])
            ]
        ]
        
        # Unnamed 컬럼 제외
        df = df[[col for col in df.columns if "Unnamed" not in col]]
        
        # 공백 제거
        df.columns = [col.replace(" ", "") for col in df.columns]
        
        df.set_index("항목", inplace=True)
        df.index.name = None
        return df

    def crawl(
        self,
        code: str,
        report_type: Literal["ratio", "income", "balance", "cashflow"] = "ratio",
        period: Literal["quarter", "year"] = "quarter",
        max_reload: int = 30,
    ) -> pd.DataFrame:
        """
        재무 데이터를 크롤링합니다.

        Args:
            code: 종목 코드.
            report_type: 리포트 종류 ("ratio", "income", "balance", "cashflow").
            period: 기간 ("quarter", "year").

        Returns:
            크롤링된 데이터가 담긴 DataFrame.
        """
        if report_type == "ratio":
            url = f"https://navercomp.wisereport.co.kr/v2/company/c1040001.aspx?cmp_cd={code}&cn="
            tab_selector = None
            freq_sel = {"quarter": "#frqTyp1_2", "year": "#frqTyp0_2"}
            fin_sel = "#hfinGubun2"
        else:
            url = f"https://navercomp.wisereport.co.kr/v2/company/c1030001.aspx?cmp_cd={code}&cn="
            tab_map = {
                "income": "#rpt_tab1",
                "balance": "#rpt_tab2",
                "cashflow": "#rpt_tab3",
            }
            tab_selector = tab_map.get(report_type)
            freq_sel = {"quarter": "#frqTyp1", "year": "#frqTyp0"}
            fin_sel = "#hfinGubun"

        self.driver.set_page_load_timeout(5)
        if self.driver.current_url != url:
            self.driver.get(url)

        if tab_selector:
            self._wait_and_click(tab_selector)

        self._wait_and_click(freq_sel[period])
        self._wait_and_click(fin_sel)
        self._wait_for_table()

        # 테이블 로딩 재시도 루프
        df: pd.DataFrame | None = None
        last_exception: Exception | None = None
        reload_count = 0
        
        while reload_count < max_reload:
            try:
                html = self.driver.page_source
                df = self._parse_html_table(html, period)
                if df is not None:
                    break
                
                raise RetryableError("Table shape mismatch")
            except Exception as exc:
                last_exception = exc
                df = None
                reload_count += 1
                time.sleep(0.1)

        if df is None:
            if last_exception is None:
                last_exception = RetryableError("Failed to retrieve data.")
            raise last_exception

        return self._clean_dataframe(df)


def crawl_financial_data(
    driver: webdriver.Chrome,
    code: str,
    report_type: Literal["ratio", "income", "balance", "cashflow"] = "ratio",
    period: Literal["quarter", "year"] = "quarter",
) -> pd.DataFrame:
    """
    하위 호환성을 위한 래퍼 함수.
    
    Note: 이 함수는 드라이버 재시작 로직을 포함하지 않습니다. 
    드라이버 상태 관리는 호출자가 담당해야 합니다.
    """
    crawler = FinancialCrawler(driver)
    return crawler.crawl(code, report_type, period)
