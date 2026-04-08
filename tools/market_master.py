from __future__ import annotations

import os
import ssl
import urllib.request
import zipfile
from typing import Literal, cast

import pandas as pd


def download_code_master(
    base_dir: str,
    market: Literal["kospi", "kosdaq"] = "kospi",
    verbose: bool = False,
) -> None:
    """한국거래소에서 KOSPI/KOSDAQ 종목코드 마스터 파일을 다운로드합니다.

    Args:
        base_dir: 파일을 저장할 디렉토리 경로.
        market: 'kospi' 또는 'kosdaq'.
        verbose: 진행 상황 출력 여부.
    """
    ssl._create_default_https_context = ssl._create_unverified_context

    market_lower = market.lower()
    if market_lower == "kospi":
        file_name = "kospi_code"
    elif market_lower == "kosdaq":
        file_name = "kosdaq_code"
    else:
        raise ValueError("market은 'kospi' 또는 'kosdaq'이어야 합니다.")

    download_url = f"https://new.real.download.dws.co.kr/common/master/{file_name}.mst.zip"
    zip_path = os.path.join(base_dir, f"{file_name}.zip")

    if verbose:
        print(f"다운로드 중: {download_url}")

    urllib.request.urlretrieve(download_url, zip_path)

    if verbose:
        print(f"압축 해제 중: {base_dir}")

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(base_dir)

    if os.path.exists(zip_path):
        os.remove(zip_path)

    if verbose:
        print("✅ 다운로드 완료")


def get_sector_master_dataframe(
    base_dir: str = ".",
    *,
    keep_files: bool = False,
    verbose: bool = False,
) -> pd.DataFrame:
    """업종(지수) 코드 마스터 파일을 내려받아 DataFrame으로 반환합니다.

    Args:
        base_dir: 파일을 저장할 디렉토리 경로.
        keep_files: 다운로드한 파일 보존 여부.
        verbose: 진행 상황 출력 여부.

    Returns:
        업종코드와 업종명이 포함된 DataFrame.
    """
    ssl._create_default_https_context = ssl._create_unverified_context

    base_dir = os.path.abspath(base_dir)
    os.makedirs(base_dir, exist_ok=True)

    download_url = "https://new.real.download.dws.co.kr/common/master/idxcode.mst.zip"
    zip_path = os.path.join(base_dir, "idxcode.zip")
    mst_path = os.path.join(base_dir, "idxcode.mst")

    if verbose:
        print(f"다운로드 중: {download_url}")

    urllib.request.urlretrieve(download_url, zip_path)

    if verbose:
        print(f"압축 해제 중: {base_dir}")

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(base_dir)

    rows = []

    with open(mst_path, mode="r", encoding="cp949") as f:
        for line in f:
            code = line[1:5].strip()
            name = line[3:43].rstrip()
            if code:
                rows.append((code, name))

    df = pd.DataFrame(rows, columns=pd.Index(["업종코드", "업종명"]))

    if not keep_files:
        if os.path.exists(zip_path):
            os.remove(zip_path)
        if os.path.exists(mst_path):
            os.remove(mst_path)

    if verbose:
        print(f"✅ 업종 코드 {len(df)}건 로드 완료")

    return df


def _parse_master_file(
    base_dir: str,
    file_name_prefix: str,
    part1_columns: list[str],
    part2_columns: list[str],
    field_specs: list[int],
    row_len_offset: int,
) -> pd.DataFrame:
    """마스터 파일을 파싱하여 DataFrame으로 반환하는 공통 함수."""
    file_name = os.path.join(base_dir, f"{file_name_prefix}.mst")
    tmp_fil1 = os.path.join(base_dir, f"{file_name_prefix}_part1.tmp")
    tmp_fil2 = os.path.join(base_dir, f"{file_name_prefix}_part2.tmp")

    with open(file_name, mode="r", encoding="cp949") as f:
        with open(tmp_fil1, mode="w", encoding="utf-8") as wf1, open(
            tmp_fil2, mode="w", encoding="utf-8"
        ) as wf2:
            for row in f:
                # Part 1 처리
                rf1 = row[0 : len(row) - row_len_offset]
                rf1_1 = rf1[0:9].rstrip()
                rf1_2 = rf1[9:21].rstrip()
                rf1_3 = rf1[21:].strip()
                wf1.write(f"{rf1_1},{rf1_2},{rf1_3}\n")

                # Part 2 처리
                rf2 = row[-row_len_offset:]
                wf2.write(rf2)

    df1 = pd.read_csv(
        tmp_fil1, header=None, names=part1_columns, encoding="utf-8"
    )

    df2 = pd.read_fwf(
        tmp_fil2, widths=field_specs, names=part2_columns
    )

    df_merged = pd.merge(df1, df2, how="outer", left_index=True, right_index=True)

    os.remove(tmp_fil1)
    os.remove(tmp_fil2)

    return df_merged


def _read_kospi_master(base_dir: str) -> pd.DataFrame:
    """KOSPI 마스터 파일을 읽어 DataFrame으로 반환합니다."""
    part1_columns = ["단축코드", "표준코드", "한글명"]
    part2_columns = [
        '그룹코드', '시가총액규모', '지수업종대분류', '지수업종중분류', '지수업종소분류',
        '제조업', '저유동성', '지배구조지수종목', 'KOSPI200섹터업종', 'KOSPI100',
        'KOSPI50', 'KRX', 'ETP', 'ELW발행', 'KRX100',
        'KRX자동차', 'KRX반도체', 'KRX바이오', 'KRX은행', 'SPAC',
        'KRX에너지화학', 'KRX철강', '단기과열', 'KRX미디어통신', 'KRX건설',
        'Non1', 'KRX증권', 'KRX선박', 'KRX섹터_보험', 'KRX섹터_운송',
        'SRI', '기준가', '매매수량단위', '시간외수량단위', '거래정지',
        '정리매매', '관리종목', '시장경고', '경고예고', '불성실공시',
        '우회상장', '락구분', '액면변경', '증자구분', '증거금비율',
        '신용가능', '신용기간', '전일거래량', '액면가', '상장일자',
        '상장주수', '자본금', '결산월', '공모가', '우선주',
        '공매도과열', '이상급등', 'KRX300', 'KOSPI', '매출액',
        '영업이익', '경상이익', '당기순이익', 'ROE', '기준년월',
        '시가총액', '그룹사코드', '회사신용한도초과', '담보대출가능', '대주가능'
    ]
    field_specs = [
        2, 1, 4, 4, 4,
        1, 1, 1, 1, 1,
        1, 1, 1, 1, 1,
        1, 1, 1, 1, 1,
        1, 1, 1, 1, 1,
        1, 1, 1, 1, 1,
        1, 9, 5, 5, 1,
        1, 1, 2, 1, 1,
        1, 2, 2, 2, 3,
        1, 3, 12, 12, 8,
        15, 21, 2, 7, 1,
        1, 1, 1, 1, 9,
        9, 9, 5, 9, 8,
        9, 3, 1, 1, 1
    ]

    df = _parse_master_file(
        base_dir,
        "kospi_code",
        part1_columns,
        part2_columns,
        field_specs,
        row_len_offset=228
    )

    df = cast(
        pd.DataFrame,
        df.loc[
            (df["그룹코드"] == "ST")
            & (df["SPAC"] == "N")
            & (df["기준년월"].notna()),
            ["단축코드", "한글명", "거래정지", "정리매매", "관리종목", "시장경고", "경고예고", "상장주수", "시가총액"],
        ].copy(),
    )

    df["상장주수"] = cast(pd.Series, pd.to_numeric(df["상장주수"], errors="coerce")).mul(1000)
    df["market_cap"] = pd.to_numeric(df["시가총액"], errors="coerce")
    df = df.drop(columns=["시가총액"])

    df["시장구분"] = "KOSPI"
    return df


def _read_kosdaq_master(base_dir: str) -> pd.DataFrame:
    """KOSDAQ 마스터 파일을 읽어 DataFrame으로 반환합니다."""
    part1_columns = ["단축코드", "표준코드", "한글명"]
    part2_columns = [
        '증권그룹구분코드', '시가총액 규모 구분 코드 유가',
        '지수업종 대분류 코드', '지수 업종 중분류 코드', '지수업종 소분류 코드', '벤처기업 여부 (Y/N)',
        '저유동성종목 여부', 'KRX 종목 여부', 'ETP 상품구분코드', 'KRX100 종목 여부 (Y/N)',
        'KRX 자동차 여부', 'KRX 반도체 여부', 'KRX 바이오 여부', 'KRX 은행 여부', '기업인수목적회사여부',
        'KRX 에너지 화학 여부', 'KRX 철강 여부', '단기과열종목구분코드', 'KRX 미디어 통신 여부',
        'KRX 건설 여부', '(코스닥)투자주의환기종목여부', 'KRX 증권 구분', 'KRX 선박 구분',
        'KRX섹터지수 보험여부', 'KRX섹터지수 운송여부', 'KOSDAQ150지수여부 (Y,N)', '주식 기준가',
        '정규 시장 매매 수량 단위', '시간외 시장 매매 수량 단위', '거래정지 여부', '정리매매 여부',
        '관리 종목 여부', '시장 경고 구분 코드', '시장 경고위험 예고 여부', '불성실 공시 여부',
        '우회 상장 여부', '락구분 코드', '액면가 변경 구분 코드', '증자 구분 코드', '증거금 비율',
        '신용주문 가능 여부', '신용기간', '전일 거래량', '주식 액면가', '주식 상장 일자', '상장 주수(천)',
        '자본금', '결산 월', '공모 가격', '우선주 구분 코드', '공매도과열종목여부', '이상급등종목여부',
        'KRX300 종목 여부 (Y/N)', '매출액', '영업이익', '경상이익', '단기순이익', 'ROE(자기자본이익률)',
        '기준년월', '전일기준 시가총액 (억)', '그룹사 코드', '회사신용한도초과여부', '담보대출가능여부', '대주가능여부'
    ]
    field_specs = [
        2, 1,
        4, 4, 4, 1, 1,
        1, 1, 1, 1, 1,
        1, 1, 1, 1, 1,
        1, 1, 1, 1, 1,
        1, 1, 1, 1, 9,
        5, 5, 1, 1, 1,
        2, 1, 1, 1, 2,
        2, 2, 3, 1, 3,
        12, 12, 8, 15, 21,
        2, 7, 1, 1, 1,
        1, 9, 9, 9, 5,
        9, 8, 9, 3, 1,
        1, 1
    ]

    df = _parse_master_file(
        base_dir,
        "kosdaq_code",
        part1_columns,
        part2_columns,
        field_specs,
        row_len_offset=222
    )

    df = cast(
        pd.DataFrame,
        df.loc[
            (df["증권그룹구분코드"] == "ST")
            & (df["기업인수목적회사여부"] == "N")
            & (df["기준년월"].notna()),
            [
                "단축코드",
                "한글명",
                "거래정지 여부",
                "정리매매 여부",
                "관리 종목 여부",
                "시장 경고 구분 코드",
                "시장 경고위험 예고 여부",
                "상장 주수(천)",
                "전일기준 시가총액 (억)",
            ],
        ].copy(),
    )

    df = df.rename(
        columns={
            "거래정지 여부": "거래정지",
            "정리매매 여부": "정리매매",
            "관리 종목 여부": "관리종목",
            "시장 경고 구분 코드": "시장경고",
            "시장 경고위험 예고 여부": "경고예고",
            "상장 주수(천)": "상장주수",
            "전일기준 시가총액 (억)": "market_cap",
        }
    )

    df["상장주수"] = cast(pd.Series, pd.to_numeric(df["상장주수"], errors="coerce")).mul(1000)
    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")

    df["시장구분"] = "KOSDAQ"
    return df


def get_kospi_kosdaq_master_dataframe(base_dir: str) -> pd.DataFrame:
    """KOSPI와 KOSDAQ 종목 마스터 파일을 읽어서 DataFrame으로 통합합니다."""
    df_kospi = _read_kospi_master(base_dir)
    df_kosdaq = _read_kosdaq_master(base_dir)
    df_all = pd.concat([df_kospi, df_kosdaq], ignore_index=True)
    return cast(
        pd.DataFrame,
        df_all.loc[
            :,
            ["단축코드", "한글명", "거래정지", "정리매매", "관리종목", "시장경고", "경고예고", "시장구분", "market_cap", "상장주수"],
        ].copy(),
    )


__all__ = [
    "download_code_master",
    "get_sector_master_dataframe",
    "get_kospi_kosdaq_master_dataframe",
]
