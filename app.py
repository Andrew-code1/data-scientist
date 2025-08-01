# backdata_dashboard.py
"""구매 데이터 대시보드 (Streamlit + DuckDB)

- **파일 업로드 방식**: 실행 후 CSV(인코딩 cp949)를 업로드합니다.
- **필터**: 연도(마감월→연도), 플랜트, 구매그룹, 공급업체(업체명)
- **집계**:
    1. 연도별 송장수량(천 EA), 송장금액(백만 원)
    2. (공급업체명 컬럼 존재 시) 업체별 송장수량·금액 테이블
- **기능**: 자재명 부분 검색, 결과 CSV 다운로드(UTF-8 BOM)

실행::
    streamlit run backdata_dashboard.py
"""

from __future__ import annotations

from io import BytesIO
from typing import Optional

import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="구매 데이터 대시보드", layout="wide")

# ---------------------------------------------------------------------------
# 유틸리티 함수
# ---------------------------------------------------------------------------

def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """컬럼명 공백 제거 + 공통 명칭 통일"""
    df.columns = df.columns.str.strip()

    # 유사 명칭 매핑 규칙
    rename_map = {}
    for col in df.columns:
        if col == "공급업체명":
            continue
        if "공급업체" in col or "공급사" in col:
            rename_map[col] = "공급업체명"
        elif col.replace(" ", "") == "구매그룹명":
            rename_map[col] = "구매그룹"

    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def load_csv(upload: BytesIO) -> pd.DataFrame:
    """업로드된 CSV(bytes) → DataFrame 변환 및 전처리"""
    df = pd.read_csv(upload, encoding="cp949", low_memory=False)
    df = _standardize_columns(df)

    # 날짜 변환 ↔ 연도 컬럼 생성
    if "마감월" not in df.columns:
        st.error("⚠️ '마감월' 컬럼을 찾을 수 없습니다. 헤더명을 확인해 주세요.")
        st.stop()

    df["마감월"] = pd.to_datetime(df["마감월"], errors="coerce")
    df["연도"] = df["마감월"].dt.year.astype("Int64")

    # 숫자형 컬럼 정리
    for ncol in ["송장수량", "송장금액", "플랜트", "구매그룹"]:
        if ncol in df.columns:
            df[ncol] = pd.to_numeric(df[ncol], errors="coerce").fillna(0)

    # 문자열 공백 제거 (공급업체명 있을 때)
    if "공급업체명" in df.columns:
        df["공급업체명"] = df["공급업체명"].astype(str).str.strip()

    return df


def num_list_to_sql(values: list[int]) -> str:
    return ",".join(map(str, values)) if values else "-1"  # 존재하지 않을 값


def str_list_to_sql(values: list[str]) -> str:
    return ",".join(f"'{v.replace("'", "''")}'" for v in values) if values else "''"

# ---------------------------------------------------------------------------
# 사이드바 – CSV 업로드
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("CSV 업로드")
    uploaded_file = st.file_uploader("backdata.csv (cp949) 업로드", type="csv")

# ---------------------------------------------------------------------------
# 데이터 준비 (Session State 활용)
# ---------------------------------------------------------------------------

if uploaded_file is not None:
    if (
        "file_name" not in st.session_state
        or st.session_state["file_name"] != uploaded_file.name
    ):
        st.session_state["df"] = load_csv(uploaded_file)
        st.session_state["file_name"] = uploaded_file.name

    df = st.session_state["df"]
else:
    st.info("먼저 CSV 파일을 업로드해 주세요.")
    df: Optional[pd.DataFrame] = None

# ---------------------------------------------------------------------------
# 대시보드 본문 (파일 업로드 후 표시)
# ---------------------------------------------------------------------------

if df is not None and not df.empty:
    con = duckdb.connect(database=":memory:")
    con.register("data", df)

    # -------------------- 필터 --------------------
    with st.sidebar:
        st.header("필터 조건")
        years_all = df["연도"].dropna().astype(int).sort_values().unique().tolist()
        plants_all = df["플랜트"].dropna().astype(int).sort_values().unique().tolist() if "플랜트" in df.columns else []
        groups_all = df["구매그룹"].dropna().astype(int).sort_values().unique().tolist() if "구매그룹" in df.columns else []
        suppliers_all = (
            df["공급업체명"].dropna().sort_values().unique().tolist() if "공급업체명" in df.columns else []
        )

        selected_years = st.multiselect("연도", years_all, default=years_all)
        selected_plants = st.multiselect("플랜트", plants_all, default=plants_all) if plants_all else []
        selected_groups = st.multiselect("구매그룹", groups_all, default=groups_all) if groups_all else []
        if suppliers_all:
            selected_suppliers = st.multiselect("공급업체", suppliers_all, default=suppliers_all)
        else:
            selected_suppliers = []

    year_clause = num_list_to_sql(selected_years)
    plant_clause = num_list_to_sql(selected_plants)
    group_clause = num_list_to_sql(selected_groups)
    supplier_clause = str_list_to_sql(selected_suppliers)

    # -------------------- 연도별 집계 --------------------
    filter_where = f"""
        WHERE 연도 IN ({year_clause})
    """
    if plants_all:
        filter_where += f" AND 플랜트 IN ({plant_clause})"
    if groups_all:
        filter_where += f" AND 구매그룹 IN ({group_clause})"
    if suppliers_all:
        filter_where += f" AND 공급업체명 IN ({supplier_clause})"

    year_df = con.execute(
        f"""
        SELECT 연도,
               ROUND(SUM(송장수량) / 1000, 2)  AS 송장수량_천EA,
               ROUND(SUM(송장금액) / 1000000, 2) AS 송장금액_백만원
        FROM data
        {filter_where}
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchdf()

    st.title("📊 연도별 구매 현황")
    st.dataframe(year_df, hide_index=True, use_container_width=True)
    if not year_df.empty:
        st.line_chart(year_df.set_index("연도"))
    st.caption("단위: 송장수량 = 천 EA, 송장금액 = 백만 원")

    # -------------------- 업체별 집계 (공급업체명 있는 경우) --------------------
    if suppliers_all:
        sup_df = con.execute(
            f"""
            SELECT 공급업체명,
                   ROUND(SUM(송장수량) / 1000, 2)  AS 송장수량_천EA,
                   ROUND(SUM(송장금액) / 1000000, 2) AS 송장금액_백만원
            FROM data
            {filter_where}
            GROUP BY 1
            ORDER BY 2 DESC
            """
        ).fetchdf()

        st.markdown("---")
        st.header("🏢 업체별 구매 현황")
        st.dataframe(sup_df, hide_index=True, use_container_width=True)

        if not sup_df.empty:
            sup_csv = sup_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button("업체별 CSV 다운로드", sup_csv, file_name="supplier_summary.csv", mime="text/csv")

    # -------------------- 자재명 부분 검색 --------------------
    st.markdown("---")
    st.header("🔍 자재명 검색")
    keyword = st.text_input("자재명(일부 문자열 입력 가능)")

    if keyword:
        safe_kw = keyword.replace("'", "''")
        search_where = filter_where + f" AND 자재명 ILIKE '%{safe_kw}%'"
        search_df = con.execute(
            f"""
            SELECT 마감월, 연도, 플랜트, 구매그룹,
                   {"공급업체명," if suppliers_all else ""}
                   자재   AS 자재코드,
                   자재명,
                   ROUND(송장수량 / 1000, 2)  AS 송장수량_천EA,
                   ROUND(송장금액 / 1000000, 2) AS 송장금액_백만원
            FROM data
            {search_where}
            ORDER BY 마감월
            """
        ).fetchdf()

        st.write(f"검색 결과: **{len(search_df):,}건** 일치")
        st.dataframe(search_df, use_container_width=True)

        if not search_df.empty:
            csv_bytes = search_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button(
                "CSV 다운로드", csv_bytes, file_name=f"search_{keyword}.csv", mime="text/csv"
            )
    else:
        st.info("자재명을 입력하시면 검색 결과가 표시됩니다.")
