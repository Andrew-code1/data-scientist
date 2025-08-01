# backdata_dashboard.py
"""구매 데이터 대시보드 (Streamlit + DuckDB)

- **파일 업로드**: 앱 실행 후 CSV(인코딩 cp949)를 업로드합니다.
- **필터**: 연도, 플랜트, 구매그룹, 공급업체명
- **집계**
    1. 연도별 송장수량(천 EA)·송장금액(백만 원)
    2. 공급업체별 송장수량·금액 (해당 컬럼이 있는 경우)
- **기능**: 자재명 부분 검색, 결과 CSV 다운로드(UTF‑8 BOM)

실행::
    streamlit run backdata_dashboard.py
"""

from __future__ import annotations

from io import BytesIO
from typing import Optional, List

import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="구매 데이터 대시보드", layout="wide")

# ---------------------------------------------------------------------------
# 유틸리티 함수
# ---------------------------------------------------------------------------

def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """공백 제거·유사 헤더 통합·중복 열 제거"""
    df.columns = df.columns.str.strip()

    rename_map: dict[str, str] = {}
    for col in df.columns:
        if col == "공급업체명":
            continue
        if "공급업체" in col or "공급사" in col:
            rename_map[col] = "공급업체명"
        elif col.replace(" ", "") == "구매그룹명":
            rename_map[col] = "구매그룹"

    if rename_map:
        df = df.rename(columns=rename_map)

    # duplicated columns → keep first
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]

    return df


def load_csv(upload: BytesIO) -> pd.DataFrame:
    """CSV(bytes) → DataFrame 전처리"""
    df = pd.read_csv(upload, encoding="cp949", low_memory=False)
    df = _standardize_columns(df)

    if "마감월" not in df.columns:
        st.error("⚠️ '마감월' 컬럼을 찾을 수 없습니다. 헤더를 확인해 주세요.")
        st.stop()

    # 날짜 변환 & 연도 파생
    df["마감월"] = pd.to_datetime(df["마감월"], errors="coerce")
    df["연도"] = df["마감월"].dt.year.astype("Int64")

    # 숫자 컬럼 변환
    numeric_cols: List[str] = [
        c for c in ["송장수량", "송장금액", "단가", "플랜트", "구매그룹"] if c in df.columns
    ]
    if numeric_cols:
        df[numeric_cols] = (
            df[numeric_cols]
            .apply(lambda s: pd.to_numeric(s, errors="coerce"))
            .fillna(0)
        )

    if "공급업체명" in df.columns:
        df["공급업체명"] = df["공급업체명"].astype(str).str.strip()

    return df


def num_list_to_sql(values: list[int]) -> str:
    return ",".join(map(str, values)) if values else "-1"


def str_list_to_sql(values: list[str]) -> str:
    if not values:
        return "''"
    escaped = [v.replace("'", "''") for v in values]
    return ",".join(f"'{v}'" for v in escaped)

# ---------------------------------------------------------------------------
# 파일 업로드 (사이드바)
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("CSV 업로드")
    uploaded_file = st.file_uploader("backdata.csv (cp949) 업로드", type="csv")

# ---------------------------------------------------------------------------
# 세션 상태
# ---------------------------------------------------------------------------

if uploaded_file is not None:
    if (
        "file_name" not in st.session_state or
        st.session_state["file_name"] != uploaded_file.name
    ):
        st.session_state["df"] = load_csv(uploaded_file)
        st.session_state["file_name"] = uploaded_file.name

    df: Optional[pd.DataFrame] = st.session_state["df"]
else:
    st.info("먼저 CSV 파일을 업로드해 주세요.")
    df = None

# ---------------------------------------------------------------------------
# 대시보드 본문
# ---------------------------------------------------------------------------

if df is not None and not df.empty:
    con = duckdb.connect(database=":memory:")
    con.register("data", df)

    # ---------------- 필터 ----------------
    with st.sidebar:
        st.header("필터 조건")
        years_all = df["연도"].dropna().astype(int).sort_values().unique().tolist()
        plants_all = df["플랜트"].dropna().astype(int).sort_values().unique().tolist() if "플랜트" in df.columns else []
        groups_all = df["구매그룹"].dropna().astype(int).sort_values().unique().tolist() if "구매그룹" in df.columns else []
        suppliers_all = df["공급업체명"].dropna().sort_values().unique().tolist() if "공급업체명" in df.columns else []

        selected_years = st.multiselect("연도", years_all, default=years_all)
        selected_plants = st.multiselect("플랜트", plants_all, default=plants_all) if plants_all else []
        selected_groups = st.multiselect("구매그룹", groups_all, default=groups_all) if groups_all else []
        selected_suppliers = st.multiselect("공급업체", suppliers_all, default=suppliers_all) if suppliers_all else []

    year_clause = num_list_to_sql(selected_years)
    plant_clause = num_list_to_sql(selected_plants)
    group_clause = num_list_to_sql(selected_groups)
    supplier_clause = str_list_to_sql(selected_suppliers)

    where_parts = [f"연도 IN ({year_clause})"]
    if plants_all:
        where_parts.append(f"플랜트 IN ({plant_clause})")
    if groups_all:
        where_parts.append(f"구매그룹 IN ({group_clause})")
    if suppliers_all:
        where_parts.append(f"공급업체명 IN ({supplier_clause})")

    filter_where = " WHERE " + " AND ".join(where_parts)

    # ---------------- 연도별 집계 ----------------
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

    # ---------------- 업체별 집계 ----------------
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

    # ---------------- 자재명 검색 ----------------
    st.markdown("---")
    st.header("🔍 자재명 검색")
    keyword = st.text_input("자재명(일부 문자열 입력 가능)")

    if keyword:
        safe_kw = keyword.replace("'", "''")
        search_where = filter_where + f" AND 자재명 ILIKE '%{safe_kw}%'"
        select_cols = "마감월, 연도, 플랜트, 구매그룹, " if "공급업체명" not in df.columns else "마감월, 연도, 플랜트, 구매그룹, 공급업체명, "
        search_df = con.execute(
            f"""
            SELECT {select_cols}자재 AS 자재코드,
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
            st.download_button("CSV 다운로드", csv_bytes, file_name=f"search_{keyword}.csv", mime="text/csv")
    else:
        st.info("자재명을 입력하시면 검색 결과가 표시됩니다.")
