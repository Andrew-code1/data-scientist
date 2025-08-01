# backdata_dashboard.py
"""구매 데이터 대시보드 (Streamlit + DuckDB)

- **파일 업로드 방식**: 앱 실행 후 CSV(인코딩 cp949)를 업로드합니다.
- **필터**: 연도(마감월→연도), 플랜트, 구매그룹
- **집계**: 연도별 송장수량(천 EA), 송장금액(백만 원)
- **기능**: 자재명 부분 검색, 결과 CSV 다운로드(UTF‑8 BOM)

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

def load_csv(upload: BytesIO) -> pd.DataFrame:
    """업로드된 CSV(bytes) → DataFrame 변환 및 전처리"""
    df = pd.read_csv(upload, encoding="cp949", low_memory=False)

    # 날짜 변환 ↔ 연도 컬럼 생성
    df["마감월"] = pd.to_datetime(df["마감월"], errors="coerce")
    df["연도"] = df["마감월"].dt.year.astype("Int64")

    # 숫자형 컬럼 정리
    df["송장수량"] = pd.to_numeric(df["송장수량"], errors="coerce").fillna(0)
    df["송장금액"] = pd.to_numeric(df["송장금액"], errors="coerce").fillna(0)

    return df


def list_to_sql_in(values: list) -> str:
    """Python 리스트를 SQL IN 절 문자열로 변환 (비어있으면 NULL 방지용 dummy)"""
    return ",".join(map(str, values)) if values else "-1"  # 존재하지 않을 값

# ---------------------------------------------------------------------------
# 사이드바 – CSV 업로드
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("CSV 업로드")
    uploaded_file = st.file_uploader("backdata.csv (cp949) 업로드", type="csv")

# ---------------------------------------------------------------------------
# 데이터 준비 (Session State 활용)
# ---------------------------------------------------------------------------

if uploaded_file:
    if "df" not in st.session_state or st.session_state["file_id"] != uploaded_file.id:
        st.session_state["df"] = load_csv(uploaded_file)
        st.session_state["file_id"] = uploaded_file.id  # 새 파일 업로드 여부 체크

    df = st.session_state["df"]
else:
    st.info("먼저 CSV 파일을 업로드해 주세요.")
    df: Optional[pd.DataFrame] = None

# ---------------------------------------------------------------------------
# 대시보드 본문 (파일 업로드 후 표시)
# ---------------------------------------------------------------------------

if df is not None and not df.empty:
    # DuckDB 연결 (매 요청마다 메모리 DB 새로 생성하여 데이터 등록)
    con = duckdb.connect(database=":memory:")
    con.register("data", df)

    # -------------------- 필터 --------------------
    with st.sidebar:
        st.header("필터 조건")
        years_all = df["연도"].dropna().astype(int).sort_values().unique().tolist()
        plants_all = df["플랜트"].dropna().astype(int).sort_values().unique().tolist()
        groups_all = df["구매그룹"].dropna().astype(int).sort_values().unique().tolist()

        selected_years = st.multiselect("연도", years_all, default=years_all)
        selected_plants = st.multiselect("플랜트", plants_all, default=plants_all)
        selected_groups = st.multiselect("구매그룹", groups_all, default=groups_all)

    year_clause = list_to_sql_in(selected_years)
    plant_clause = list_to_sql_in(selected_plants)
    group_clause = list_to_sql_in(selected_groups)

    # -------------------- 집계 쿼리 --------------------
    agg_sql = f"""
        SELECT
            연도,
            ROUND(SUM(송장수량) / 1000, 2)  AS 송장수량_천EA,
            ROUND(SUM(송장금액) / 1000000, 2) AS 송장금액_백만원
        FROM data
        WHERE 연도 IN ({year_clause})
          AND 플랜트 IN ({plant_clause})
          AND 구매그룹 IN ({group_clause})
        GROUP BY 1
        ORDER BY 1
    """
    result_df = con.execute(agg_sql).fetchdf()

    # -------------------- 시각화/표 --------------------
    st.title("📊 연도별 구매 현황")
    st.dataframe(result_df, hide_index=True, use_container_width=True)

    if not result_df.empty:
        st.line_chart(result_df.set_index("연도"))
    st.caption("단위: 송장수량 = 천 EA, 송장금액 = 백만 원")

    # -------------------- 자재명 부분 검색 --------------------
    st.markdown("---")
    st.header("🔍 자재명 검색")
    keyword = st.text_input("자재명(일부 문자열 입력 가능)")

    if keyword:
        safe_kw = keyword.replace("'", "''")
        search_sql = f"""
            SELECT 마감월, 연도, 플랜트, 구매그룹,
                   자재   AS 자재코드,
                   자재명,
                   ROUND(송장수량 / 1000, 2)  AS 송장수량_천EA,
                   ROUND(송장금액 / 1000000, 2) AS 송장금액_백만원
            FROM data
            WHERE 자재명 ILIKE '%{safe_kw}%'
            ORDER BY 마감월
        """
        search_df = con.execute(search_sql).fetchdf()

        st.write(f"검색 결과: **{len(search_df):,}건** 일치")
        st.dataframe(search_df, use_container_width=True)

        if not search_df.empty:
            csv_bytes = search_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button(
                "CSV 다운로드", csv_bytes, file_name=f"search_{keyword}.csv", mime="text/csv"
            )
    else:
        st.info("자재명을 입력하시면 검색 결과가 표시됩니다.")
