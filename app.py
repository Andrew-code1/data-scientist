# backdata_dashboard.py
"""구매 데이터 대시보드 (Streamlit + DuckDB)

[업데이트 ⚙️]
1. **전체‑선택 / 전체‑해제** 버튼(연도‧플랜트‧구매그룹‧공급업체) 추가
2. **자재명 와일드카드 검색** ─ `*` → SQL `%` 로 매핑 (예: `*퍼퓸*1L*`)
3. **월별 시계열** (`YYYY년MM월`) + Altair 툴팁 지원
4. **공급업체 필터**: `코드_업체명` 형식으로 표시해 이름·코드 모두 검색 가능

실행::
    streamlit run backdata_dashboard.py
"""

from __future__ import annotations

from io import BytesIO
from typing import List, Optional

import duckdb
import pandas as pd
import streamlit as st
import altair as alt

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
        elif col.replace(" ", "") == "공급업체코드":
            rename_map[col] = "공급업체코드"
    if rename_map:
        df = df.rename(columns=rename_map)
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]
    return df


def load_csv(upload: BytesIO) -> pd.DataFrame:
    df = pd.read_csv(upload, encoding="cp949", low_memory=False)
    df = _standardize_columns(df)

    if "마감월" not in df.columns:
        st.error("⚠️ '마감월' 컬럼을 찾을 수 없습니다. 헤더를 확인해 주세요.")
        st.stop()

    # 날짜 변환 & 파생 연·월
    df["마감월"] = pd.to_datetime(df["마감월"], errors="coerce")
    df["연도"] = df["마감월"].dt.year.astype("Int64")
    df["연월"] = df["마감월"].dt.to_period("M").dt.to_timestamp()

    # 숫자 컬럼 정리
    num_cols: List[str] = [c for c in ["송장수량", "송장금액", "단가", "플랜트", "구매그룹"] if c in df.columns]
    if num_cols:
        df[num_cols] = df[num_cols].apply(lambda s: pd.to_numeric(s, errors="coerce")).fillna(0)

    # 공급업체 정보 정리
    if "공급업체명" in df.columns:
        df["공급업체명"] = df["공급업체명"].astype(str).str.strip()
    if "공급업체코드" in df.columns:
        df["공급업체코드"] = df["공급업체코드"].astype(str).str.strip()
        # 표시용 컬럼 (코드_이름)
        df["업체표시"] = df["공급업체코드"].str.zfill(5) + "_" + df["공급업체명"].fillna("")
    elif "공급업체명" in df.columns:
        df["업체표시"] = df["공급업체명"]

    return df


def sql_list_num(values: list[int]) -> str:
    return ",".join(map(str, values)) if values else "-1"


def sql_list_str(values: list[str]) -> str:
    if not values:
        return "''"
    esc = [v.replace("'", "''") for v in values]
    return ",".join(f"'{v}'" for v in esc)

# ---------------------------------------------------------------------------
# 공통 위젯: 멀티셀렉트 + 전체/해제 버튼
# ---------------------------------------------------------------------------

def multiselect_with_toggle(label: str, options: list, default: list, key_prefix: str):
    ms_key = f"{key_prefix}_ms"
    if ms_key not in st.session_state:
        st.session_state[ms_key] = default

    cols = st.columns([3, 1, 1])
    with cols[0]:
        selected = st.multiselect(label, options, default=st.session_state[ms_key], key=ms_key)
    with cols[1]:
        if st.button("전체", key=f"{key_prefix}_all"):
            st.session_state[ms_key] = options
            selected = options
    with cols[2]:
        if st.button("해제", key=f"{key_prefix}_none"):
            st.session_state[ms_key] = []
            selected = []
    return selected

# ---------------------------------------------------------------------------
# 파일 업로드 (사이드바)
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("CSV 업로드")
    uploaded_file = st.file_uploader("backdata.csv (cp949) 업로드", type="csv")

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
        suppliers_all = df["업체표시"].dropna().sort_values().unique().tolist() if "업체표시" in df.columns else []

        selected_years = multiselect_with_toggle("연도", years_all, years_all, "yr")
        selected_plants = multiselect_with_toggle("플랜트", plants_all, plants_all, "pl") if plants_all else []
        selected_groups = multiselect_with_toggle("구매그룹", groups_all, groups_all, "gr") if groups_all else []
        selected_suppliers = multiselect_with_toggle("공급업체", suppliers_all, suppliers_all, "sp") if suppliers_all else []

    # ---- SQL WHERE 절 조립 ----
    where_parts = [f"연도 IN ({sql_list_num(selected_years)})"]
    if plants_all:
        where_parts.append(f"플랜트 IN ({sql_list_num(selected_plants)})")
    if groups_all:
        where_parts.append(f"구매그룹 IN ({sql_list_num(selected_groups)})")
    if suppliers_all:
        # 선택된 표시값 -> 이름 파싱 (코드_이름 → 이름)
        supplier_names = [s.split("_", 1)[1] if "_" in s else s for s in selected_suppliers]
        where_parts.append(f"공급업체명 IN ({sql_list_str(supplier_names)})")

    filter_where = " WHERE " + " AND ".join(where_parts)

    # ---------------- 월별 시계열 집계 ----------------
    month_df = con.execute(
        f"""
        SELECT date_trunc('month', 마감월) AS 연월,
               ROUND(SUM(송장수량) / 1000, 2)  AS 송장수량_천EA,
               ROUND(SUM(송장금액) / 1000000, 2) AS 송장금액_백만원
        FROM data
        {filter_where}
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchdf()
    month_df["연월표시"] = month_df["연월"].dt.strftime("%Y년%m월")

    st.title("📈 월별 구매 현황")
    st.dataframe(month_df[["연월표시", "송장수량_천EA", "송장금액_백만원"]], hide_index=True, use_container_width=True)

    if not month_df.empty:
        line = (
            alt.Chart(month_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("연월:T", title="연월"),
                y=alt.Y("송장금액_백만원:Q", title="금액(백만 원)"),
                tooltip=["연월표시:N", "송장수량_천EA:Q", "송장금액_백만원:Q"]
            )
            .interactive()
        )
        st.altair_chart(line, use_container_width=True)
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
    st.header("🔍 자재명 검색 (와일드카드: *)")
    keyword = st.text_input("자재명 패턴 예) *퍼퓸*1L*")

    if keyword:
        pattern_sql = keyword.replace("*", "%").replace("'", "''")
        search_where = filter_where + f" AND 자재명 ILIKE '{pattern_sql}'"

        select_cols = "마감월, 연월, 연도, 플랜트, 구매그룹, "
        if suppliers_all:
            select_cols += "공급업체명, "
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
            csv_bytes = search
