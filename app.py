# backdata_dashboard.py
"""구매 데이터 대시보드 (Streamlit + DuckDB)

실행::
    streamlit run backdata_dashboard.py
"""
from __future__ import annotations

from io import BytesIO
from typing import List, Optional

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="구매 데이터 대시보드", layout="wide")

# 작은 버튼 스타일 적용
st.markdown("""
<style>
    /*  */
    .stSidebar .stButton > button {
        height: 2.2rem !important;
        font-size: 0.6rem !important;
        padding: 0.3rem 0.5rem !important;
        min-height: 2.2rem !important;
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
        min-width: fit-content !important;
    }
</style>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════════════
#  Data Load
# ════════════════════════════════════════════════════════════════════════

def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    rename_map: dict[str, str] = {}
    for col in df.columns:
        norm = col.replace(" ", "")
        if norm in {"공급업체", "공급사명"}:
            rename_map[col] = "공급업체명"
        elif norm in {"공급업체코드", "공급사코드"}:
            rename_map[col] = "공급업체코드"
        elif norm == "구매그룹명":
            rename_map[col] = "구매그룹"
    df = df.rename(columns=rename_map)
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]
    return df


@st.cache_data(show_spinner=False)
def load_csv(upload: BytesIO) -> pd.DataFrame:
    df = pd.read_csv(upload, encoding="cp949", low_memory=False)
    df = _standardize_columns(df)

    if "마감월" not in df.columns:
        st.error(" '마감월' 컬럼을 찾을 수 없습니다. 헤더명을 확인해 주세요.")
        st.stop()

    # Excel Date conversion
    if pd.api.types.is_numeric_dtype(df["마감월"]):
        df["마감월"] = pd.to_datetime(df["마감월"], unit="D", origin="1899-12-30", errors="coerce")
    else:
        df["마감월"] = pd.to_datetime(df["마감월"], errors="coerce")

    df["연도"] = df["마감월"].dt.year.astype("Int64")
    df["연월"] = df["마감월"].dt.to_period("M").dt.to_timestamp()

    num_cols: List[str] = [c for c in ["송장수량", "송장금액", "단가", "플랜트", "구매그룹"] if c in df.columns]
    if num_cols:
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    # 공급업체 표시
    if "공급업체명" in df.columns:
        df["공급업체명"] = df["공급업체명"].astype(str).str.strip()
    if "공급업체코드" in df.columns:
        df["공급업체코드"] = df["공급업체코드"].astype(str).str.strip()
        df["업체표시"] = df["공급업체코드"].str.zfill(5) + "_" + df["공급업체명"].fillna("")
    elif "공급업체명" in df.columns:
        df["업체표시"] = df["공급업체명"]

    return df

# ════════════════════════════════════════════════════════════════════════
#  헬퍼 함수
# ════════════════════════════════════════════════════════════════════════

def sql_list_num(vals: list[int]) -> str:
    return ",".join(map(str, vals)) if vals else "-1"


def sql_list_str(vals: list[str]) -> str:
    if not vals:
        return "''"
    esc = [v.replace("'", "''") for v in vals]
    return ",".join(f"'{v}'" for v in esc)


# ---- 멀티셀렉트 전체/해제 ----

def _set_all(key: str, opts: list):
    st.session_state[key] = opts

def _clear_all(key: str):
    st.session_state[key] = []

def multiselect_with_toggle(label: str, options: list, key_prefix: str) -> list:
    ms_key = f"{key_prefix}_ms"
    if ms_key not in st.session_state:
        st.session_state[ms_key] = options
    col1, col2, col3 = st.columns([7, 0.7, 0.7])
    with col1:
        sel = st.multiselect(label, options, key=ms_key)
    with col2:
        st.button("전체", on_click=_set_all, args=(ms_key, options), key=f"{key_prefix}_all")
    with col3:
        st.button("해제", on_click=_clear_all, args=(ms_key,), key=f"{key_prefix}_none")
    return sel

# ════════════════════════════════════════════════════════════════════════
#  파일 업로드
# ════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.header("CSV 업로드")
    uploaded_file = st.file_uploader("backdata.csv (cp949)", type="csv")

if uploaded_file:
    with st.spinner("CSV 불러오는 중..."):
        if st.session_state.get("file_name") != uploaded_file.name:
            st.session_state["df"] = load_csv(uploaded_file)
            st.session_state["file_name"] = uploaded_file.name
    df: Optional[pd.DataFrame] = st.session_state["df"]
else:
    st.info("먼저 CSV 파일을 업로드해 주세요.")
    df = None

# ════════════════════════════════════════════════════════════════════════
#  대시보드
# ════════════════════════════════════════════════════════════════════════
if df is not None and not df.empty:
    con = duckdb.connect(database=":memory:")
    con.register("data", df)

    # --- 사이드바 필터 ---
    with st.sidebar:
        st.header("필터 조건")
        years_all = sorted(df["연도"].dropna().astype(int).unique().tolist())
        plants_all = sorted(df["플랜트"].dropna().astype(int).unique().tolist()) if "플랜트" in df.columns else []
        groups_all = sorted(df["구매그룹"].dropna().astype(int).unique().tolist()) if "구매그룹" in df.columns else []
        suppliers_all = sorted(df["업체표시"].dropna().unique().tolist()) if "업체표시" in df.columns else []

        sel_years = multiselect_with_toggle("연도", years_all, "yr")
        sel_plants = multiselect_with_toggle("플랜트", plants_all, "pl") if plants_all else []
        sel_groups = multiselect_with_toggle("구매그룹", groups_all, "gr") if groups_all else []
        sel_suppliers = multiselect_with_toggle("공급업체", suppliers_all, "sp") if suppliers_all else []

    clauses = [f"연도 IN ({sql_list_num(sel_years)})"]
    if plants_all:
        clauses.append(f"플랜트 IN ({sql_list_num(sel_plants)})")
    if groups_all:
        clauses.append(f"구매그룹 IN ({sql_list_num(sel_groups)})")
    if suppliers_all:
        names = [s.split("_", 1)[1] if "_" in s else s for s in sel_suppliers]
        clauses.append(f"공급업체명 IN ({sql_list_str(names)})")

    where_sql = " WHERE " + " AND ".join(clauses)

    # --- 월별 시계열 ---
    st.title("월별 구매 추이")
    
    # 시계열 옵션 선택
    col1, col2 = st.columns(2)
    with col1:
        metric_option = st.selectbox(
            "표시할 지표",
            ["송장금액", "송장수량"],
            key="metric_select"
        )
    with col2:
        group_option = st.selectbox(
            "분석 단위",
            ["전체", "플랜트별", "업체별", "플랜트+업체별"],
            key="group_select"
        )

    # 지표별 설정
    if metric_option == "송장금액":
        metric_col = "SUM(송장금액)/1000000"
        metric_name = "송장금액_백만원"
        unit_text = "백만원"
        y_title = "송장금액 (백만원)"
    else:
        metric_col = "SUM(송장수량)/1000"
        metric_name = "송장수량_천EA"
        unit_text = "천EA"
        y_title = "송장수량 (천EA)"

    # 그룹별 SQL 쿼리 생성
    if group_option == "전체":
        group_by_sql = ""
        group_col = ""
        select_cols = f"date_trunc('month', 마감월) AS 연월, {metric_col} AS {metric_name}"
        group_by_clause = "GROUP BY 1"
    elif group_option == "플랜트별":
        group_by_sql = "플랜트,"
        group_col = "플랜트"
        select_cols = f"date_trunc('month', 마감월) AS 연월, {group_by_sql} {metric_col} AS {metric_name}"
        group_by_clause = "GROUP BY 1, 2"
    elif group_option == "업체별":
        group_by_sql = "공급업체명,"
        group_col = "공급업체명"
        select_cols = f"date_trunc('month', 마감월) AS 연월, {group_by_sql} {metric_col} AS {metric_name}"
        group_by_clause = "GROUP BY 1, 2"
    else:  # 플랜트+업체별
        group_by_sql = "플랜트, 공급업체명,"
        group_col = "플랜트_업체"
        select_cols = f"date_trunc('month', 마감월) AS 연월, {group_by_sql} {metric_col} AS {metric_name}"
        group_by_clause = "GROUP BY 1, 2, 3"

    # 데이터 조회
    month_df = con.execute(
        f"""
        SELECT {select_cols}
        FROM data
        {where_sql}
        {group_by_clause}
        ORDER BY 1, 2{', 3' if group_option == '플랜트+업체별' else ''}
        """
    ).fetchdf()

    if month_df.empty:
        st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
    else:
        month_df["연월표시"] = month_df["연월"].dt.strftime("%Y년%m월")
        
        # 플랜트+업체별인 경우 조합 컬럼 생성
        if group_option == "플랜트+업체별":
            month_df["플랜트_업체"] = month_df["플랜트"].astype(str) + "_" + month_df["공급업체명"]
        
        # 데이터테이블 표시
        if group_option == "전체":
            display_cols = ["연월표시", metric_name]
            st.dataframe(month_df[display_cols], hide_index=True, use_container_width=True)
        elif group_option == "플랜트+업체별":
            display_cols = ["연월표시", "플랜트", "공급업체명", metric_name]
            st.dataframe(month_df[display_cols], hide_index=True, use_container_width=True)
        else:
            display_cols = ["연월표시", group_col, metric_name]
            st.dataframe(month_df[display_cols], hide_index=True, use_container_width=True)

        # 차트 생성
        if group_option == "전체":
            chart = (
                alt.Chart(month_df)
                .mark_line(point=True)
                .encode(
                    x=alt.X("연월:T", title="연월", axis=alt.Axis(format="%Y년%m월", labelAngle=-45)),
                    y=alt.Y(f"{metric_name}:Q", title=y_title),
                    tooltip=["연월표시:N", f"{metric_name}:Q"],
                )
                .interactive()
            )
        elif group_option == "플랜트+업체별":
            chart = (
                alt.Chart(month_df)
                .mark_line(point=True)
                .encode(
                    x=alt.X("연월:T", title="연월", axis=alt.Axis(format="%Y년%m월", labelAngle=-45)),
                    y=alt.Y(f"{metric_name}:Q", title=y_title),
                    color=alt.Color("플랜트_업체:N", title="플랜트_업체"),
                    tooltip=["연월표시:N", "플랜트:O", "공급업체명:N", f"{metric_name}:Q"],
                )
                .interactive()
            )
        else:
            chart = (
                alt.Chart(month_df)
                .mark_line(point=True)
                .encode(
                    x=alt.X("연월:T", title="연월", axis=alt.Axis(format="%Y년%m월", labelAngle=-45)),
                    y=alt.Y(f"{metric_name}:Q", title=y_title),
                    color=alt.Color(f"{group_col}:N", title=group_col),
                    tooltip=["연월표시:N", f"{group_col}:N", f"{metric_name}:Q"],
                )
                .interactive()
            )
        
        st.altair_chart(chart, use_container_width=True)
        
    st.caption(f"단위: {metric_option} = {unit_text}")

    # --- 업체별 집계 ---
    if suppliers_all:
        sup_df = con.execute(
            f"""
            SELECT 공급업체명,
                   SUM(송장수량)/1000    AS 송장수량_천EA,
                   SUM(송장금액)/1000000 AS 송장금액_백만원
            FROM data
            {where_sql}
            GROUP BY 1
            ORDER BY 2 DESC
            """
        ).fetchdf()

        st.markdown("---")
        st.header(" 업체별 구매 현황")
        st.dataframe(sup_df, hide_index=True, use_container_width=True)

        if not sup_df.empty:
            st.download_button(
                "업체별 CSV 다운로드",
                sup_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                file_name="supplier_summary.csv",
                mime="text/csv",
            )

    # --- 자재명 검색 ---
    st.markdown("---")
    st.header(" 자재명 검색 (와일드카드 * 사용 가능)")
    patt = st.text_input("자재명 패턴", placeholder="예) *퍼퓸*1L*")

    if patt:
        patt_sql = patt.replace("*", "%").replace("'", "''")
        search_df = con.execute(
            f"""
            SELECT 마감월, 연월, 연도, 플랜트, 구매그룹,
                   {"공급업체명, " if "공급업체명" in df.columns else ""}
                   자재 AS 자재코드,
                   자재명,
                   송장수량/1000    AS 송장수량_천EA,
                   송장금액/1000000 AS 송장금액_백만원
            FROM data
            {where_sql} AND 자재명 ILIKE '{patt_sql}'
            ORDER BY 마감월
            """
        ).fetchdf()

        st.write(f"검색 결과: **{len(search_df):,}건** 일치")
        if search_df.empty:
            st.info("검색 결과가 없습니다.")
        else:
            st.dataframe(search_df, use_container_width=True)
            st.download_button(
                "검색결과 CSV 다운로드",
                search_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                file_name="search_results.csv",
                mime="text/csv",
            )
