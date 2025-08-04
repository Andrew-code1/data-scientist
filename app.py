from io import BytesIO
from typing import List, Optional

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="구매 데이터 대시보드", layout="wide")

st.markdown("""
<style>
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


def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    rename_map: dict[str, str] = {}
    for col in df.columns:
        norm = col.replace(" ", "")
        if norm in {"업체명"}:
            rename_map[col] = "공급업체명"
        elif norm in {"공급업체", "공급사코드"}:
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

    if pd.api.types.is_numeric_dtype(df["마감월"]):
        df["마감월"] = pd.to_datetime(df["마감월"], unit="D", origin="1899-12-30", errors="coerce")
    else:
        df["마감월"] = pd.to_datetime(df["마감월"], errors="coerce")

    df["연도"] = df["마감월"].dt.year.astype("Int64")
    df["연월"] = df["마감월"].dt.to_period("M").dt.to_timestamp()

    num_cols: List[str] = [c for c in ["송장수량", "송장금액", "단가", "플랜트", "구매그룹"] if c in df.columns]
    if num_cols:
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    if "공급업체명" in df.columns:
        df["공급업체명"] = df["공급업체명"].astype(str).str.strip()
    if "공급업체코드" in df.columns:
        df["공급업체코드"] = df["공급업체코드"].astype(str).str.strip()
        df["업체표시"] = df["공급업체코드"].str.zfill(5) + "_" + df["공급업체명"].fillna("")
    elif "공급업체명" in df.columns:
        df["업체표시"] = df["공급업체명"]

    return df


def sql_list_num(vals: list[int]) -> str:
    return ",".join(map(str, vals)) if vals else "-1"


def sql_list_str(vals: list[str]) -> str:
    if not vals:
        return "''"
    esc = [v.replace("'", "''") for v in vals]
    return ",".join(f"'{v}'" for v in esc)



def _set_all(key: str, opts: list):
    st.session_state[key] = opts

def _clear_all(key: str):
    st.session_state[key] = []

def multiselect_with_toggle(label: str, options: list, key_prefix: str) -> list:
    ms_key = f"{key_prefix}_ms"
    if ms_key not in st.session_state:
        st.session_state[ms_key] = options
    col1, col2 = st.columns([8.5, 0.7])
    with col1:
        sel = st.multiselect(label, options, key=ms_key)
    with col2:
        st.button("⚫", on_click=_set_all, args=(ms_key, options), key=f"{key_prefix}_all", help="전체 선택")
    return sel

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

if df is not None and not df.empty:
    con = duckdb.connect(database=":memory:")
    con.register("data", df)

    with st.sidebar:
        st.header("필터 조건")
        years_all = sorted(df["연도"].dropna().astype(int).unique().tolist())
        plants_all = sorted(df["플랜트"].dropna().astype(int).unique().tolist()) if "플랜트" in df.columns else []
        groups_all = sorted(df["구매그룹"].dropna().astype(int).unique().tolist()) if "구매그룹" in df.columns else []
        suppliers_all = sorted(df["업체표시"].dropna().unique().tolist()) if "업체표시" in df.columns else []

        # 연도 범위 선택
        min_year, max_year = min(years_all), max(years_all)
        
        st.subheader("연도 범위")
        year_filter_type = st.radio("선택 방식", ["슬라이더", "직접 입력"], horizontal=True)
        
        if year_filter_type == "슬라이더":
            year_range = st.slider(
                "연도 범위 선택",
                min_value=min_year,
                max_value=max_year,
                value=(min_year, max_year),
                step=1
            )
            start_year, end_year = year_range
        else:
            col1, col2 = st.columns(2)
            with col1:
                start_year = st.number_input("시작 연도", min_value=min_year, max_value=max_year, value=min_year)
            with col2:
                end_year = st.number_input("끝 연도", min_value=min_year, max_value=max_year, value=max_year)
        
        # 선택된 연도 범위에 해당하는 연도들
        sel_years = [year for year in years_all if start_year <= year <= end_year]
        
        st.write(f"선택된 연도: {start_year}년 ~ {end_year}년 ({len(sel_years)}개)")

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

    st.title("월별 구매 추이")
    
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
        
        if group_option == "플랜트+업체별":
            month_df["플랜트_업체"] = month_df["플랜트"].astype(str) + "_" + month_df["공급업체명"]
        
        if group_option == "전체":
            display_cols = ["연월표시", metric_name]
            st.dataframe(month_df[display_cols], hide_index=True, use_container_width=True)
        elif group_option == "플랜트+업체별":
            display_cols = ["연월표시", "플랜트", "공급업체명", metric_name]
            st.dataframe(month_df[display_cols], hide_index=True, use_container_width=True)
        else:
            display_cols = ["연월표시", group_col, metric_name]
            st.dataframe(month_df[display_cols], hide_index=True, use_container_width=True)

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

    st.markdown("---")
    st.header(" 자재명 검색 (와일드카드 * 사용 가능)")
    patt = st.text_input("자재명 패턴", placeholder="예) *퍼퓸*1L*")

    if patt:
        # 와일드카드가 없으면 자동으로 추가
        if "*" not in patt:
            if " " in patt:
                # 띄어쓰기가 있으면 각 단어에 와일드카드 적용
                words = patt.split()
                patt = "*" + "*".join(words) + "*"
            else:
                # 단일 단어도 양쪽에 와일드카드 추가
                patt = "*" + patt + "*"
        
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
