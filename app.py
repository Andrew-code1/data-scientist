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
        # 업체코드가 있는 경우 업체코드로 필터링, 없는 경우 업체명으로 필터링
        if "공급업체코드" in df.columns:
            codes = [s.split("_", 1)[0] if "_" in s else s for s in sel_suppliers]
            clauses.append(f"공급업체코드 IN ({sql_list_str(codes)})")
        else:
            names = [s.split("_", 1)[1] if "_" in s else s for s in sel_suppliers]
            clauses.append(f"공급업체명 IN ({sql_list_str(names)})")

    where_sql = " WHERE " + " AND ".join(clauses)

    st.title("구매 데이터 추이 분석")
    
    col1, col2, col3 = st.columns(3)
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
    with col3:
        time_unit = st.selectbox(
            "집계 단위",
            ["월별", "연도별"],
            key="time_unit_select"
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

    # 시간 집계 단위에 따른 설정
    if time_unit == "월별":
        time_col = "date_trunc('month', 마감월)"
        time_name = "연월"
        time_format = "%Y년%m월"
    else:  # 연도별
        time_col = "연도"
        time_name = "연도"
        time_format = "%Y년"

    if group_option == "전체":
        group_by_sql = ""
        group_col = ""
        select_cols = f"{time_col} AS {time_name}, {metric_col} AS {metric_name}"
        group_by_clause = "GROUP BY 1"
    elif group_option == "플랜트별":
        group_by_sql = "플랜트,"
        group_col = "플랜트"
        select_cols = f"{time_col} AS {time_name}, {group_by_sql} {metric_col} AS {metric_name}"
        group_by_clause = "GROUP BY 1, 2"
    elif group_option == "업체별":
        group_by_sql = "공급업체명,"
        group_col = "공급업체명"
        select_cols = f"{time_col} AS {time_name}, {group_by_sql} {metric_col} AS {metric_name}"
        group_by_clause = "GROUP BY 1, 2"
    else:  # 플랜트+업체별
        group_by_sql = "플랜트, 공급업체명,"
        group_col = "플랜트_업체"
        select_cols = f"{time_col} AS {time_name}, {group_by_sql} {metric_col} AS {metric_name}"
        group_by_clause = "GROUP BY 1, 2, 3"

    time_df = con.execute(
        f"""
        SELECT {select_cols}
        FROM data
        {where_sql}
        {group_by_clause}
        ORDER BY 1, 2{', 3' if group_option == '플랜트+업체별' else ''}
        """
    ).fetchdf()

    if time_df.empty:
        st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
    else:
        # 시간 표시 컬럼 생성
        if time_unit == "월별":
            time_df["시간표시"] = time_df[time_name].dt.strftime(time_format)
        else:  # 연도별
            time_df["시간표시"] = time_df[time_name].astype(int).astype(str) + "년"
        
        if group_option == "플랜트+업체별":
            time_df["플랜트_업체"] = time_df["플랜트"].astype(str) + "_" + time_df["공급업체명"]
        
        # 데이터 테이블 표시
        if group_option == "전체":
            display_cols = ["시간표시", metric_name]
            st.dataframe(time_df[display_cols], hide_index=True, use_container_width=True)
        elif group_option == "플랜트+업체별":
            display_cols = ["시간표시", "플랜트", "공급업체명", metric_name]
            st.dataframe(time_df[display_cols], hide_index=True, use_container_width=True)
        else:
            display_cols = ["시간표시", group_col, metric_name]
            st.dataframe(time_df[display_cols], hide_index=True, use_container_width=True)

        # 차트 생성 - 클릭 이벤트 추가
        click = alt.selection_point(name="point_select")
        
        if time_unit == "월별":
            x_encoding = alt.X(f"{time_name}:T", title=time_unit, axis=alt.Axis(format=time_format, labelAngle=-45))
        else:
            x_encoding = alt.X(f"{time_name}:O", title=time_unit)

        if group_option == "전체":
            chart = (
                alt.Chart(time_df)
                .mark_line(point=True)
                .encode(
                    x=x_encoding,
                    y=alt.Y(f"{metric_name}:Q", title=y_title),
                    tooltip=["시간표시:N", f"{metric_name}:Q"],
                )
                .add_params(click)
            )
        elif group_option == "플랜트+업체별":
            chart = (
                alt.Chart(time_df)
                .mark_line(point=True)
                .encode(
                    x=x_encoding,
                    y=alt.Y(f"{metric_name}:Q", title=y_title),
                    color=alt.Color("플랜트_업체:N", title="플랜트_업체"),
                    tooltip=["시간표시:N", "플랜트:O", "공급업체명:N", f"{metric_name}:Q"],
                )
                .add_params(click)
            )
        else:
            chart = (
                alt.Chart(time_df)
                .mark_line(point=True)
                .encode(
                    x=x_encoding,
                    y=alt.Y(f"{metric_name}:Q", title=y_title),
                    color=alt.Color(f"{group_col}:N", title=group_col),
                    tooltip=["시간표시:N", f"{group_col}:N", f"{metric_name}:Q"],
                )
                .add_params(click)
            )
        
        # 차트 표시 및 클릭 이벤트 처리
        event = st.altair_chart(chart, use_container_width=True, key="main_chart")
        
        # 디버깅: 이벤트 정보 표시
        if st.checkbox("디버그 모드 (이벤트 정보 표시)", key="debug_mode"):
            st.write("Event object type:", type(event))
            st.write("Event object:", event)
            if event is not None and hasattr(event, 'selection'):
                st.write("Selection type:", type(event.selection))
                st.write("Selection:", event.selection)
                if event.selection is not None:
                    st.write("Selection keys:", list(event.selection.keys()) if isinstance(event.selection, dict) else "Not a dict")
            else:
                st.write("Event has no selection attribute")
        
        # 클릭 이벤트 처리 (안전한 방식)
        selected_data = None
        try:
            if (event is not None and 
                hasattr(event, 'selection') and 
                event.selection is not None and 
                isinstance(event.selection, dict) and
                "point_select" in event.selection):
                selected_data = event.selection["point_select"]
                if selected_data:
                    st.info(f"차트 클릭 감지됨! 선택된 데이터: {selected_data}")
        except Exception as e:
            if st.session_state.get("debug_mode", False):
                st.error(f"Selection 처리 중 오류: {e}")
        
        # 대체 방안: 드롭다운으로 데이터 선택
        st.markdown("---")
        st.subheader("📊 상세 Raw 데이터 조회")
        
        with st.expander("데이터 선택 방식", expanded=True):
            col1, col2 = st.columns(2)
            
            with col1:
                # 시간 선택
                available_times = time_df[time_name].unique()
                if time_unit == "월별":
                    time_options = [(t.strftime(time_format), t) for t in available_times]
                else:
                    time_options = [(f"{int(t)}년", t) for t in available_times]
                
                selected_time_display = st.selectbox(
                    f"{time_unit} 선택",
                    options=[opt[0] for opt in time_options],
                    key="time_select"
                )
                selected_time_value = dict(time_options)[selected_time_display]
            
            with col2:
                # 그룹 선택 (필요한 경우)
                if group_option != "전체":
                    if group_option == "플랜트별":
                        available_groups = time_df[time_df[time_name] == selected_time_value]["플랜트"].unique()
                        selected_group = st.selectbox("플랜트 선택", options=available_groups, key="plant_select")
                        info_text = f"선택된 기간: {selected_time_display}, 플랜트: {selected_group}"
                    elif group_option == "업체별":
                        available_groups = time_df[time_df[time_name] == selected_time_value]["공급업체명"].unique()
                        selected_group = st.selectbox("업체 선택", options=available_groups, key="supplier_select")
                        info_text = f"선택된 기간: {selected_time_display}, 업체: {selected_group}"
                    else:  # 플랜트+업체별
                        filtered_df = time_df[time_df[time_name] == selected_time_value]
                        available_combos = filtered_df[["플랜트", "공급업체명"]].drop_duplicates()
                        combo_options = []
                        for _, row in available_combos.iterrows():
                            plant = row['플랜트']
                            supplier = row['공급업체명']
                            if pd.notna(plant) and pd.notna(supplier):
                                try:
                                    plant_int = int(plant)
                                    combo_options.append(f"플랜트{plant_int}-{supplier}")
                                except (ValueError, TypeError):
                                    continue
                        if combo_options:
                            selected_combo = st.selectbox("플랜트-업체 선택", options=combo_options, key="combo_select")
                            try:
                                plant_str = selected_combo.split('-')[0].replace('플랜트', '')
                                plant_val = int(plant_str) if plant_str else 0
                                supplier_val = selected_combo.split('-', 1)[1] if '-' in selected_combo else ""
                                info_text = f"선택된 기간: {selected_time_display}, 플랜트: {plant_val}, 업체: {supplier_val}"
                            except (ValueError, IndexError, AttributeError):
                                plant_val = 0
                                supplier_val = ""
                                info_text = f"선택된 기간: {selected_time_display}, 플랜트+업체 데이터 오류"
                        else:
                            st.warning("해당 기간에 플랜트+업체 데이터가 없습니다.")
                            plant_val = 0
                            supplier_val = ""
                            info_text = f"선택된 기간: {selected_time_display}, 데이터 없음"
                else:
                    info_text = f"선택된 기간: {selected_time_display}"
            
            # Raw 데이터 조회 버튼
            if st.button("상세 데이터 조회", type="primary"):
                # Raw 데이터 쿼리 생성
                if time_unit == "월별":
                    time_filter = f"date_trunc('month', 마감월) = '{selected_time_value}'"
                else:
                    time_filter = f"연도 = {selected_time_value}"
                
                # 기본 쿼리
                raw_data_query = f"""
                SELECT 마감월, 플랜트, 구매그룹, 
                       {"CAST(공급업체코드 AS INTEGER) AS 공급업체코드, " if "공급업체코드" in df.columns else ""}
                       공급업체명, 자재 AS 자재코드, 자재명,
                       송장수량, 송장금액, 단가
                FROM data
                WHERE {time_filter}
                """
                
                # 기존 필터 조건 추가 (연도 필터 제외)
                additional_filters = []
                if plants_all and sel_plants:
                    additional_filters.append(f"플랜트 IN ({sql_list_num(sel_plants)})")
                if groups_all and sel_groups:
                    additional_filters.append(f"구매그룹 IN ({sql_list_num(sel_groups)})")
                if suppliers_all and sel_suppliers:
                    # 업체코드가 있는 경우 업체코드로 필터링, 없는 경우 업체명으로 필터링
                    if "공급업체코드" in df.columns:
                        codes = [s.split("_", 1)[0] if "_" in s else s for s in sel_suppliers]
                        additional_filters.append(f"공급업체코드 IN ({sql_list_str(codes)})")
                    else:
                        names = [s.split("_", 1)[1] if "_" in s else s for s in sel_suppliers]
                        additional_filters.append(f"공급업체명 IN ({sql_list_str(names)})")
                
                # 그룹별 추가 필터
                if group_option == "플랜트별":
                    additional_filters.append(f"플랜트 = {selected_group}")
                elif group_option == "업체별":
                    additional_filters.append(f"공급업체명 = '{selected_group}'")
                elif group_option == "플랜트+업체별":
                    if 'plant_val' in locals() and 'supplier_val' in locals() and plant_val != 0 and supplier_val:
                        additional_filters.append(f"플랜트 = {plant_val} AND 공급업체명 = '{supplier_val}'")
                
                if additional_filters:
                    raw_data_query += " AND " + " AND ".join(additional_filters)
                
                raw_data_query += " ORDER BY 마감월, 공급업체명, 자재코드"
                
                # 쿼리 실행
                raw_df = con.execute(raw_data_query).fetchdf()
                
                # 결과 표시
                if not raw_df.empty:
                    st.success(f"**총 {len(raw_df):,}건의 데이터를 찾았습니다!**")
                    st.dataframe(raw_df, use_container_width=True, hide_index=True)
                    
                    # 요약 정보
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("총 송장금액", f"{raw_df['송장금액'].sum():,.0f}원")
                    with col2:
                        st.metric("총 송장수량", f"{raw_df['송장수량'].sum():,.0f}")
                    
                    # CSV 다운로드
                    st.download_button(
                        "상세 데이터 CSV 다운로드",
                        raw_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                        file_name=f"raw_data_{info_text.replace(' ', '_').replace(':', '').replace('-', '_')}.csv",
                        mime="text/csv",
                    )
                else:
                    st.warning("해당 조건에 맞는 상세 데이터가 없습니다.")
        
    st.caption(f"단위: {metric_option} = {unit_text}")

    if suppliers_all:
        sup_df = con.execute(
            f"""
            SELECT {"CAST(공급업체코드 AS INTEGER) AS 공급업체코드, " if "공급업체코드" in df.columns else ""}
                   공급업체명,
                   SUM(송장수량)/1000    AS 송장수량_천EA,
                   SUM(송장금액)/1000000 AS 송장금액_백만원
            FROM data
            {where_sql}
            GROUP BY {"1, 2" if "공급업체코드" in df.columns else "1"}
            ORDER BY {"3" if "공급업체코드" in df.columns else "2"} DESC
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
    st.header("🔍 자재 검색 (와일드카드 * 사용 가능)")
    col1, col2 = st.columns(2)
    with col1:
        material_name_patt = st.text_input("자재명 패턴", placeholder="예) *퍼퓸*1L*")
    with col2:
        material_code_patt = st.text_input("자재코드 패턴", placeholder="예) *1234567*")

    # 패턴 강화 함수
    def enhance_pattern(pattern):
        if "*" not in pattern:
            if " " in pattern:
                # 띄어쓰기가 있으면 각 단어에 와일드카드 적용
                words = pattern.split()
                pattern = "*" + "*".join(words) + "*"
            else:
                # 단일 단어도 양쪽에 와일드카드 추가
                pattern = "*" + pattern + "*"
        return pattern.replace("*", "%").replace("'", "''")

    # 검색 조건 생성
    search_conditions = []
    search_info = []
    
    if material_name_patt:
        enhanced_name_patt = enhance_pattern(material_name_patt)
        search_conditions.append(f"자재명 ILIKE '{enhanced_name_patt}'")
        search_info.append(f"자재명: {material_name_patt}")
    
    if material_code_patt:
        enhanced_code_patt = enhance_pattern(material_code_patt)
        search_conditions.append(f"CAST(자재 AS VARCHAR) ILIKE '{enhanced_code_patt}'")
        search_info.append(f"자재코드: {material_code_patt}")

    if search_conditions:
        # AND 조건으로 검색 (둘 다 입력된 경우) 또는 개별 조건
        search_where = " AND ".join(search_conditions)
        
        search_df = con.execute(
            f"""
            SELECT 마감월, 연월, 연도, 플랜트, 구매그룹,
                   {"CAST(공급업체코드 AS INTEGER) AS 공급업체코드, " if "공급업체코드" in df.columns else ""}
                   {"공급업체명, " if "공급업체명" in df.columns else ""}
                   자재 AS 자재코드,
                   자재명,
                   단가,
                   송장수량/1000    AS 송장수량_천EA,
                   송장금액/1000000 AS 송장금액_백만원
            FROM data
            {where_sql} AND ({search_where})
            ORDER BY 마감월
            """
        ).fetchdf()

        # 검색 조건 표시
        search_info_text = ", ".join(search_info)
        st.write(f"검색 조건: {search_info_text}")
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
