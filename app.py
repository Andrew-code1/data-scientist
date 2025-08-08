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
        # 공급업체코드 안전하게 처리 - 소수점을 정수로 변환
        def clean_supplier_code(x):
            if pd.isna(x) or str(x).lower() in ['nan', 'none', ''] or str(x).strip() == '':
                return ""
            try:
                # 숫자로 변환 가능한 경우 정수로 변환
                float_val = float(str(x).strip())
                return str(int(float_val))
            except (ValueError, TypeError):
                # 변환 불가능한 경우 원본 문자열 유지
                return str(x).strip()
        
        df["공급업체코드"] = df["공급업체코드"].apply(clean_supplier_code)
        # 공급업체코드가 있는 경우만 업체표시 생성
        df["업체표시"] = df.apply(
            lambda row: (
                str(row["공급업체코드"]) + "_" + str(row["공급업체명"]).strip()
                if row["공급업체코드"] and str(row["공급업체명"]).strip() and str(row["공급업체명"]) != "nan"
                else str(row["공급업체명"]).strip() if str(row["공급업체명"]) != "nan" else ""
            ), axis=1
        )
    elif "공급업체명" in df.columns:
        df["업체표시"] = df["공급업체명"]

    return df


def sql_list_num(vals: list[int]) -> str:
    return ",".join(map(str, vals)) if vals else "-1"


def sql_list_str(vals: list[str]) -> str:
    if not vals:
        return "''"
    # 안전한 문자열 이스케이프 - 한글/특수문자 포함
    safe_vals = []
    for v in vals:
        if v is None:
            continue
        # 문자열로 변환 후 안전하게 이스케이프
        v_str = str(v).strip()
        if v_str:  # 빈 문자열이 아닌 경우만 추가
            # SQL 이스케이프: 작은따옴표 이중화
            escaped = v_str.replace("'", "''")
            safe_vals.append(f"'{escaped}'")
    return ",".join(safe_vals) if safe_vals else "''"


def format_numeric_columns(df: pd.DataFrame, numeric_cols: list[str]) -> pd.DataFrame:
    """숫자 컬럼에 천단위 콤마 적용"""
    df_formatted = df.copy()
    for col in numeric_cols:
        if col in df_formatted.columns:
            df_formatted[col] = df_formatted[col].apply(
                lambda x: f"{x:,.1f}" if pd.notnull(x) and isinstance(x, (int, float)) else str(x)
            )
    return df_formatted



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
        # 안전한 필터 옵션 생성
        yearmonths_all = sorted(df["연월"].dropna().dt.strftime('%Y-%m').unique().tolist())
        plants_all = sorted([x for x in df["플랜트"].dropna().astype(int).unique() if x > 0]) if "플랜트" in df.columns else []
        groups_all = sorted([x for x in df["구매그룹"].dropna().astype(int).unique() if x > 0]) if "구매그룹" in df.columns else []
        suppliers_all = sorted([x for x in df["업체표시"].dropna().unique() 
                                if str(x).strip() != '' and 'nan' not in str(x).lower() and not str(x).startswith('0_')]) if "업체표시" in df.columns else []

        # 연월 범위 선택
        st.subheader("기간 입력 (YYYY-MM)")
        
        # 기본값을 최근 6개월로 설정
        default_start_idx = max(0, len(yearmonths_all) - 6)
        default_end_idx = len(yearmonths_all) - 1
        
        col1, col2 = st.columns(2)
        with col1:
            start_ym = st.selectbox("시작 연월", options=yearmonths_all, index=default_start_idx, key="start_ym")
        with col2:
            end_ym = st.selectbox("끝 연월", options=yearmonths_all, index=default_end_idx, key="end_ym")
        
        # 범위 내 연월들 선택
        start_idx = yearmonths_all.index(start_ym)
        end_idx = yearmonths_all.index(end_ym)
        if start_idx <= end_idx:
            sel_yearmonths = yearmonths_all[start_idx:end_idx+1]
        else:
            sel_yearmonths = yearmonths_all[end_idx:start_idx+1]
        
        st.write(f"선택된 연월: {len(sel_yearmonths)}개월 ({min(sel_yearmonths)} ~ {max(sel_yearmonths)})")

        sel_plants = multiselect_with_toggle("플랜트", plants_all, "pl") if plants_all else []
        sel_groups = multiselect_with_toggle("구매그룹", groups_all, "gr") if groups_all else []
        sel_suppliers = multiselect_with_toggle("공급업체", suppliers_all, "sp") if suppliers_all else []

    # 연월 필터링을 위한 SQL 조건 생성
    ym_conditions = []
    for ym in sel_yearmonths:
        year, month = ym.split('-')
        ym_conditions.append(f"(EXTRACT(YEAR FROM 마감월) = {year} AND EXTRACT(MONTH FROM 마감월) = {int(month)})")
    
    clauses = [f"({' OR '.join(ym_conditions)})"]
    if plants_all:
        clauses.append(f"플랜트 IN ({sql_list_num(sel_plants)})")
    if groups_all:
        clauses.append(f"구매그룹 IN ({sql_list_num(sel_groups)})")
    if suppliers_all:
        # 안전한 업체 필터 조건 생성
        if "공급업체코드" in df.columns:
            codes = []
            for s in sel_suppliers:
                if "_" in s:
                    code = s.split("_", 1)[0]
                    if code and code != "0":  # 유효한 코드만 추가
                        codes.append(code)
                elif s and s != "0":
                    codes.append(s)
            if codes:
                clauses.append(f"공급업체코드 IN ({sql_list_str(codes)})")
        else:
            names = []
            for s in sel_suppliers:
                if "_" in s:
                    name = s.split("_", 1)[1]
                    if name and name.strip():
                        names.append(name.strip())
                elif s and s.strip():
                    names.append(s.strip())
            if names:
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
            st.dataframe(
                time_df[display_cols], 
                hide_index=True, 
                use_container_width=True,
                column_config={
                    metric_name: st.column_config.NumberColumn(
                        metric_name.replace("_", "(").replace("EA", "EA)").replace("원", "원)"),
                        format="%.1f"
                    )
                }
            )
        elif group_option == "플랜트+업체별":
            display_cols = ["시간표시", "플랜트", "공급업체명", metric_name]
            st.dataframe(
                time_df[display_cols], 
                hide_index=True, 
                use_container_width=True,
                column_config={
                    metric_name: st.column_config.NumberColumn(
                        metric_name.replace("_", "(").replace("EA", "EA)").replace("원", "원)"),
                        format="%.1f"
                    )
                }
            )
        else:
            display_cols = ["시간표시", group_col, metric_name]
            st.dataframe(
                time_df[display_cols], 
                hide_index=True, 
                use_container_width=True,
                column_config={
                    metric_name: st.column_config.NumberColumn(
                        metric_name.replace("_", "(").replace("EA", "EA)").replace("원", "원)"),
                        format="%.1f"
                    )
                }
            )

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
        
        # Raw 데이터 조회 섹션
        st.markdown("---")
        st.subheader("📊 상세 Raw 데이터 조회")
        
        with st.expander("기간별 데이터 조회", expanded=True):
            # 조회 방식 선택
            query_mode = st.radio(
                "조회 방식 선택",
                ["특정 시점", "특정 기간"],
                horizontal=True,
                key="query_mode"
            )
            
            if query_mode == "특정 시점":
                # 특정 연월 선택
                selected_ym = st.selectbox(
                    "조회할 연월 (YYYY-MM) 선택",
                    options=sel_yearmonths,
                    key="single_ym_select"
                )
                query_yearmonths = [selected_ym]
                st.info(f"선택된 시점: {selected_ym} (해당 월 데이터)")
            else:
                # 기간 범위 선택  
                col1, col2 = st.columns(2)
                with col1:
                    period_start = st.selectbox(
                        "시작 연월", 
                        options=sel_yearmonths, 
                        index=0,
                        key="period_start"
                    )
                with col2:
                    period_end = st.selectbox(
                        "끝 연월", 
                        options=sel_yearmonths,
                        index=len(sel_yearmonths)-1,
                        key="period_end"
                    )
                
                # 선택된 기간 내 연월들
                start_idx = sel_yearmonths.index(period_start)
                end_idx = sel_yearmonths.index(period_end)
                if start_idx <= end_idx:
                    query_yearmonths = sel_yearmonths[start_idx:end_idx+1]
                else:
                    query_yearmonths = sel_yearmonths[end_idx:start_idx+1]
                
                st.info(f"선택된 기간: {min(query_yearmonths)} ~ {max(query_yearmonths)} ({len(query_yearmonths)}개월 누계)")
            
            col1, col2 = st.columns(2)
            
            with col2:
                # 그룹 선택 (필요한 경우)
                if group_option != "전체":
                    # 선택된 기간의 모든 데이터에서 그룹 옵션 가져오기
                    period_filter_conditions = []
                    for ym in query_yearmonths:
                        year, month = ym.split('-')
                        period_filter_conditions.append(f"(EXTRACT(YEAR FROM 마감월) = {year} AND EXTRACT(MONTH FROM 마감월) = {int(month)})")
                    
                    period_where = " OR ".join(period_filter_conditions)
                    
                    if group_option == "플랜트별":
                        # 기간 내 플랜트 옵션 조회
                        plants_in_period = con.execute(f"""
                            SELECT DISTINCT 플랜트 FROM data 
                            WHERE ({period_where}) AND 플랜트 > 0
                            ORDER BY 플랜트
                        """).fetchdf()['플랜트'].tolist()
                        
                        if plants_in_period:
                            selected_group = st.selectbox("플랜트 선택", options=plants_in_period, key="plant_select_period")
                            info_text = f"플랜트: {selected_group}"
                        else:
                            st.warning("해당 기간에 플랜트 데이터가 없습니다.")
                            selected_group = None
                            
                    elif group_option == "업체별":
                        # 기간 내 업체 옵션 조회
                        suppliers_in_period = con.execute(f"""
                            SELECT DISTINCT 공급업체명 FROM data 
                            WHERE ({period_where}) AND 공급업체명 IS NOT NULL AND 공급업체명 != ''
                            ORDER BY 공급업체명
                        """).fetchdf()['공급업체명'].tolist()
                        
                        if suppliers_in_period:
                            selected_group = st.selectbox("업체 선택", options=suppliers_in_period, key="supplier_select_period")
                            info_text = f"업체: {selected_group}"
                        else:
                            st.warning("해당 기간에 업체 데이터가 없습니다.")
                            selected_group = None
                            
                    else:  # 플랜트+업체별
                        # 기간 내 플랜트+업체 조합 조회
                        combos_in_period = con.execute(f"""
                            SELECT DISTINCT 플랜트, 공급업체명 FROM data 
                            WHERE ({period_where}) AND 플랜트 > 0 AND 공급업체명 IS NOT NULL AND 공급업체명 != ''
                            ORDER BY 플랜트, 공급업체명
                        """).fetchdf()
                        
                        if not combos_in_period.empty:
                            combo_options = []
                            for _, row in combos_in_period.iterrows():
                                plant = int(row['플랜트'])
                                supplier = row['공급업체명']
                                combo_options.append(f"플랜트{plant}-{supplier}")
                            
                            selected_combo = st.selectbox("플랜트-업체 선택", options=combo_options, key="combo_select_period")
                            plant_val = int(selected_combo.split('-')[0].replace('플랜트', ''))
                            supplier_val = selected_combo.split('-', 1)[1]
                            info_text = f"플랜트: {plant_val}, 업체: {supplier_val}"
                        else:
                            st.warning("해당 기간에 플랜트+업체 데이터가 없습니다.")
                            plant_val = None
                            supplier_val = None
                else:
                    info_text = f"전체 데이터"
            
            # Raw 데이터 조회 버튼
            if st.button("상세 데이터 조회", type="primary", key="raw_data_query_btn"):
                # 연월 기간 필터 조건 생성
                period_conditions = []
                for ym in query_yearmonths:
                    year, month = ym.split('-')
                    period_conditions.append(f"(EXTRACT(YEAR FROM 마감월) = {year} AND EXTRACT(MONTH FROM 마감월) = {int(month)})")
                
                period_filter = " OR ".join(period_conditions)
                
                # 기본 쿼리 - 안전한 캐스팅 적용
                supplier_code_select = ""
                if "공급업체코드" in df.columns:
                    supplier_code_select = """
                       CASE 
                           WHEN 공급업체코드 = '' OR 공급업체코드 IS NULL THEN NULL
                           ELSE CAST(CAST(공급업체코드 AS FLOAT) AS BIGINT)
                       END AS 공급업체코드,
                    """
                
                raw_data_query = f"""
                SELECT strftime(마감월, '%Y-%m') AS 마감월, 플랜트, 구매그룹,{supplier_code_select}
                       공급업체명, 자재 AS 자재코드, 자재명,
                       송장수량, 송장금액, 단가
                FROM data
                WHERE ({period_filter})
                """
                
                # 기존 필터 조건 추가
                additional_filters = []
                if plants_all and sel_plants:
                    additional_filters.append(f"플랜트 IN ({sql_list_num(sel_plants)})")
                if groups_all and sel_groups:
                    additional_filters.append(f"구매그룹 IN ({sql_list_num(sel_groups)})")
                if suppliers_all and sel_suppliers:
                    # 안전한 업체 필터 조건 생성
                    if "공급업체코드" in df.columns:
                        codes = []
                        for s in sel_suppliers:
                            if "_" in s:
                                code = s.split("_", 1)[0]
                                if code and code != "0":  # 유효한 코드만 추가
                                    codes.append(code)
                            elif s and s != "0":
                                codes.append(s)
                        if codes:
                            additional_filters.append(f"공급업체코드 IN ({sql_list_str(codes)})")
                    else:
                        names = []
                        for s in sel_suppliers:
                            if "_" in s:
                                name = s.split("_", 1)[1]
                                if name and name.strip():
                                    names.append(name.strip())
                            elif s and s.strip():
                                names.append(s.strip())
                        if names:
                            additional_filters.append(f"공급업체명 IN ({sql_list_str(names)})")
                
                # 그룹별 추가 필터
                if group_option == "플랜트별" and 'selected_group' in locals() and selected_group is not None:
                    additional_filters.append(f"플랜트 = {selected_group}")
                elif group_option == "업체별" and 'selected_group' in locals() and selected_group is not None:
                    additional_filters.append(f"공급업체명 = '{selected_group.replace("'", "''")}'")  # SQL 이스케이프
                elif group_option == "플랜트+업체별" and 'plant_val' in locals() and 'supplier_val' in locals() and plant_val is not None and supplier_val is not None:
                    additional_filters.append(f"플랜트 = {plant_val} AND 공급업체명 = '{supplier_val.replace("'", "''")}'")  # SQL 이스케이프
                
                if additional_filters:
                    raw_data_query += " AND " + " AND ".join(additional_filters)
                
                raw_data_query += " ORDER BY 마감월, 공급업체명, 자재코드"
                
                # 쿼리 실행
                raw_df = con.execute(raw_data_query).fetchdf()
                
                # 결과 표시
                if not raw_df.empty:
                    period_text = f"{min(query_yearmonths)}~{max(query_yearmonths)}" if len(query_yearmonths) > 1 else query_yearmonths[0]
                    st.success(f"**{period_text} 기간 총 {len(raw_df):,}건의 데이터를 찾았습니다!**")
                    
                    # 기간별 요약 정보 먼저 표시
                    if len(query_yearmonths) > 1:
                        summary_df = raw_df.groupby('마감월').agg({
                            '송장금액': 'sum',
                            '송장수량': 'sum',
                            '자재코드': 'count'
                        }).reset_index()
                        summary_df.columns = ['연월', '송장금액', '송장수량', '자재건수']
                        
                        st.subheader("📈 월별 누계 현황")
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("총 송장금액", f"{summary_df['송장금액'].sum():,.0f}원")
                        with col2:
                            st.metric("총 송장수량", f"{summary_df['송장수량'].sum():,.0f}")
                        with col3:
                            st.metric("총 자재건수", f"{summary_df['자재건수'].sum():,.0f}건")
                        
                        st.dataframe(
                            summary_df, 
                            use_container_width=True, 
                            hide_index=True,
                            column_config={
                                "송장금액": st.column_config.NumberColumn(
                                    "송장금액",
                                    format="%.0f"
                                ),
                                "송장수량": st.column_config.NumberColumn(
                                    "송장수량", 
                                    format="%.0f"
                                )
                            }
                        )
                    
                    st.subheader("📋 상세 Raw 데이터")
                    st.dataframe(
                        raw_df, 
                        use_container_width=True, 
                        hide_index=True,
                        column_config={
                            "송장금액": st.column_config.NumberColumn(
                                "송장금액",
                                format="%.0f"
                            ),
                            "송장수량": st.column_config.NumberColumn(
                                "송장수량", 
                                format="%.0f"
                            ),
                            "단가": st.column_config.NumberColumn(
                                "단가",
                                format="%.0f"
                            )
                        }
                    )
                    
                    # CSV 다운로드
                    filename_suffix = period_text.replace('~', '_to_').replace('-', '')
                    if group_option != "전체":
                        filename_suffix += f"_{info_text.replace(' ', '_').replace(':', '').replace('-', '_')}"
                    
                    st.download_button(
                        "상세 데이터 CSV 다운로드",
                        raw_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                        file_name=f"raw_data_{filename_suffix}.csv",
                        mime="text/csv",
                    )
                else:
                    st.warning("해당 조건에 맞는 상세 데이터가 없습니다.")
        
    st.caption(f"단위: {metric_option} = {unit_text}")

    if suppliers_all:
        # 안전한 업체별 구매 현황 쿼리
        supplier_code_select = ""
        group_by_clause = "1"
        order_by_clause = "2"
        
        if "공급업체코드" in df.columns:
            supplier_code_select = """
                   CASE 
                       WHEN 공급업체코드 = '' OR 공급업체코드 IS NULL THEN NULL
                       ELSE CAST(CAST(공급업체코드 AS FLOAT) AS BIGINT)
                   END AS 공급업체코드,
            """
            group_by_clause = "1, 2"
            order_by_clause = "3"
        
        sup_df = con.execute(
            f"""
            SELECT{supplier_code_select}
                   공급업체명,
                   SUM(송장수량)/1000    AS 송장수량_천EA,
                   SUM(송장금액)/1000000 AS 송장금액_백만원
            FROM data
            {where_sql}
            GROUP BY {group_by_clause}
            ORDER BY 송장금액_백만원 DESC, 송장수량_천EA DESC
            """
        ).fetchdf()

        st.markdown("---")
        st.header(" 업체별 구매 현황")
        st.dataframe(
            sup_df, 
            hide_index=True, 
            use_container_width=True,
            column_config={
                "송장금액_백만원": st.column_config.NumberColumn(
                    "송장금액(백만원)",
                    format="%.1f"
                ),
                "송장수량_천EA": st.column_config.NumberColumn(
                    "송장수량(천EA)", 
                    format="%.1f"
                )
            }
        )

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
        
        # 자재 검색 쿼리 - 안전한 캐스팅 적용
        search_supplier_code_select = ""
        if "공급업체코드" in df.columns:
            search_supplier_code_select = """
                   CASE 
                       WHEN 공급업체코드 = '' OR 공급업체코드 IS NULL THEN NULL
                       ELSE CAST(CAST(공급업체코드 AS FLOAT) AS BIGINT)
                   END AS 공급업체코드,
            """
        
        search_df = con.execute(
            f"""
            SELECT strftime(마감월, '%Y-%m') AS 마감월, strftime(연월, '%Y-%m') AS 연월, 연도, 플랜트, 구매그룹,{search_supplier_code_select}
                   {"공급업체명, " if "공급업체명" in df.columns else ""}
                   자재 AS 자재코드,
                   자재명,
                   단가,
                   송장수량/1000    AS 송장수량_천EA,
                   송장금액/1000000 AS 송장금액_백만원
            FROM data
            {where_sql} AND ({search_where})
            ORDER BY 마감월, 공급업체명, 자재코드
            """
        ).fetchdf()

        # 검색 조건 표시
        search_info_text = ", ".join(search_info)
        st.write(f"검색 조건: {search_info_text}")
        st.write(f"검색 결과: **{len(search_df):,}건** 일치")
        
        if search_df.empty:
            st.info("검색 결과가 없습니다.")
        else:
            # 연월별 검색 결과 요약
            if len(search_df) > 0 and len(sel_yearmonths) > 1:
                search_summary = search_df.groupby('연월').agg({
                    '송장금액_백만원': 'sum',
                    '송장수량_천EA': 'sum',
                    '자재코드': 'count'
                }).reset_index()
                search_summary.columns = ['연월', '송장금액_백만원', '송장수량_천EA', '자재건수']
                
                st.subheader("🔍 검색결과 월별 요약")
                st.dataframe(
                    search_summary, 
                    use_container_width=True, 
                    hide_index=True,
                    column_config={
                        "송장금액_백만원": st.column_config.NumberColumn(
                            "송장금액(백만원)",
                            format="%.1f"
                        ),
                        "송장수량_천EA": st.column_config.NumberColumn(
                            "송장수량(천EA)", 
                            format="%.1f"
                        )
                    }
                )
            
            st.subheader("📋 검색결과 상세")
            st.dataframe(
                search_df, 
                use_container_width=True,
                column_config={
                    "송장금액_백만원": st.column_config.NumberColumn(
                        "송장금액(백만원)",
                        format="%.1f"
                    ),
                    "송장수량_천EA": st.column_config.NumberColumn(
                        "송장수량(천EA)", 
                        format="%.1f"
                    ),
                    "단가": st.column_config.NumberColumn(
                        "단가",
                        format="%.0f"
                    )
                }
            )
            st.download_button(
                "검색결과 CSV 다운로드",
                search_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                file_name="search_results.csv",
                mime="text/csv",
            )
