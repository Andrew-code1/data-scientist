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
    
    # 향상된 컬럼 매핑 로직 - 각 변형들을 개별적으로 처리
    column_mappings = [
        # 공급업체 관련
        (["업체명", "공급업체명", "밤더명"], "공급업체명"),
        (["공급업체", "공급사코드", "공급업체코드", "밤더코드"], "공급업체코드"),
        # 구매그룹 관련
        (["구매그룹명", "구매그룹"], "구매그룹"),
        # 송장 관련
        (["송장금액", "인보이스금액", "발주금액"], "송장금액"),
        (["송장수량", "인보이스수량", "발주수량"], "송장수량"),
        # 자재 관련
        (["자재", "자재코드", "자재번호"], "자재"),
        (["자재명", "자재설명"], "자재명")
    ]
    
    for col in df.columns:
        norm = col.replace(" ", "").replace("(", "").replace(")", "").strip()
        
        # 각 매핑 그룹을 확인하여 매칭되는 컬럼 찾기
        for variations, target_name in column_mappings:
            if any(norm == var.replace(" ", "") for var in variations):
                rename_map[col] = target_name
                break
    
    df = df.rename(columns=rename_map)
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]
    return df


@st.cache_data(show_spinner=False)
def load_csv(upload: BytesIO) -> pd.DataFrame:
    df = pd.read_csv(upload, encoding="cp949", low_memory=False)
    
    # 원본 컬럼 정보 저장 (디버깅용)
    original_columns = list(df.columns)
    st.session_state["original_columns"] = original_columns
    
    df = _standardize_columns(df)
    
    # 컬럼 변환 후 정보 저장
    st.session_state["processed_columns"] = list(df.columns)
    
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
    
    # 숫자 컬럼 처리 전 데이터 샘플 저장 (디버깅용)
    numeric_debug_info = {}
    for col in ["송장수량", "송장금액", "단가"]:
        if col in df.columns:
            sample_values = df[col].head(5).tolist()
            numeric_debug_info[col] = {
                'sample_values': sample_values,
                'data_type': str(df[col].dtype),
                'null_count': df[col].isnull().sum(),
                'total_count': len(df[col])
            }
    st.session_state["numeric_debug_info"] = numeric_debug_info
    
    if num_cols:
        # 숫자 변환 시 오류 추적
        conversion_errors = {}
        for col in num_cols:
            original_values = df[col].copy()
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            
            # 변환 실패한 값들 추적
            failed_conversion = original_values[pd.to_numeric(original_values, errors="coerce").isnull()]
            if not failed_conversion.empty:
                conversion_errors[col] = failed_conversion.head(10).tolist()
        
        st.session_state["conversion_errors"] = conversion_errors
    
    # 데이터 품질 검사
    data_quality_issues = []
    
    # 송장금액 검사
    if "송장금액" in df.columns:
        zero_amount = (df["송장금액"] == 0).sum()
        total_rows = len(df)
        if zero_amount > total_rows * 0.5:  # 50% 이상이 0인 경우
            data_quality_issues.append(f"송장금액: {zero_amount}/{total_rows}건이 0 또는 비어있음")
    else:
        data_quality_issues.append("송장금액 컬럼이 발견되지 않음")
    
    # 송장수량 검사  
    if "송장수량" in df.columns:
        zero_quantity = (df["송장수량"] == 0).sum()
        total_rows = len(df)
        if zero_quantity > total_rows * 0.5:
            data_quality_issues.append(f"송장수량: {zero_quantity}/{total_rows}건이 0 또는 비어있음")
    else:
        data_quality_issues.append("송장수량 컬럼이 발견되지 않음")
    
    # 공급업체 정보 검사
    if "공급업체명" not in df.columns:
        data_quality_issues.append("공급업체명 컬럼이 발견되지 않음")
    
    st.session_state["data_quality_issues"] = data_quality_issues

    if "공급업체명" in df.columns:
        df["공급업체명"] = df["공급업체명"].astype(str).str.strip()
    if "공급업체코드" in df.columns:
        # 공급업체코드 안전하게 처리 - 소수점은 제거하되 원본 값 보존
        def clean_supplier_code(x):
            if pd.isna(x) or str(x).lower() in ['nan', 'none', ''] or str(x).strip() == '':
                return ""
            try:
                # 숫자로 변환 가능한 경우 소수점만 제거
                float_val = float(str(x).strip())
                # 정수 부분만 추출하되 문자열로 유지
                return str(int(float_val)) if float_val == int(float_val) else str(x).strip()
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
    st.info("파일 요구사항:")
    st.write("- 인코딩: CP949")
    st.write("- 필수 컬럼: 마감월, 송장금액, 송장수량")
    uploaded_file = st.file_uploader("CSV 파일 선택", type="csv", help="CP949 인코딩으로 저장된 CSV 파일")

if uploaded_file:
    with st.spinner("CSV 불러오는 중..."):
        if st.session_state.get("file_name") != uploaded_file.name:
            st.session_state["df"] = load_csv(uploaded_file)
            st.session_state["file_name"] = uploaded_file.name
    df: Optional[pd.DataFrame] = st.session_state["df"]
else:
    st.info("먼저 CSV 파일을 업로드해 주세요.")
    with st.expander("파일 업로드 도움말", expanded=False):
        st.write("**예상 컬럼 구조:**")
        st.write("- 마감월: 날짜 정보")
        st.write("- 송장금액: 숫자 데이터")
        st.write("- 송장수량: 숫자 데이터")
        st.write("- 공급업체명 또는 업체명")
        st.write("- 자재, 자재명")
        
        st.write("**지원되는 컬럼명 변형:**")
        st.write("- 업체명 → 공급업체명")
        st.write("- 공급업체 → 공급업체코드")
        st.write("- 인보이스금액 → 송장금액")
        st.write("- 발주수량 → 송장수량")
    
    df = None

if df is not None and not df.empty:
    # 디버깅 섹션 추가
    with st.expander("파일 분석 및 디버깅 정보 확인", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("원본 컬럼 목록")
            if "original_columns" in st.session_state:
                for i, col in enumerate(st.session_state["original_columns"], 1):
                    st.write(f"{i}. {col}")
            
            # 주요 컬럼 존재 여부 확인
            st.subheader("주요 컬럼 검사")
            key_columns = ["송장금액", "송장수량", "공급업체명", "자재", "자재명"]
            for col in key_columns:
                status = "✓" if col in df.columns else "❌"
                st.write(f"{status} {col}")
        
        with col2:
            st.subheader("처리 후 컬럼 목록")
            if "processed_columns" in st.session_state:
                for i, col in enumerate(st.session_state["processed_columns"], 1):
                    st.write(f"{i}. {col}")
            
            # 숫자 컬럼 데이터 품질 확인
            st.subheader("숫자 데이터 품질")
            if "numeric_debug_info" in st.session_state:
                for col, info in st.session_state["numeric_debug_info"].items():
                    st.write(f"**{col}**:")
                    st.write(f"- 데이터 타입: {info['data_type']}")
                    st.write(f"- 널 값: {info['null_count']}/{info['total_count']}")
                    st.write(f"- 샘플 값: {info['sample_values']}")
        
        # 변환 오류 정보
        if "conversion_errors" in st.session_state and st.session_state["conversion_errors"]:
            st.subheader("데이터 변환 문제")
            for col, errors in st.session_state["conversion_errors"].items():
                if errors:
                    st.error(f"{col} 컬럼에서 숫자로 변환할 수 없는 값들: {errors}")
        
        # 데이터 품질 경고
        if "data_quality_issues" in st.session_state and st.session_state["data_quality_issues"]:
            st.subheader("데이터 품질 문제")
            for issue in st.session_state["data_quality_issues"]:
                st.warning(issue)
            
            # 개선 제안
            st.info("해결 방안:")
            st.write("1. CSV 파일의 컬럼명이 올바른지 확인하세요")
            st.write("2. 송장금액, 송장수량 컬럼에 숫자 데이터가 들어있는지 확인하세요")
            st.write("3. 파일 인코딩이 CP949인지 확인하세요")
        
        # 데이터 미리보기
        st.subheader("데이터 미리보기 (5개 행)")
        preview_df = df.head()
        # 주요 컬럼만 보여주기 위해 컬럼 선택
        key_cols = [col for col in ["마감월", "공급업체명", "자재", "자재명", "송장수량", "송장금액", "단가"] if col in preview_df.columns]
        if key_cols:
            st.dataframe(preview_df[key_cols], use_container_width=True)
        else:
            st.dataframe(preview_df, use_container_width=True)
        
        # 집계 결과 디버깅 정보 (있는 경우)
        if "debug_aggregation_info" in st.session_state:
            st.subheader("차트 집계 결과 분석")
            agg_info = st.session_state["debug_aggregation_info"]
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("총 집계 행수", agg_info['total_rows'])
            with col2:
                st.metric("고유 월 수", agg_info['unique_months'])
            with col3:
                st.metric("날짜 범위", agg_info['date_range'])
            
            if agg_info['group_option'] != "전체":
                st.write(f"**분석 단위**: {agg_info['group_option']}")
                if 'unique_groups' in agg_info:
                    st.write(f"**고유 그룹 수**: {agg_info['unique_groups']}")
                    if 'groups_list' in agg_info:
                        st.write(f"**그룹 예시**: {', '.join(map(str, agg_info['groups_list'][:5]))}")
            
            # 중복 월 경고 - 더 상세한 분석
            expected_rows = agg_info['unique_months'] * (agg_info.get('unique_groups', 1) if agg_info['group_option'] != "전체" else 1)
            if agg_info['total_rows'] > expected_rows:
                st.warning(f"⚠️ X축 중복 감지! 예상: {expected_rows}행, 실제: {agg_info['total_rows']}행")
                st.error("이는 X축에 같은 월이 여러 번 나타나는 원인입니다.")
                
                # 해결 방안 제시
                st.info("**해결 방안:**")
                if agg_info['group_option'] == "전체":
                    st.write("- 원본 데이터에 같은 월의 중복 레코드가 있을 가능성")
                    st.write("- SQL 집계가 올바르게 되지 않고 있음")
                else:
                    st.write("- 각 그룹별로 시계열을 보려면 정상적인 현상일 수 있음")
                    st.write("- 전체 합계를 보려면 '전체' 분석 옵션을 선택하세요")
            else:
                st.success("✅ 정상적인 집계 결과입니다.")
            
            # 상세 차트 데이터 분석 추가
            if 'chart_data_sample' in agg_info:
                with st.expander("차트 데이터 샘플 (X축 중복 분석용)"):
                    st.dataframe(agg_info['chart_data_sample'], use_container_width=True)
            
            with st.expander("SQL 쿼리 확인"):
                st.code(agg_info['sql_query'], language="sql")
    
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
        
        # 새로운 필터 옵션들 추가
        parts_all = sorted([x for x in df["파트"].dropna().unique() 
                           if str(x).strip() != '' and 'nan' not in str(x).lower()]) if "파트" in df.columns else []
        categories_all = sorted([x for x in df["카테고리(최종)"].dropna().unique() 
                                if str(x).strip() != '' and 'nan' not in str(x).lower()]) if "카테고리(최종)" in df.columns else []
        kpi_categories_all = sorted([x for x in df["KPI용카테고리"].dropna().unique() 
                                    if str(x).strip() != '' and 'nan' not in str(x).lower()]) if "KPI용카테고리" in df.columns else []

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
        
        # 새로운 필터들 추가
        sel_parts = multiselect_with_toggle("파트", parts_all, "pt") if parts_all else []
        sel_categories = multiselect_with_toggle("카테고리(최종)", categories_all, "ct") if categories_all else []
        sel_kpi_categories = multiselect_with_toggle("KPI용카테고리", kpi_categories_all, "kc") if kpi_categories_all else []

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
    
    # 새로운 필터 조건들 추가
    if parts_all and sel_parts:
        clauses.append(f"파트 IN ({sql_list_str(sel_parts)})")
    if categories_all and sel_categories:
        clauses.append(f"\"카테고리(최종)\" IN ({sql_list_str(sel_categories)})")
    if kpi_categories_all and sel_kpi_categories:
        clauses.append(f"KPI용카테고리 IN ({sql_list_str(sel_kpi_categories)})")

    where_sql = " WHERE " + " AND ".join(clauses)

    st.title("구매 데이터 추이 분석")
    
    # 차트 해석 도움말
    with st.expander("📊 차트 해석 가이드", expanded=False):
        st.write("**월별 그래프에서 같은 월이 여러 번 나타나는 경우:**")
        st.write("- '전체' 분석: 일반적으로 월당 1개 데이터포인트")
        st.write("- '업체별' 분석: 같은 월에 여러 업체가 있으면 각각 별도 라인으로 표시")
        st.write("- '플랜트별' 분석: 같은 월에 여러 플랜트가 있으면 각각 별도 라인으로 표시")
        st.write("- 이는 정상적인 동작이며, 각 그룹별로 시계열을 보여주는 것입니다.")
        st.info("같은 월에 대한 전체 합계를 보고 싶다면 '전체' 분석 옵션을 선택하세요.")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        metric_option = st.selectbox(
            "표시할 지표",
            ["송장금액", "송장수량", "송장금액+송장수량"],
            key="metric_select"
        )
    with col2:
        group_option = st.selectbox(
            "분석 단위",
            ["전체", "플랜트별", "업체별", "플랜트+업체별", "파트별", "카테고리(최종)별", "KPI용카테고리별", "파트+카테고리(최종)별", "파트+KPI용카테고리별"],
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
        is_combined = False
    elif metric_option == "송장수량":
        metric_col = "SUM(송장수량)/1000"
        metric_name = "송장수량_천EA"
        unit_text = "천EA"
        y_title = "송장수량 (천EA)"
        is_combined = False
    else:  # 송장금액+송장수량
        metric_col = "SUM(송장금액)/1000000, SUM(송장수량)/1000"
        metric_name = "송장금액_백만원"  # 주 메트릭
        unit_text = "송장금액(백만원) / 송장수량(천EA)"
        y_title = "송장금액 (백만원)"
        is_combined = True

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
        if is_combined:
            select_cols = f"{time_col} AS {time_name}, SUM(송장금액)/1000000 AS 송장금액_백만원, SUM(송장수량)/1000 AS 송장수량_천EA"
        else:
            select_cols = f"{time_col} AS {time_name}, {metric_col} AS {metric_name}"
        # 시간별로만 그룹화하여 각 월당 1개 행만 생성
        group_by_clause = f"GROUP BY {time_col}"
    elif group_option == "플랜트별":
        group_by_sql = "플랜트,"
        group_col = "플랜트"
        if is_combined:
            select_cols = f"{time_col} AS {time_name}, {group_by_sql} SUM(송장금액)/1000000 AS 송장금액_백만원, SUM(송장수량)/1000 AS 송장수량_천EA"
        else:
            select_cols = f"{time_col} AS {time_name}, {group_by_sql} {metric_col} AS {metric_name}"
        # 시간과 플랜트별로만 그룹화
        group_by_clause = f"GROUP BY {time_col}, 플랜트"
    elif group_option == "업체별":
        group_by_sql = "공급업체명,"
        group_col = "공급업체명"
        if is_combined:
            select_cols = f"{time_col} AS {time_name}, {group_by_sql} SUM(송장금액)/1000000 AS 송장금액_백만원, SUM(송장수량)/1000 AS 송장수량_천EA"
        else:
            select_cols = f"{time_col} AS {time_name}, {group_by_sql} {metric_col} AS {metric_name}"
        # 시간과 업체별로만 그룹화
        group_by_clause = f"GROUP BY {time_col}, 공급업체명"
    elif group_option == "플랜트+업체별":
        group_by_sql = "플랜트, 공급업체명,"
        group_col = "플랜트_업체"
        if is_combined:
            select_cols = f"{time_col} AS {time_name}, {group_by_sql} SUM(송장금액)/1000000 AS 송장금액_백만원, SUM(송장수량)/1000 AS 송장수량_천EA"
        else:
            select_cols = f"{time_col} AS {time_name}, {group_by_sql} {metric_col} AS {metric_name}"
        # 시간, 플랜트, 업체별로 그룹화
        group_by_clause = f"GROUP BY {time_col}, 플랜트, 공급업체명"
    elif group_option == "파트별":
        group_by_sql = "파트,"
        group_col = "파트"
        if is_combined:
            select_cols = f"{time_col} AS {time_name}, {group_by_sql} SUM(송장금액)/1000000 AS 송장금액_백만원, SUM(송장수량)/1000 AS 송장수량_천EA"
        else:
            select_cols = f"{time_col} AS {time_name}, {group_by_sql} {metric_col} AS {metric_name}"
        group_by_clause = f"GROUP BY {time_col}, 파트"
    elif group_option == "카테고리(최종)별":
        group_by_sql = "\"카테고리(최종)\","
        group_col = "카테고리(최종)"
        if is_combined:
            select_cols = f"{time_col} AS {time_name}, {group_by_sql} SUM(송장금액)/1000000 AS 송장금액_백만원, SUM(송장수량)/1000 AS 송장수량_천EA"
        else:
            select_cols = f"{time_col} AS {time_name}, {group_by_sql} {metric_col} AS {metric_name}"
        group_by_clause = f"GROUP BY {time_col}, \"카테고리(최종)\""
    elif group_option == "KPI용카테고리별":
        group_by_sql = "KPI용카테고리,"
        group_col = "KPI용카테고리"
        if is_combined:
            select_cols = f"{time_col} AS {time_name}, {group_by_sql} SUM(송장금액)/1000000 AS 송장금액_백만원, SUM(송장수량)/1000 AS 송장수량_천EA"
        else:
            select_cols = f"{time_col} AS {time_name}, {group_by_sql} {metric_col} AS {metric_name}"
        group_by_clause = f"GROUP BY {time_col}, KPI용카테고리"
    elif group_option == "파트+카테고리(최종)별":
        group_by_sql = "파트, \"카테고리(최종)\","
        group_col = "파트_카테고리"
        if is_combined:
            select_cols = f"{time_col} AS {time_name}, {group_by_sql} SUM(송장금액)/1000000 AS 송장금액_백만원, SUM(송장수량)/1000 AS 송장수량_천EA"
        else:
            select_cols = f"{time_col} AS {time_name}, {group_by_sql} {metric_col} AS {metric_name}"
        group_by_clause = f"GROUP BY {time_col}, 파트, \"카테고리(최종)\""
    else:  # 파트+KPI용카테고리별
        group_by_sql = "파트, KPI용카테고리,"
        group_col = "파트_KPI카테고리"
        if is_combined:
            select_cols = f"{time_col} AS {time_name}, {group_by_sql} SUM(송장금액)/1000000 AS 송장금액_백만원, SUM(송장수량)/1000 AS 송장수량_천EA"
        else:
            select_cols = f"{time_col} AS {time_name}, {group_by_sql} {metric_col} AS {metric_name}"
        group_by_clause = f"GROUP BY {time_col}, 파트, KPI용카테고리"

    # SQL 쿼리 실행 및 디버깅 정보 수집
    sql_query = f"""
        SELECT {select_cols}
        FROM data
        {where_sql}
        {group_by_clause}
        ORDER BY 1, 2{', 3' if group_option in ['플랜트+업체별', '파트+카테고리(최종)별', '파트+KPI용카테고리별'] else ''}
        """
    
    time_df = con.execute(sql_query).fetchdf()
    
    # 디버깅을 위한 집계 정보 저장
    if not time_df.empty:
        debug_aggregation_info = {
            'total_rows': len(time_df),
            'unique_months': time_df[time_name].nunique() if time_name in time_df.columns else 0,
            'date_range': f"{time_df[time_name].min()} ~ {time_df[time_name].max()}" if time_name in time_df.columns else "N/A",
            'sql_query': sql_query,
            'group_option': group_option,
            'time_unit': time_unit
        }
        
        # 그룹별 분석인 경우 그룹 정보도 추가
        if group_option != "전체" and group_col in time_df.columns:
            debug_aggregation_info['unique_groups'] = time_df[group_col].nunique()
            debug_aggregation_info['groups_list'] = time_df[group_col].unique().tolist()[:10]  # 최대 10개만
        else:
            debug_aggregation_info['unique_groups'] = 1  # 전체 분석시
        
        # 차트 데이터 샘플 추가 (X축 중복 분석용)
        sample_cols = [time_name]
        if group_option != "전체" and group_col in time_df.columns:
            sample_cols.append(group_col)
        # 첫 번째 메트릭 컬럼 추가
        if is_combined:
            sample_cols.extend(['송장금액_백만원', '송장수량_천EA'])
        else:
            sample_cols.append(metric_name)
        
        # 상위 10개 행 샘플
        debug_aggregation_info['chart_data_sample'] = time_df[sample_cols].head(10)
        
        st.session_state["debug_aggregation_info"] = debug_aggregation_info

    if time_df.empty:
        st.error("선택한 조건에 해당하는 데이터가 없습니다.")
        st.info("해결 방법:")
        st.write("1. 다른 기간을 선택해보세요")
        st.write("2. 필터 조건을 더 넓히 설정해보세요")
        st.write("3. 송장금액이나 송장수량 데이터가 없을 수 있습니다")
    else:
        # 시간 표시 컬럼 생성 - 중복 방지 개선
        if time_unit == "월별":
            # 날짜 타입 확인 후 처리
            if pd.api.types.is_datetime64_any_dtype(time_df[time_name]):
                time_df["시간표시"] = time_df[time_name].dt.strftime(time_format)
            else:
                # 문자열이나 다른 타입인 경우 날짜로 변환 시도
                try:
                    time_df[time_name] = pd.to_datetime(time_df[time_name])
                    time_df["시간표시"] = time_df[time_name].dt.strftime(time_format)
                except:
                    # 변환 실패시 원본 사용
                    time_df["시간표시"] = time_df[time_name].astype(str)
        else:  # 연도별
            time_df["시간표시"] = time_df[time_name].astype(int).astype(str) + "년"
        
        # 데이터 정렬 및 중복 방지 - 시간 순서로 정렬하여 차트에서 올바른 순서 보장
        sort_columns = [time_name]
        if group_option != "전체" and group_col in time_df.columns:
            sort_columns.append(group_col)
        
        time_df = time_df.sort_values(sort_columns)
        
        # 추가 안전장치: 완전히 동일한 행이 있다면 제거 (GROUP BY가 제대로 작동하지 않은 경우 대비)
        if group_option == "전체":
            # 전체 분석의 경우 같은 시간에 대해서는 1개 행만 있어야 함
            time_df = time_df.drop_duplicates(subset=[time_name], keep='first')
        else:
            # 그룹별 분석의 경우 (시간 + 그룹)에 대해 1개 행만 있어야 함
            if group_col in time_df.columns:
                dedup_columns = [time_name, group_col]
                time_df = time_df.drop_duplicates(subset=dedup_columns, keep='first')
        
        if group_option == "플랜트+업체별":
            time_df["플랜트_업체"] = time_df["플랜트"].astype(str) + "_" + time_df["공급업체명"]
        elif group_option == "파트+카테고리(최종)별":
            time_df["파트_카테고리"] = time_df["파트"].astype(str) + "_" + time_df["카테고리(최종)"]
        elif group_option == "파트+KPI용카테고리별":
            time_df["파트_KPI카테고리"] = time_df["파트"].astype(str) + "_" + time_df["KPI용카테고리"]
        
        # 데이터 테이블 표시
        if is_combined:
            # 복합 차트용 테이블 표시
            if group_option == "전체":
                display_cols = ["시간표시", "송장금액_백만원", "송장수량_천EA"]
            elif group_option in ["플랜트+업체별", "파트+카테고리(최종)별", "파트+KPI용카테고리별"]:
                if group_option == "플랜트+업체별":
                    display_cols = ["시간표시", "플랜트", "공급업체명", "송장금액_백만원", "송장수량_천EA"]
                elif group_option == "파트+카테고리(최종)별":
                    display_cols = ["시간표시", "파트", "카테고리(최종)", "송장금액_백만원", "송장수량_천EA"]
                else:  # 파트+KPI용카테고리별
                    display_cols = ["시간표시", "파트", "KPI용카테고리", "송장금액_백만원", "송장수량_천EA"]
            else:
                display_cols = ["시간표시", group_col, "송장금액_백만원", "송장수량_천EA"]
            
            st.dataframe(
                time_df[display_cols], 
                hide_index=True, 
                use_container_width=True,
                column_config={
                    "송장금액_백만원": st.column_config.NumberColumn(
                        "송장금액(백만원)",
                        format="%.0f"
                    ),
                    "송장수량_천EA": st.column_config.NumberColumn(
                        "송장수량(천EA)",
                        format="%.0f"
                    )
                }
            )
        elif group_option == "전체":
            display_cols = ["시간표시", metric_name]
            st.dataframe(
                time_df[display_cols], 
                hide_index=True, 
                use_container_width=True,
                column_config={
                    metric_name: st.column_config.NumberColumn(
                        metric_name.replace("_", "(").replace("EA", "EA)").replace("원", "원)"),
                        format="%.0f"
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
                        format="%.0f"
                    )
                }
            )
        elif group_option == "파트+카테고리(최종)별":
            display_cols = ["시간표시", "파트", "카테고리(최종)", metric_name]
            st.dataframe(
                time_df[display_cols], 
                hide_index=True, 
                use_container_width=True,
                column_config={
                    metric_name: st.column_config.NumberColumn(
                        metric_name.replace("_", "(").replace("EA", "EA)").replace("원", "원)"),
                        format="%.0f"
                    )
                }
            )
        elif group_option == "파트+KPI용카테고리별":
            display_cols = ["시간표시", "파트", "KPI용카테고리", metric_name]
            st.dataframe(
                time_df[display_cols], 
                hide_index=True, 
                use_container_width=True,
                column_config={
                    metric_name: st.column_config.NumberColumn(
                        metric_name.replace("_", "(").replace("EA", "EA)").replace("원", "원)"),
                        format="%.0f"
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
                        format="%.0f"
                    )
                }
            )

        # 차트 생성 - 클릭 이벤트 추가
        click = alt.selection_point(name="point_select")
        
        # X축 설정 개선 - 중복 방지 및 정렬
        if time_unit == "월별":
            # 월별 차트의 경우 시간을 정확히 처리하고 중복 방지
            unique_months = sorted(time_df[time_name].unique())
            
            x_encoding = alt.X(
                f"{time_name}:T", 
                title=time_unit, 
                axis=alt.Axis(
                    format=time_format, 
                    labelAngle=-45,
                    labelOverlap=False,
                    labelSeparation=15,
                    values=unique_months  # 정확한 월 값들만 표시
                ),
                sort="ascending",
                scale=alt.Scale(
                    type="time",
                    nice=False,
                    domain=unique_months  # 도메인을 정확한 월들로 제한
                )
            )
        else:
            # 연도별의 경우
            unique_years = sorted(time_df[time_name].unique())
            x_encoding = alt.X(
                f"{time_name}:O", 
                title=time_unit,
                sort="ascending",
                scale=alt.Scale(domain=unique_years)  # 도메인 명시적 지정
            )

        # 복합 차트 생성 함수 (이중축)
        def create_combined_chart(data, group_col_name=None):
            base_chart = alt.Chart(data)
            
            # 툴팁 설정
            tooltip_cols = ["시간표시:N", "송장금액_백만원:Q", "송장수량_천EA:Q"]
            if group_col_name:
                tooltip_cols.insert(1, f"{group_col_name}:N")
            
            # 축 범위 계산 - 송장금액 축의 최대값을 130%로 확장하여 레이블 여백 확보
            max_amount = data['송장금액_백만원'].max() if not data.empty else 100
            expanded_max_amount = max_amount * 1.3
            
            # 왼쪽 차트 - 송장금액 막대 차트 (왼쪽 축만 표시)
            left_chart = base_chart.mark_bar(opacity=0.6).encode(
                x=x_encoding,
                y=alt.Y('송장금액_백만원:Q', 
                       title='송장금액(백만원)', 
                       axis=alt.Axis(
                           orient='left', 
                           titleColor='steelblue', 
                           grid=True,
                           labelColor='steelblue',
                           tickColor='steelblue'
                       ),
                       scale=alt.Scale(domain=[0, expanded_max_amount])),
                color=alt.Color(f"{group_col_name}:N", legend=alt.Legend(title=group_col_name)) if group_col_name else alt.value('steelblue'),
                tooltip=tooltip_cols
            )
            
            # 막대 차트 데이터 레이블
            bar_text = base_chart.mark_text(dy=-8, fontSize=9, fontWeight='bold').encode(
                x=x_encoding,
                y=alt.Y('송장금액_백만원:Q', 
                       axis=None,  # 레이블용이므로 축 숨김
                       scale=alt.Scale(domain=[0, expanded_max_amount])),
                text=alt.condition(
                    alt.datum.송장금액_백만원 > 0,
                    alt.Text('송장금액_백만원:Q', format='.0f'),
                    alt.value('')
                ),
                color=alt.Color(f"{group_col_name}:N") if group_col_name else alt.value('black')
            )
            
            # 오른쪽 차트 - 송장수량 꺾은선 차트 (오른쪽 축만 표시)
            right_chart = base_chart.mark_line(point=alt.OverlayMarkDef(size=80), strokeWidth=3).encode(
                x=x_encoding,
                y=alt.Y('송장수량_천EA:Q', 
                       title='송장수량(천EA)', 
                       axis=alt.Axis(
                           orient='right', 
                           titleColor='red', 
                           grid=False,
                           labelColor='red',
                           tickColor='red'
                       )),
                color=alt.Color(f"{group_col_name}:N") if group_col_name else alt.value('red'),
                tooltip=tooltip_cols
            )
            
            # 꺾은선 차트 데이터 레이블
            line_text = base_chart.mark_text(dy=-18, fontSize=8, fontWeight='bold').encode(
                x=x_encoding,
                y=alt.Y('송장수량_천EA:Q', axis=None),  # 레이블용이므로 축 숨김
                text=alt.condition(
                    alt.datum.송장수량_천EA > 0,
                    alt.Text('송장수량_천EA:Q', format='.0f'),
                    alt.value('')
                ),
                color=alt.Color(f"{group_col_name}:N") if group_col_name else alt.value('red')
            )
            
            # 완전한 이중축 차트 - 각 축이 독립적으로 표시
            return alt.layer(
                left_chart,   # 왼쪽 축만 표시되는 막대차트
                right_chart,  # 오른쪽 축만 표시되는 꺾은선차트  
                bar_text,     # 막대차트 레이블
                line_text     # 꺾은선차트 레이블
            ).resolve_scale(y='independent').add_params(click)

        if is_combined:
            # 복합 차트 처리
            if group_option == "전체":
                chart = create_combined_chart(time_df)
            elif group_option in ["플랜트+업체별", "파트+카테고리(최종)별", "파트+KPI용카테고리별"]:
                chart = create_combined_chart(time_df, group_col)
            else:
                chart = create_combined_chart(time_df, group_col)
        elif group_option == "전체":
            base = alt.Chart(time_df)
            line = base.mark_line(point=True).encode(
                x=x_encoding,
                y=alt.Y(f"{metric_name}:Q", title=y_title),
                tooltip=["시간표시:N", f"{metric_name}:Q"]
            )
            text = base.mark_text(dy=-10, fontSize=10, fontWeight='bold').encode(
                x=x_encoding,
                y=alt.Y(f"{metric_name}:Q"),
                text=alt.condition(
                    f"datum.{metric_name} > 0",
                    alt.Text(f"{metric_name}:Q", format='.0f'),
                    alt.value('')
                )
            )
            chart = (line + text).add_params(click)
        elif group_option == "플랜트+업체별":
            base = alt.Chart(time_df)
            line = base.mark_line(point=True).encode(
                x=x_encoding,
                y=alt.Y(f"{metric_name}:Q", title=y_title),
                color=alt.Color("플랜트_업체:N", title="플랜트_업체"),
                tooltip=["시간표시:N", "플랜트:O", "공급업체명:N", f"{metric_name}:Q"]
            )
            text = base.mark_text(dy=-10, fontSize=8, fontWeight='bold').encode(
                x=x_encoding,
                y=alt.Y(f"{metric_name}:Q"),
                text=alt.condition(
                    f"datum.{metric_name} > 0",
                    alt.Text(f"{metric_name}:Q", format='.0f'),
                    alt.value('')
                ),
                color=alt.Color("플랜트_업체:N")
            )
            chart = (line + text).add_params(click)
        elif group_option == "파트+카테고리(최종)별":
            base = alt.Chart(time_df)
            line = base.mark_line(point=True).encode(
                x=x_encoding,
                y=alt.Y(f"{metric_name}:Q", title=y_title),
                color=alt.Color("파트_카테고리:N", title="파트_카테고리"),
                tooltip=["시간표시:N", "파트:N", "카테고리(최종):N", f"{metric_name}:Q"]
            )
            text = base.mark_text(dy=-10, fontSize=8, fontWeight='bold').encode(
                x=x_encoding,
                y=alt.Y(f"{metric_name}:Q"),
                text=alt.condition(
                    f"datum.{metric_name} > 0",
                    alt.Text(f"{metric_name}:Q", format='.0f'),
                    alt.value('')
                ),
                color=alt.Color("파트_카테고리:N")
            )
            chart = (line + text).add_params(click)
        elif group_option == "파트+KPI용카테고리별":
            base = alt.Chart(time_df)
            line = base.mark_line(point=True).encode(
                x=x_encoding,
                y=alt.Y(f"{metric_name}:Q", title=y_title),
                color=alt.Color("파트_KPI카테고리:N", title="파트_KPI카테고리"),
                tooltip=["시간표시:N", "파트:N", "KPI용카테고리:N", f"{metric_name}:Q"]
            )
            text = base.mark_text(dy=-10, fontSize=8, fontWeight='bold').encode(
                x=x_encoding,
                y=alt.Y(f"{metric_name}:Q"),
                text=alt.condition(
                    f"datum.{metric_name} > 0",
                    alt.Text(f"{metric_name}:Q", format='.0f'),
                    alt.value('')
                ),
                color=alt.Color("파트_KPI카테고리:N")
            )
            chart = (line + text).add_params(click)
        else:
            base = alt.Chart(time_df)
            line = base.mark_line(point=True).encode(
                x=x_encoding,
                y=alt.Y(f"{metric_name}:Q", title=y_title),
                color=alt.Color(f"{group_col}:N", title=group_col),
                tooltip=["시간표시:N", f"{group_col}:N", f"{metric_name}:Q"]
            )
            text = base.mark_text(dy=-10, fontSize=8, fontWeight='bold').encode(
                x=x_encoding,
                y=alt.Y(f"{metric_name}:Q"),
                text=alt.condition(
                    f"datum.{metric_name} > 0",
                    alt.Text(f"{metric_name}:Q", format='.0f'),
                    alt.value('')
                ),
                color=alt.Color(f"{group_col}:N")
            )
            chart = (line + text).add_params(click)
        
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
                
                # 새로운 컬럼들을 SELECT 절에 추가
                additional_cols = ""
                if "파트" in df.columns:
                    additional_cols += ", 파트"
                if "카테고리(최종)" in df.columns:
                    additional_cols += ", \"카테고리(최종)\""
                if "KPI용카테고리" in df.columns:
                    additional_cols += ", KPI용카테고리"
                
                raw_data_query = f"""
                SELECT strftime(마감월, '%Y-%m') AS 마감월, 플랜트, 구매그룹,{supplier_code_select}
                       공급업체명{additional_cols}, 자재 AS 자재코드, 자재명,
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
                
                # 새로운 필터 조건들 추가
                if parts_all and sel_parts:
                    additional_filters.append(f"파트 IN ({sql_list_str(sel_parts)})")
                if categories_all and sel_categories:
                    additional_filters.append(f"\"카테고리(최종)\" IN ({sql_list_str(sel_categories)})")
                if kpi_categories_all and sel_kpi_categories:
                    additional_filters.append(f"KPI용카테고리 IN ({sql_list_str(sel_kpi_categories)})")
                
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
                    
                    # 데이터 품질 간단 체크
                    zero_amounts = (raw_df['송장금액'] == 0).sum() if '송장금액' in raw_df.columns else 0
                    zero_quantities = (raw_df['송장수량'] == 0).sum() if '송장수량' in raw_df.columns else 0
                    
                    if zero_amounts > len(raw_df) * 0.3:
                        st.warning(f"주의: 송장금액이 0인 데이터가 {zero_amounts}건 있습니다.")
                    if zero_quantities > len(raw_df) * 0.3:
                        st.warning(f"주의: 송장수량이 0인 데이터가 {zero_quantities}건 있습니다.")
                    
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
                    
                    # 합계 행 추가
                    if not raw_df.empty:
                        # 숫자 컬럼들의 합계/평균 계산
                        totals = {
                            '송장수량': raw_df['송장수량'].sum(),
                            '송장금액': raw_df['송장금액'].sum(),
                            '단가': raw_df['단가'].mean()  # 단가는 평균으로 계산
                        }
                        
                        # 합계 행 생성 - 모든 필수 컬럼 포함
                        total_row_data = {
                            '마감월': '합계',
                            '플랜트': None,
                            '구매그룹': None,
                            '공급업체명': '전체 합계',
                            '자재코드': None,
                            '자재명': '총계',
                            '송장수량': totals['송장수량'],
                            '송장금액': totals['송장금액'],
                            '단가': totals['단가']
                        }
                        
                        # 공급업체코드 컬럼이 있는 경우 추가
                        if "공급업체코드" in raw_df.columns:
                            total_row_data['공급업체코드'] = None
                        
                        # 새로운 컬럼들이 있는 경우 추가
                        if "파트" in raw_df.columns:
                            total_row_data['파트'] = None
                        if "카테고리(최종)" in raw_df.columns:
                            total_row_data['카테고리(최종)'] = None
                        if "KPI용카테고리" in raw_df.columns:
                            total_row_data['KPI용카테고리'] = None
                        
                        total_row = pd.DataFrame([total_row_data])
                        
                        # 원본 데이터와 합계 행 결합
                        raw_df_with_totals = pd.concat([raw_df, total_row], ignore_index=True)
                    else:
                        raw_df_with_totals = raw_df
                    
                    st.dataframe(
                        raw_df_with_totals, 
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
                    st.info("해결 방법:")
                    st.write("1. 다른 기간을 선택해보세요")
                    st.write("2. 선택된 필터 조건을 확인해보세요")
                    st.write("3. 데이터 파일에 해당 기간의 데이터가 있는지 확인해보세요")
        
    st.caption(f"단위: {unit_text}")

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
        
        # 합계 행 추가
        if not sup_df.empty:
            # 숫자 컬럼들의 합계 계산
            totals = {
                '송장수량_천EA': sup_df['송장수량_천EA'].sum(),
                '송장금액_백만원': sup_df['송장금액_백만원'].sum()
            }
            
            # 합계 행 생성
            if "공급업체코드" in sup_df.columns:
                total_row = pd.DataFrame([{
                    '공급업체코드': None,
                    '공급업체명': '합계',
                    '송장수량_천EA': totals['송장수량_천EA'],
                    '송장금액_백만원': totals['송장금액_백만원']
                }])
            else:
                total_row = pd.DataFrame([{
                    '공급업체명': '합계',
                    '송장수량_천EA': totals['송장수량_천EA'],
                    '송장금액_백만원': totals['송장금액_백만원']
                }])
            
            # 원본 데이터와 합계 행 결합
            sup_df_with_totals = pd.concat([sup_df, total_row], ignore_index=True)
        else:
            sup_df_with_totals = sup_df
        
        st.dataframe(
            sup_df_with_totals, 
            hide_index=True, 
            use_container_width=True,
            column_config={
                "송장금액_백만원": st.column_config.NumberColumn(
                    "송장금액(백만원)",
                    format="%.0f"
                ),
                "송장수량_천EA": st.column_config.NumberColumn(
                    "송장수량(천EA)", 
                    format="%.0f"
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
            st.warning("검색 결과가 없습니다.")
            st.info("검색 팁:")
            st.write("1. 와일드카드 '*' 사용: *퍼퓸*1L*")
            st.write("2. 더 짧은 키워드 사용: 퍼퓸 대신 *퍼*")
            st.write("3. 자재코드로도 검색해보세요")
            st.write("4. 현재 선택된 기간과 필터 조건을 확인해보세요")
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
                            format="%.0f"
                        ),
                        "송장수량_천EA": st.column_config.NumberColumn(
                            "송장수량(천EA)", 
                            format="%.0f"
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
                        format="%.0f"
                    ),
                    "송장수량_천EA": st.column_config.NumberColumn(
                        "송장수량(천EA)", 
                        format="%.0f"
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
