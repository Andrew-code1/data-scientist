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
        font-size: 1rem !important;
        padding: 0.2rem 0.4rem !important;
        min-height: 2.2rem !important;
        min-width: 2.2rem !important;
        white-space: nowrap !important;
        overflow: visible !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        line-height: 1 !important;
    }

    /* 세션 타이머 스타일 */
    .session-timer {
        position: fixed;
        top: 10px;
        left: 20px;
        z-index: 9999;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 12px 20px;
        border-radius: 25px;
        font-family: 'Courier New', monospace;
        font-size: 16px;
        font-weight: bold;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
        display: flex;
        align-items: center;
        gap: 8px;
        transition: all 0.3s ease;
    }

    .session-timer:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.3);
    }

    .session-timer.warning {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        animation: pulse 1s infinite;
    }

    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.8; }
    }

    .timer-icon {
        font-size: 18px;
    }

    .timer-text {
        font-size: 14px;
        opacity: 0.9;
        margin-right: 5px;
    }
</style>

<div class="session-timer" id="sessionTimer">
    <span class="timer-icon">⏱️</span>
    <span class="timer-text">세션:</span>
    <span id="timerDisplay">30:00</span>
</div>

<script>
    // 세션 타이머 설정 (분 단위)
    const SESSION_DURATION = 30; // 30분
    const WARNING_TIME = 5; // 5분 남았을 때 경고

    // 타이머 시작 시간 저장 (localStorage 사용)
    const TIMER_KEY = 'sessionTimerStart';

    function initTimer() {
        let startTime = localStorage.getItem(TIMER_KEY);

        if (!startTime) {
            startTime = Date.now();
            localStorage.setItem(TIMER_KEY, startTime);
        }

        updateTimer(parseInt(startTime));
    }

    function updateTimer(startTime) {
        const timerDisplay = document.getElementById('timerDisplay');
        const timerContainer = document.getElementById('sessionTimer');

        if (!timerDisplay || !timerContainer) {
            setTimeout(() => updateTimer(startTime), 100);
            return;
        }

        function update() {
            const elapsed = Math.floor((Date.now() - startTime) / 1000);
            const remaining = Math.max(0, SESSION_DURATION * 60 - elapsed);

            const minutes = Math.floor(remaining / 60);
            const seconds = remaining % 60;

            timerDisplay.textContent =
                String(minutes).padStart(2, '0') + ':' +
                String(seconds).padStart(2, '0');

            // 경고 상태 표시
            if (minutes < WARNING_TIME) {
                timerContainer.classList.add('warning');
            } else {
                timerContainer.classList.remove('warning');
            }

            // 시간 종료
            if (remaining === 0) {
                timerDisplay.textContent = '00:00';
                alert('세션 시간이 종료되었습니다. 페이지를 새로고침 해주세요.');
                localStorage.removeItem(TIMER_KEY);
                return;
            }

            setTimeout(update, 1000);
        }

        update();
    }

    // 페이지 로드 시 타이머 시작
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initTimer);
    } else {
        initTimer();
    }

    // 타이머 리셋 함수 (사용자 활동 감지 시)
    function resetTimer() {
        localStorage.setItem(TIMER_KEY, Date.now());
    }

    // 사용자 활동 감지
    ['click', 'scroll', 'keypress', 'mousemove'].forEach(event => {
        document.addEventListener(event, () => {
            // 5분마다만 리셋 (너무 자주 리셋되는 것 방지)
            const lastReset = parseInt(localStorage.getItem('lastResetTime') || '0');
            if (Date.now() - lastReset > 5 * 60 * 1000) {
                localStorage.setItem('lastResetTime', Date.now());
                // 활동이 있으면 타이머를 연장하지 않고 계속 카운트다운
                // 필요시 resetTimer() 호출하여 타이머 리셋 가능
            }
        }, { passive: true });
    });
</script>
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
        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if "공급업체명" in df.columns:
        df["공급업체명"] = df["공급업체명"].astype(str).str.strip()
    if "공급업체코드" in df.columns:
        # 공급업체코드 안전하게 처리 - 문자열 기반으로 소수점만 제거
        def clean_supplier_code(x):
            if pd.isna(x) or str(x).lower() in ['nan', 'none', ''] or str(x).strip() == '':
                return ""
            
            str_val = str(x).strip()
            
            # .0으로 끝나는 경우만 제거 (예: "123.0" -> "123")
            if str_val.endswith('.0'):
                return str_val[:-2]
            # .00으로 끝나는 경우도 제거 (예: "123.00" -> "123") 
            elif str_val.endswith('.00'):
                return str_val[:-3]
            # 그 외에는 원본 유지
            else:
                return str_val
        
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


def enhance_pattern(pattern: str) -> str:
    """자재 검색 패턴 강화 함수"""
    if "*" not in pattern:
        if " " in pattern:
            # 띄어쓰기가 있으면 각 단어에 와일드카드 적용
            words = pattern.split()
            pattern = "*" + "*".join(words) + "*"
        else:
            # 단일 단어도 양쪽에 와일드카드 추가
            pattern = "*" + pattern + "*"
    return pattern.replace("*", "%").replace("'", "''")



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
        st.button("☑", on_click=_set_all, args=(ms_key, options), key=f"{key_prefix}_all", help="전체 선택")
    return sel

with st.sidebar:
    st.header("CSV 업로드")
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
    # 전역 자재 검색을 위한 session_state 초기화
    if 'global_material_name_search' not in st.session_state:
        st.session_state.global_material_name_search = ""
    if 'global_material_code_search' not in st.session_state:
        st.session_state.global_material_code_search = ""
    
    
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
        
        # 필터 초기화 버튼
        if st.button("🗑️ 모든 필터 초기화", key="clear_all_filters"):
            # 세션 상태 초기화 (자재 검색 제외, 하단에서 관리)
            for key in list(st.session_state.keys()):
                if key.endswith("_ms"):
                    del st.session_state[key]
            st.rerun()

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
    
    # 자재 검색 조건 추가 (하단 검색과 전역 연동) - 다중 필터 지원
    material_search_conditions = []
    material_name_search = st.session_state.global_material_name_search
    material_code_search = st.session_state.global_material_code_search
    
    # 자재명 다중 검색 처리 (OR 조건)
    if material_name_search and material_name_search.strip():
        name_patterns = []
        # 쉼표, 개행, 세미콜론으로 분리하여 다중 검색어 처리
        name_terms = [term.strip() for term in material_name_search.replace('\n', ',').replace(';', ',').split(',') if term.strip()]
        for term in name_terms:
            enhanced_name_patt = enhance_pattern(term)
            name_patterns.append(f"자재명 ILIKE '{enhanced_name_patt}'")
        
        if name_patterns:
            name_clause = " OR ".join(name_patterns)
            material_search_conditions.append(f"({name_clause})")
    
    # 자재코드 다중 검색 처리 (OR 조건, 엑셀 복사 지원)
    if material_code_search and material_code_search.strip():
        code_patterns = []
        # 쉼표, 개행, 탭, 세미콜론으로 분리하여 다중 검색어 처리 (엑셀 복사 대응)
        code_terms = [term.strip() for term in material_code_search.replace('\n', ',').replace('\t', ',').replace(';', ',').split(',') if term.strip()]
        for term in code_terms:
            # 자재명과 동일한 로직: 모든 경우에 enhance_pattern 적용 (와일드카드 자동 추가)
            enhanced_code_patt = enhance_pattern(term)
            code_patterns.append(f"CAST(자재 AS VARCHAR) ILIKE '{enhanced_code_patt}'")
        
        if code_patterns:
            code_clause = " OR ".join(code_patterns)
            material_search_conditions.append(f"({code_clause})")
    
    if material_search_conditions:
        # 자재명과 자재코드 검색 조건을 AND로 연결 (둘 다 입력된 경우)
        material_clause = " AND ".join(material_search_conditions)
        clauses.append(f"({material_clause})")

    where_sql = " WHERE " + " AND ".join(clauses)

    st.title("구매 데이터 추이 분석")
    
    # 활성화된 필터 조건 표시
    active_filters = []
    
    # 기간 필터
    if len(sel_yearmonths) < len(yearmonths_all):
        period_text = f"{min(sel_yearmonths)}~{max(sel_yearmonths)}" if len(sel_yearmonths) > 1 else sel_yearmonths[0]
        active_filters.append(f"📅 기간: {period_text}")
    
    # 기본 필터들
    if sel_plants and len(sel_plants) < len(plants_all):
        plant_text = ", ".join(map(str, sel_plants[:3]))
        if len(sel_plants) > 3:
            plant_text += f" 외 {len(sel_plants)-3}개"
        active_filters.append(f"플랜트: {plant_text}")
    
    if sel_groups and len(sel_groups) < len(groups_all):
        group_text = ", ".join(map(str, sel_groups[:3]))
        if len(sel_groups) > 3:
            group_text += f" 외 {len(sel_groups)-3}개"
        active_filters.append(f"🔧 구매그룹: {group_text}")
    
    if sel_suppliers and len(sel_suppliers) < len(suppliers_all):
        supplier_text = ", ".join([s.split("_", 1)[1] if "_" in s else s for s in sel_suppliers[:2]])
        if len(sel_suppliers) > 2:
            supplier_text += f" 외 {len(sel_suppliers)-2}개"
        active_filters.append(f"🏢 공급업체: {supplier_text}")
    
    # 자재 검색 필터 (하단 검색과 연동) - 다중 검색 표시
    if material_name_search and material_name_search.strip():
        name_terms = [term.strip() for term in material_name_search.replace('\n', ',').replace(';', ',').split(',') if term.strip()]
        if len(name_terms) > 1:
            active_filters.append(f"🔍 자재명: {len(name_terms)}개 조건")
        else:
            active_filters.append(f"🔍 자재명: {name_terms[0]}")
    if material_code_search and material_code_search.strip():
        code_terms = [term.strip() for term in material_code_search.replace('\n', ',').replace('\t', ',').replace(';', ',').split(',') if term.strip()]
        if len(code_terms) > 1:
            active_filters.append(f"📊 자재코드: {len(code_terms)}개 조건")
        else:
            active_filters.append(f"📊 자재코드: {code_terms[0]}")
    
    # 활성 필터 표시
    if active_filters:
        st.info(f"**활성 필터**: {' | '.join(active_filters)}")
        if len(active_filters) > 1:
            st.caption("여러 필터가 동시에 적용되어 데이터가 교집합으로 필터링됩니다.")
        
        # 필터링된 데이터 요약 정보 추가
        if material_name_search or material_code_search:
            st.success("**자재 검색 필터가 전체 대시보드에 적용되었습니다!**")
            st.caption("구매 데이터 추이, Raw 데이터 조회, 업체별 구매 현황이 모두 검색된 자재로 필터링됩니다.")
    else:
        st.info("**전체 데이터** 표시 중 (필터 없음)")
    
    
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

    # SQL 쿼리 실행 및 디버깅 정보 수집
    sql_query = f"""
        SELECT {select_cols}
        FROM data
        {where_sql}
        {group_by_clause}
        ORDER BY 1, 2{', 3' if group_option == '플랜트+업체별' else ''}
        """
    
    time_df = con.execute(sql_query).fetchdf()
    

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
        
        # 데이터 테이블 표시
        if is_combined:
            # 복합 차트용 테이블 표시
            if group_option == "전체":
                display_cols = ["시간표시", "송장금액_백만원", "송장수량_천EA"]
            elif group_option == "플랜트+업체별":
                display_cols = ["시간표시", "플랜트", "공급업체명", "송장금액_백만원", "송장수량_천EA"]
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
                    values=unique_months,  # 정확한 월 값들만 표시
                    offset=10  # X축을 아래로 이동하여 Y축과 거리 확보
                ),
                sort="ascending",
                scale=alt.Scale(
                    type="time",
                    nice=False,
                    domain=unique_months,  # 도메인을 정확한 월들로 제한
                    padding=0.2,  # X축 양쪽 여백을 20%로 증가
                    range=[50, {"expr": "width-50"}]  # 실제 차트 영역을 왼쪽 50px, 오른쪽 50px 안쪽으로 제한
                )
            )
        else:
            # 연도별의 경우
            unique_years = sorted(time_df[time_name].unique())
            x_encoding = alt.X(
                f"{time_name}:O", 
                title=time_unit,
                axis=alt.Axis(offset=10),  # X축을 아래로 이동
                sort="ascending",
                scale=alt.Scale(
                    domain=unique_years,  # 도메인 명시적 지정
                    padding=0.2,  # X축 양쪽 여백을 20%로 증가
                    range=[50, {"expr": "width-50"}]  # 실제 차트 영역을 왼쪽 50px, 오른쪽 50px 안쪽으로 제한
                )
            )

        # 복합 차트 생성 함수 (이중축) - 누적막대 + 심미적 개선
        def create_combined_chart(data, group_col_name=None):
            # 데이터 포인트 수에 따른 동적 막대 두께 계산
            data_points = len(data[time_name].unique()) if not data.empty else 1
            # 2개월이면 두껍게, 12개월이면 적당하게
            bar_size = max(15, min(60, 120 - data_points * 5))
            
            # 차트 속성 정의 - padding은 LayerChart에서 적용
            chart_props = {
                "height": 600,  # 고정 높이
                "width": max(400, data_points * 80)  # 최소 400px, 데이터 포인트당 80px
            }
            
            # 툴팁 설정
            tooltip_cols = ["시간표시:N", "송장금액_백만원:Q", "송장수량_천EA:Q"]
            if group_col_name:
                tooltip_cols.insert(1, f"{group_col_name}:N")
            
            # **누적 막대를 위한 축 범위 계산 개선**
            if group_col_name:
                # 그룹별 데이터인 경우 시간별 누적값 계산
                stacked_amounts = data.groupby(time_name)['송장금액_백만원'].sum()
                max_stacked_amount = stacked_amounts.max() if not stacked_amounts.empty else 100
            else:
                # 전체 데이터인 경우
                max_stacked_amount = data['송장금액_백만원'].max() if not data.empty else 100
            
            # 송장수량 범위 계산 (꺾은선을 누적막대 상단에 배치) - 개선된 축 설정
            non_zero_quantities = data[data['송장수량_천EA'] > 0]['송장수량_천EA']
            if not non_zero_quantities.empty:
                max_quantity = non_zero_quantities.max()
                # 최댓값을 10단위로 반올림 (깔끔한 축 표시)
                import math
                max_quantity_rounded = math.ceil(max_quantity / 10) * 10
                
                # 누적막대 최대값의 120% 지점을 꺾은선 시작점으로 설정
                line_start_point = max_stacked_amount * 1.2
                # 송장수량의 전체 범위를 상단 영역에 배치
                line_height = max_stacked_amount * 0.6  # 누적막대 높이의 60%를 꺾은선 영역으로
                min_quantity = 0  # 최솟값을 0으로 고정
                
                # 0부터 반올림된 최댓값까지의 범위를 line_height에 매핑
                expanded_max_quantity = line_start_point + line_height
                
                # 데이터 변환을 위한 스케일링 팩터 계산 (0~max_quantity_rounded를 line_start_point~expanded_max_quantity로 변환)
                if max_quantity_rounded > 0:
                    quantity_scale_factor = line_height / max_quantity_rounded
                    quantity_offset = line_start_point
                else:
                    quantity_scale_factor = 1
                    quantity_offset = line_start_point
            else:
                max_quantity_rounded = 50
                line_start_point = max_stacked_amount * 1.2
                min_quantity = 0  # 최솟값을 0으로 고정
                line_height = max_stacked_amount * 0.6
                expanded_max_quantity = line_start_point + line_height
                quantity_scale_factor = line_height / max_quantity_rounded
                quantity_offset = line_start_point
                
            # 송장금액 범위는 누적값 기준으로 설정
            expanded_max_amount = max_stacked_amount * 1.5  # 20% 여유공간
            
            # 송장수량 데이터를 상단 영역으로 변환
            data = data.copy()
            data['송장수량_변환'] = data['송장수량_천EA'] * quantity_scale_factor + quantity_offset
            
            # **누적 막대차트** - 왼쪽 축만 표시
            if group_col_name:
                # 그룹별 누적 막대차트
                left_chart = alt.Chart(data).mark_bar(opacity=0.8, size=bar_size).encode(
                    x=x_encoding,
                    y=alt.Y('송장금액_백만원:Q', 
                           title='송장금액(백만원)', 
                           axis=alt.Axis(
                               orient='left', 
                               titleColor='steelblue', 
                               grid=True,
                               labelColor='steelblue',
                               tickColor='steelblue',
                               labelPadding=15,
                               titlePadding=20,
                               offset=5
                           ),
                           scale=alt.Scale(domain=[0, expanded_max_amount]),
                           stack='zero'),  # **누적 설정**
                    color=alt.Color(f"{group_col_name}:N", 
                                   legend=alt.Legend(title=group_col_name, orient='right')),
                    tooltip=tooltip_cols,
                    order=alt.Order(f"{group_col_name}:N", sort='ascending')  # 누적 순서 일관성
                ).properties(**chart_props)
            else:
                # 전체 데이터 막대차트 (누적 없음)
                left_chart = alt.Chart(data).mark_bar(opacity=0.7, size=bar_size).encode(
                    x=x_encoding,
                    y=alt.Y('송장금액_백만원:Q', 
                           title='송장금액(백만원)', 
                           axis=alt.Axis(
                               orient='left', 
                               titleColor='steelblue', 
                               grid=True,
                               labelColor='steelblue',
                               tickColor='steelblue',
                               labelPadding=15,
                               titlePadding=20,
                               offset=5
                           ),
                           scale=alt.Scale(domain=[0, expanded_max_amount])),
                    color=alt.value('steelblue'),
                    tooltip=tooltip_cols
                ).properties(**chart_props)
            
            # **꺾은선 차트** - 오른쪽 축만 표시, 확장된 Y축 범위
            if group_col_name:
                # 그룹별 꺾은선차트
                right_chart = alt.Chart(data).mark_line(
                    point=alt.OverlayMarkDef(size=100, filled=True), 
                    strokeWidth=4
                ).encode(
                    x=x_encoding,
                    y=alt.Y('송장수량_변환:Q', 
                           title='송장수량(천EA)', 
                           axis=alt.Axis(
                               orient='right', 
                               titleColor='red', 
                               grid=False,
                               labelColor='red',
                               tickColor='red',
                               labelPadding=15,
                               titlePadding=20,
                               offset=5,
                               labelExpr=f'max(0, round((datum.value - {quantity_offset}) / {quantity_scale_factor}))'
                           ),
                           # **상단 영역으로 변환된 데이터 범위**
                           scale=alt.Scale(domain=[min_quantity, expanded_max_quantity])),
                    color=alt.Color(f"{group_col_name}:N"),
                    tooltip=tooltip_cols
                ).properties(**chart_props)
            else:
                # 전체 데이터 꺾은선차트
                right_chart = alt.Chart(data).mark_line(
                    point=alt.OverlayMarkDef(size=100, filled=True), 
                    strokeWidth=4
                ).encode(
                    x=x_encoding,
                    y=alt.Y('송장수량_변환:Q', 
                           title='송장수량(천EA)', 
                           axis=alt.Axis(
                               orient='right', 
                               titleColor='red', 
                               grid=False,
                               labelColor='red',
                               tickColor='red',
                               labelPadding=15,
                               titlePadding=20,
                               offset=5,
                               labelExpr=f'max(0, round((datum.value - {quantity_offset}) / {quantity_scale_factor}))'
                           ),
                           # **상단 영역으로 변환된 데이터 범위**
                           scale=alt.Scale(domain=[min_quantity, expanded_max_quantity])),
                    color=alt.value('red'),
                    tooltip=tooltip_cols
                ).properties(**chart_props)
            
            # **데이터 레이블 개선**
            if group_col_name:
                # 누적 막대의 각 세그먼트에 레이블 표시 - 정확한 중점 계산
                # 먼저 누적 데이터의 중점을 계산하기 위해 데이터를 변환
                segment_data = data.copy()
                segment_data = segment_data.sort_values([time_name, group_col_name])
                
                # 각 시점별로 누적 값 계산
                cumulative_data = []
                for time_val in segment_data[time_name].unique():
                    time_group = segment_data[segment_data[time_name] == time_val]
                    cumsum = 0
                    for _, row in time_group.iterrows():
                        start_y = cumsum
                        end_y = cumsum + row['송장금액_백만원']
                        mid_y = (start_y + end_y) / 2  # 중점 계산
                        
                        cumulative_data.append({
                            time_name: time_val,
                            group_col_name: row[group_col_name],
                            '송장금액_백만원': row['송장금액_백만원'],
                            'mid_y': mid_y  # 중점 위치
                        })
                        cumsum = end_y
                
                # 중점 데이터를 DataFrame으로 변환
                mid_point_df = pd.DataFrame(cumulative_data)
                
                segment_text = alt.Chart(mid_point_df).mark_text(
                    dy=0, fontSize=9, fontWeight='bold', color='white'
                ).encode(
                    x=x_encoding,
                    y=alt.Y('mid_y:Q', 
                           axis=None,
                           scale=alt.Scale(domain=[0, expanded_max_amount])),
                    text=alt.condition(
                        alt.datum.송장금액_백만원 >= 20,  # 20 이상인 경우만 표시 (가독성 개선)
                        alt.Text('송장금액_백만원:Q', format='.0f'),
                        alt.value('')
                    ),
                    order=alt.Order(f"{group_col_name}:N", sort='ascending')
                ).properties(**chart_props)
                
                # 전체 누적값도 상단에 표시
                stacked_totals = data.groupby(time_name)['송장금액_백만원'].sum().reset_index()
                stacked_totals[time_name] = pd.to_datetime(stacked_totals[time_name]) if time_unit == "월별" else stacked_totals[time_name]
                
                bar_text = alt.Chart(stacked_totals).mark_text(
                    dy=-8, fontSize=10, fontWeight='bold', color='steelblue'
                ).encode(
                    x=x_encoding.copy(),
                    y=alt.Y('송장금액_백만원:Q', 
                           axis=None,
                           scale=alt.Scale(domain=[0, expanded_max_amount])),
                    text=alt.condition(
                        alt.datum.송장금액_백만원 > 0,
                        alt.Text('송장금액_백만원:Q', format='.0f'),
                        alt.value('')
                    )
                ).properties(**chart_props)
            else:
                # 전체 데이터 막대 레이블
                bar_text = alt.Chart(data).mark_text(dy=-8, fontSize=10, fontWeight='bold').encode(
                    x=x_encoding,
                    y=alt.Y('송장금액_백만원:Q', 
                           axis=None,
                           scale=alt.Scale(domain=[0, expanded_max_amount])),
                    text=alt.condition(
                        alt.datum.송장금액_백만원 > 0,
                        alt.Text('송장금액_백만원:Q', format='.0f'),
                        alt.value('')
                    ),
                    color=alt.value('black')
                ).properties(**chart_props)
            
            # 꺾은선 차트 데이터 레이블 - 개선된 위치
            if group_col_name:
                line_text = alt.Chart(data).mark_text(
                    dy=-15, fontSize=9, fontWeight='bold'
                ).encode(
                    x=x_encoding,
                    y=alt.Y('송장수량_변환:Q', 
                           axis=None,
                           scale=alt.Scale(domain=[min_quantity, expanded_max_quantity])),
                    text=alt.condition(
                        alt.datum.송장수량_천EA > 0,
                        alt.Text('송장수량_천EA:Q', format='.0f'),
                        alt.value('')
                    ),
                    color=alt.Color(f"{group_col_name}:N")
                ).properties(**chart_props)
            else:
                line_text = alt.Chart(data).mark_text(
                    dy=-15, fontSize=9, fontWeight='bold'
                ).encode(
                    x=x_encoding,
                    y=alt.Y('송장수량_변환:Q', 
                           axis=None,
                           scale=alt.Scale(domain=[min_quantity, expanded_max_quantity])),
                    text=alt.condition(
                        alt.datum.송장수량_천EA > 0,
                        alt.Text('송장수량_천EA:Q', format='.0f'),
                        alt.value('')
                    ),
                    color=alt.value('red')
                ).properties(**chart_props)
            
            # **완전한 이중축 차트 - 각 축이 독립적으로 표시**
            if group_col_name:
                combined_chart = alt.layer(
                    left_chart,    # 누적 막대차트 (왼쪽 축)
                    right_chart,   # 꺾은선차트 (오른쪽 축, 확장된 범위)
                    segment_text,  # 누적 막대 세그먼트 레이블
                    bar_text,      # 막대차트 총합 레이블
                    line_text      # 꺾은선차트 레이블
                ).resolve_scale(y='independent').properties(
                    title=f"구매 데이터 추이 - {unit_text}",
                    padding={"left": 100, "top": 40, "right": 100, "bottom": 50}
                )
            else:
                combined_chart = alt.layer(
                    left_chart,   # 일반 막대차트 (왼쪽 축)
                    right_chart,  # 꺾은선차트 (오른쪽 축, 확장된 범위)
                    bar_text,     # 막대차트 레이블
                    line_text     # 꺾은선차트 레이블
                ).resolve_scale(y='independent').properties(
                    title=f"구매 데이터 추이 - {unit_text}",
                    padding={"left": 100, "top": 40, "right": 100, "bottom": 50}
                )
            
            return combined_chart.add_params(click)

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
            line = base.mark_line(point=alt.OverlayMarkDef(size=100)).encode(
                x=x_encoding,
                y=alt.Y(f"{metric_name}:Q", title=y_title),
                tooltip=["시간표시:N", f"{metric_name}:Q"]
            )
            text = base.mark_text(dy=-15, fontSize=11, fontWeight='bold', color='darkblue').encode(
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
            text = base.mark_text(dy=-15, fontSize=9, fontWeight='bold').encode(
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
        else:
            base = alt.Chart(time_df)
            line = base.mark_line(point=True).encode(
                x=x_encoding,
                y=alt.Y(f"{metric_name}:Q", title=y_title),
                color=alt.Color(f"{group_col}:N", title=group_col),
                tooltip=["시간표시:N", f"{group_col}:N", f"{metric_name}:Q"]
            )
            text = base.mark_text(dy=-15, fontSize=9, fontWeight='bold').encode(
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
        except Exception:
            pass

        # Raw 데이터 조회 섹션
        st.markdown("---")
        st.subheader("상세 Raw 데이터 조회")
        
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
                
                # 기본 쿼리 - 정밀도 보존을 위해 문자열 그대로 사용
                supplier_code_select = ""
                if "공급업체코드" in df.columns:
                    supplier_code_select = """
                       CASE 
                           WHEN 공급업체코드 = '' OR 공급업체코드 IS NULL THEN NULL
                           ELSE 공급업체코드
                       END AS 공급업체코드,
                    """
                
                # 새로운 컬럼들을 SELECT 절에 추가
                additional_cols = ""
                
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
                    
                    # 전체 데이터 요약 정보 표시
                    total_amount = raw_df['송장금액'].sum()
                    total_quantity = raw_df['송장수량'].sum()
                    total_materials = len(raw_df)
                    
                    if len(query_yearmonths) > 1:
                        # 특정 기간: 월별 누계 현황
                        summary_df = raw_df.groupby('마감월').agg({
                            '송장금액': 'sum',
                            '송장수량': 'sum',
                            '자재코드': 'count'
                        }).reset_index()
                        summary_df.columns = ['연월', '송장금액', '송장수량', '자재건수']
                        
                        st.subheader("월별 누계 현황")
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("총 송장금액", f"{total_amount:,.0f}원")
                        with col2:
                            st.metric("총 송장수량", f"{total_quantity:,.0f}")
                        with col3:
                            st.metric("총 자재건수", f"{total_materials:,.0f}건")
                        
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
                    else:
                        # 특정 시점: 데이터 요약
                        st.subheader("데이터 요약")
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("총 송장금액", f"{total_amount:,.0f}원")
                        with col2:
                            st.metric("총 송장수량", f"{total_quantity:,.0f}")
                        with col3:
                            st.metric("총 자재건수", f"{total_materials:,.0f}건")
                    
                    st.subheader("상세 Raw 데이터")
                    
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
                       ELSE 공급업체코드
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
    st.header("자재 검색 (다중 필터 지원)")
    
    # 전역 연동 안내
    st.info("**여기서 입력한 검색 조건이 위의 모든 차트와 분석에 자동 적용됩니다!**")
    
    
    col1, col2, col3 = st.columns([4, 4, 2])
    with col1:
        material_name_patt = st.text_area(
            "자재명 다중 검색", 
            placeholder="예시:\n*퍼퓸*, *로션*\n또는\n*퍼퓸*\n*로션*\n*크림*",
            value=st.session_state.global_material_name_search,
            key="material_name_input",
            height=100
        )
    with col2:
        material_code_patt = st.text_area(
            "자재코드 다중 검색", 
            placeholder="예시:\n1234567, 2345678\n또는 엑셀 복사 붙여넣기",
            value=st.session_state.global_material_code_search,
            key="material_code_input",
            height=100
        )
    with col3:
        st.write("")  # 여백
        if st.button("🗑️ 자재 검색 초기화", key="clear_material_search"):
            st.session_state.global_material_name_search = ""
            st.session_state.global_material_code_search = ""
            # widget 값 직접 초기화
            st.session_state.material_name_input = ""
            st.session_state.material_code_input = ""
            st.rerun()

    # session_state 업데이트
    if material_name_patt != st.session_state.global_material_name_search:
        st.session_state.global_material_name_search = material_name_patt
        st.rerun()
    if material_code_patt != st.session_state.global_material_code_search:
        st.session_state.global_material_code_search = material_code_patt
        st.rerun()
    
    # 검색 활성화 상태 표시 - 다중 검색 정보 표시
    if material_name_patt or material_code_patt:
        st.success("**자재 검색이 활성화되었습니다!** 위의 모든 분석이 이 조건으로 필터링됩니다.")
        search_info = []
        
        if material_name_patt:
            name_terms = [term.strip() for term in material_name_patt.replace('\n', ',').replace(';', ',').split(',') if term.strip()]
            if len(name_terms) > 1:
                search_info.append(f"자재명: {len(name_terms)}개 조건 (OR)")
            else:
                search_info.append(f"자재명: {name_terms[0]}")
                
        if material_code_patt:
            code_terms = [term.strip() for term in material_code_patt.replace('\n', ',').replace('\t', ',').replace(';', ',').split(',') if term.strip()]
            if len(code_terms) > 1:
                search_info.append(f"자재코드: {len(code_terms)}개 조건 (OR)")
            else:
                search_info.append(f"자재코드: {code_terms[0]}")
        
        st.caption(f"적용된 검색 조건: {' | '.join(search_info)}")
    else:
        st.info("검색 조건을 입력하면 전체 대시보드가 해당 자재로 필터링됩니다.")


    # 검색 조건 생성 - 다중 검색 지원
    search_conditions = []
    search_info = []
    search_where = ""  # 자재 검색 조건 (전월대비 분석 등에서도 사용)

    # 자재명 다중 검색 처리 (OR 조건)
    if material_name_patt:
        name_patterns = []
        name_terms = [term.strip() for term in material_name_patt.replace('\n', ',').replace(';', ',').split(',') if term.strip()]
        for term in name_terms:
            enhanced_name_patt = enhance_pattern(term)
            name_patterns.append(f"자재명 ILIKE '{enhanced_name_patt}'")

        if name_patterns:
            name_clause = " OR ".join(name_patterns)
            search_conditions.append(f"({name_clause})")
            if len(name_terms) > 1:
                search_info.append(f"자재명: {len(name_terms)}개 조건")
            else:
                search_info.append(f"자재명: {name_terms[0]}")

    # 자재코드 다중 검색 처리 (OR 조건, 엑셀 복사 지원)
    if material_code_patt:
        code_patterns = []
        code_terms = [term.strip() for term in material_code_patt.replace('\n', ',').replace('\t', ',').replace(';', ',').split(',') if term.strip()]
        for term in code_terms:
            # 자재명과 동일한 로직: 모든 경우에 enhance_pattern 적용 (와일드카드 자동 추가)
            enhanced_code_patt = enhance_pattern(term)
            code_patterns.append(f"CAST(자재 AS VARCHAR) ILIKE '{enhanced_code_patt}'")

        if code_patterns:
            code_clause = " OR ".join(code_patterns)
            search_conditions.append(f"({code_clause})")
            if len(code_terms) > 1:
                search_info.append(f"자재코드: {len(code_terms)}개 조건")
            else:
                search_info.append(f"자재코드: {code_terms[0]}")

    if search_conditions:
        # AND 조건으로 검색 (둘 다 입력된 경우) 또는 개별 조건
        search_where = " AND ".join(search_conditions)
        
        # 자재 검색 쿼리 - 정밀도 보존을 위해 문자열 그대로 사용
        search_supplier_code_select = ""
        if "공급업체코드" in df.columns:
            search_supplier_code_select = """
                   CASE 
                       WHEN 공급업체코드 = '' OR 공급업체코드 IS NULL THEN NULL
                       ELSE 공급업체코드
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
                
                st.subheader("검색결과 월별 요약")
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
            
            st.subheader("검색결과 상세")
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


    # 전월대비 차이액 분석 섹션
    st.markdown("---")
    st.header("전월대비 차이액 분석")

    # 자재 검색 조건과 기본 필터 조건 결합
    where_sql_with_search = where_sql
    if search_where:
        if where_sql.strip() == "":
            where_sql_with_search = f"WHERE ({search_where})"
        else:
            where_sql_with_search = f"{where_sql} AND ({search_where})"
        st.info(f"자재 검색 조건이 적용되었습니다: {' | '.join(search_info)}")

    with st.expander("전월대비 증감 분석", expanded=True):
        # 월별 데이터만 지원 (연도별은 의미가 없으므로)
        if time_unit == "월별":
            # 분석 옵션 선택
            mom_group_option = st.radio(
                "분석 단위",
                ["전체", "업체별"],
                horizontal=True,
                key="mom_group_option"
            )

            if mom_group_option == "전체":
                # 전체 데이터 월별 집계
                mom_sql = f"""
                    WITH monthly_data AS (
                        SELECT
                            date_trunc('month', 마감월) AS 연월,
                            SUM(송장금액)/1000000 AS 송장금액_백만원,
                            SUM(송장수량)/1000 AS 송장수량_천EA
                        FROM data
                        {where_sql_with_search}
                        GROUP BY date_trunc('month', 마감월)
                    ),
                    with_prev AS (
                        SELECT
                            연월,
                            송장금액_백만원,
                            송장수량_천EA,
                            LAG(송장금액_백만원) OVER (ORDER BY 연월) AS 전월금액,
                            LAG(송장수량_천EA) OVER (ORDER BY 연월) AS 전월수량
                        FROM monthly_data
                    )
                    SELECT
                        연월,
                        송장금액_백만원 AS 당월금액,
                        전월금액,
                        송장금액_백만원 - 전월금액 AS 금액차이,
                        CASE
                            WHEN 전월금액 > 0 THEN ROUND(((송장금액_백만원 - 전월금액) / 전월금액 * 100), 1)
                            ELSE NULL
                        END AS 금액증감률,
                        송장수량_천EA AS 당월수량,
                        전월수량,
                        송장수량_천EA - 전월수량 AS 수량차이,
                        CASE
                            WHEN 전월수량 > 0 THEN ROUND(((송장수량_천EA - 전월수량) / 전월수량 * 100), 1)
                            ELSE NULL
                        END AS 수량증감률
                    FROM with_prev
                    WHERE 전월금액 IS NOT NULL
                    ORDER BY 연월
                """

                mom_df = con.execute(mom_sql).fetchdf()

                if not mom_df.empty:
                    # 날짜 포맷 변환
                    mom_df['연월표시'] = pd.to_datetime(mom_df['연월']).dt.strftime('%Y년%m월')

                    # 데이터 표시
                    display_df = mom_df[['연월표시', '당월금액', '전월금액', '금액차이', '금액증감률',
                                         '당월수량', '전월수량', '수량차이', '수량증감률']].copy()

                    st.dataframe(
                        display_df,
                        hide_index=True,
                        use_container_width=True,
                        column_config={
                            "연월표시": st.column_config.TextColumn("연월", width="small"),
                            "당월금액": st.column_config.NumberColumn("당월금액(백만원)", format="%.0f"),
                            "전월금액": st.column_config.NumberColumn("전월금액(백만원)", format="%.0f"),
                            "금액차이": st.column_config.NumberColumn("금액차이(백만원)", format="%.0f"),
                            "금액증감률": st.column_config.NumberColumn("금액증감률(%)", format="%.1f%%"),
                            "당월수량": st.column_config.NumberColumn("당월수량(천EA)", format="%.0f"),
                            "전월수량": st.column_config.NumberColumn("전월수량(천EA)", format="%.0f"),
                            "수량차이": st.column_config.NumberColumn("수량차이(천EA)", format="%.0f"),
                            "수량증감률": st.column_config.NumberColumn("수량증감률(%)", format="%.1f%%")
                        }
                    )

                    # CSV 다운로드
                    csv_data = display_df.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="📥 전월대비 차이 CSV 다운로드",
                        data=csv_data,
                        file_name=f"전월대비차이_전체_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
                else:
                    st.info("전월대비 분석을 위한 데이터가 부족합니다. (최소 2개월 데이터 필요)")

            elif mom_group_option == "업체별":
                # 업체별 월별 집계
                mom_sql = f"""
                    WITH monthly_data AS (
                        SELECT
                            date_trunc('month', 마감월) AS 연월,
                            공급업체명,
                            SUM(송장금액)/1000000 AS 송장금액_백만원,
                            SUM(송장수량)/1000 AS 송장수량_천EA
                        FROM data
                        {where_sql_with_search}
                        GROUP BY date_trunc('month', 마감월), 공급업체명
                    ),
                    with_prev AS (
                        SELECT
                            연월,
                            공급업체명,
                            송장금액_백만원,
                            송장수량_천EA,
                            LAG(송장금액_백만원) OVER (PARTITION BY 공급업체명 ORDER BY 연월) AS 전월금액,
                            LAG(송장수량_천EA) OVER (PARTITION BY 공급업체명 ORDER BY 연월) AS 전월수량
                        FROM monthly_data
                    )
                    SELECT
                        연월,
                        공급업체명,
                        송장금액_백만원 AS 당월금액,
                        전월금액,
                        송장금액_백만원 - 전월금액 AS 금액차이,
                        CASE
                            WHEN 전월금액 > 0 THEN ROUND(((송장금액_백만원 - 전월금액) / 전월금액 * 100), 1)
                            ELSE NULL
                        END AS 금액증감률,
                        송장수량_천EA AS 당월수량,
                        전월수량,
                        송장수량_천EA - 전월수량 AS 수량차이,
                        CASE
                            WHEN 전월수량 > 0 THEN ROUND(((송장수량_천EA - 전월수량) / 전월수량 * 100), 1)
                            ELSE NULL
                        END AS 수량증감률
                    FROM with_prev
                    WHERE 전월금액 IS NOT NULL
                    ORDER BY 연월, 공급업체명
                """

                mom_df = con.execute(mom_sql).fetchdf()

                if not mom_df.empty:
                    # 날짜 포맷 변환
                    mom_df['연월표시'] = pd.to_datetime(mom_df['연월']).dt.strftime('%Y년%m월')

                    # 업체 선택 옵션
                    all_suppliers = sorted(mom_df['공급업체명'].unique().tolist())

                    # 전체선택 콜백 함수
                    def select_all_mom_suppliers():
                        st.session_state.mom_supplier_select = all_suppliers

                    col1, col2 = st.columns([3, 1])
                    with col1:
                        selected_suppliers = st.multiselect(
                            "업체 선택 (전체 보려면 선택하지 마세요)",
                            options=all_suppliers,
                            key="mom_supplier_select"
                        )
                    with col2:
                        st.button("☑ 전체선택", key="mom_select_all", on_click=select_all_mom_suppliers)

                    # 필터링
                    if selected_suppliers:
                        display_df = mom_df[mom_df['공급업체명'].isin(selected_suppliers)].copy()
                    else:
                        display_df = mom_df.copy()

                    display_df = display_df[['연월표시', '공급업체명', '당월금액', '전월금액', '금액차이', '금액증감률',
                                             '당월수량', '전월수량', '수량차이', '수량증감률']]

                    st.dataframe(
                        display_df,
                        hide_index=True,
                        use_container_width=True,
                        column_config={
                            "연월표시": st.column_config.TextColumn("연월", width="small"),
                            "공급업체명": st.column_config.TextColumn("업체명", width="medium"),
                            "당월금액": st.column_config.NumberColumn("당월금액(백만원)", format="%.0f"),
                            "전월금액": st.column_config.NumberColumn("전월금액(백만원)", format="%.0f"),
                            "금액차이": st.column_config.NumberColumn("금액차이(백만원)", format="%.0f"),
                            "금액증감률": st.column_config.NumberColumn("금액증감률(%)", format="%.1f%%"),
                            "당월수량": st.column_config.NumberColumn("당월수량(천EA)", format="%.0f"),
                            "전월수량": st.column_config.NumberColumn("전월수량(천EA)", format="%.0f"),
                            "수량차이": st.column_config.NumberColumn("수량차이(천EA)", format="%.0f"),
                            "수량증감률": st.column_config.NumberColumn("수량증감률(%)", format="%.1f%%")
                        }
                    )

                    # 업체별 구매액 비중 도넛 차트
                    st.subheader("업체별 구매액 비중")

                    # 업체별 총 구매액 집계 (당월금액 기준)
                    supplier_summary = display_df.groupby('공급업체명').agg({
                        '당월금액': 'sum'
                    }).reset_index()
                    supplier_summary.columns = ['공급업체명', '총구매액']
                    supplier_summary = supplier_summary.sort_values('총구매액', ascending=False).reset_index(drop=True)

                    # 비중 계산
                    total_amount = supplier_summary['총구매액'].sum()
                    supplier_summary['비중'] = (supplier_summary['총구매액'] / total_amount * 100).round(1)

                    # 정렬 순서를 위한 인덱스 추가
                    supplier_summary['order'] = range(len(supplier_summary))

                    # 도넛 차트 생성
                    # 아크 레이어
                    arc = alt.Chart(supplier_summary).mark_arc(innerRadius=80, outerRadius=140).encode(
                        theta=alt.Theta(field="총구매액", type="quantitative", stack=True),
                        order=alt.Order(field="order", type="quantitative"),
                        color=alt.Color(field="공급업체명", type="nominal", legend=alt.Legend(title="업체명"),
                                       sort=alt.EncodingSortField(field="order", order="ascending")),
                        tooltip=[
                            alt.Tooltip('공급업체명:N', title='업체명'),
                            alt.Tooltip('총구매액:Q', title='총구매액(백만원)', format=',.0f'),
                            alt.Tooltip('비중:Q', title='비중(%)', format='.1f')
                        ]
                    )

                    # 텍스트 레이어 - Altair transform을 활용하여 정확한 중앙 위치 계산
                    text = alt.Chart(supplier_summary).mark_text(
                        radius=110,
                        fontSize=14,
                        fontWeight='bold',
                        color='white'
                    ).transform_window(
                        # 누적 합계 계산 (현재 행 포함까지의 합)
                        cumulative_sum='sum(총구매액)',
                        sort=[alt.SortField('order', order='ascending')]
                    ).transform_calculate(
                        # 각 세그먼트의 중간 theta 값 계산
                        # theta_mid = (누적합 - 현재값/2) = 이전 누적합 + 현재값/2
                        theta_mid='datum.cumulative_sum - datum.총구매액 / 2'
                    ).encode(
                        theta=alt.Theta('theta_mid:Q', stack=False),
                        order=alt.Order(field="order", type="quantitative"),
                        text=alt.Text('비중:Q', format='.1f')
                    )

                    # 차트 결합
                    donut_chart = (arc + text).properties(
                        width=400,
                        height=400,
                        title="업체별 구매액 비중",
                        padding={"left": 50, "right": 50, "top": 50, "bottom": 50}
                    ).configure_view(
                        strokeWidth=0
                    )

                    # 차트와 테이블을 나란히 배치
                    col1, col2 = st.columns([1, 1])
                    with col1:
                        st.altair_chart(donut_chart, use_container_width=True)
                    with col2:
                        st.dataframe(
                            supplier_summary,
                            hide_index=True,
                            use_container_width=True,
                            column_config={
                                "공급업체명": st.column_config.TextColumn("업체명"),
                                "총구매액": st.column_config.NumberColumn("총구매액(백만원)", format="%.0f"),
                                "비중": st.column_config.NumberColumn("비중(%)", format="%.1f%%")
                            }
                        )

                    # CSV 다운로드
                    csv_data = display_df.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="📥 전월대비 차이 CSV 다운로드",
                        data=csv_data,
                        file_name=f"전월대비차이_업체별_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
                else:
                    st.info("전월대비 분석을 위한 데이터가 부족합니다. (최소 2개월 데이터 필요)")
        else:
            st.info("전월대비 차이 분석은 월별 집계에서만 사용 가능합니다. 상단에서 집계 단위를 '월별'로 변경해주세요.")

    # 미마감 자재 확인 섹션
    st.markdown("---")
    st.header("미마감 자재 확인")
    st.info("입력한 자재코드 중 현재 데이터에서 검색되지 않는 자재를 확인합니다.")

    unmatch_material_codes = st.text_area(
        "확인할 자재코드 입력 (쉼표, 개행, 탭으로 구분)",
        placeholder="예시:\n1234567, 2345678\n또는 엑셀 복사 붙여넣기",
        key="unmatch_material_codes_input",
        height=100
    )

    # 세션 상태 초기화
    if 'unmatch_result' not in st.session_state:
        st.session_state.unmatch_result = None

    if st.button("미마감 자재 확인", type="primary", key="check_unmatch_btn"):
        if unmatch_material_codes and unmatch_material_codes.strip():
            # 입력된 자재코드 파싱
            input_codes = [code.strip() for code in unmatch_material_codes.replace('\n', ',').replace('\t', ',').replace(';', ',').split(',') if code.strip()]

            if input_codes:
                # 데이터에 존재하는 자재코드 조회
                existing_codes_query = f"""
                SELECT DISTINCT CAST(자재 AS VARCHAR) AS 자재코드
                FROM data
                {where_sql}
                """
                existing_codes_df = con.execute(existing_codes_query).fetchdf()
                existing_codes_set = set(existing_codes_df['자재코드'].astype(str).str.strip())

                # 미마감 자재 찾기 (데이터에 없는 자재코드)
                unmatched_codes = []
                for code in input_codes:
                    # 정확히 일치하는 코드 확인
                    if code not in existing_codes_set:
                        # 부분 일치도 확인 (enhance_pattern 로직)
                        pattern = enhance_pattern(code)
                        match_query = f"""
                        SELECT COUNT(*) as cnt
                        FROM data
                        {where_sql} AND CAST(자재 AS VARCHAR) ILIKE '{pattern}'
                        """
                        match_count = con.execute(match_query).fetchdf()['cnt'].iloc[0]

                        if match_count == 0:
                            unmatched_codes.append(code)

                # 결과를 세션 상태에 저장
                st.session_state.unmatch_result = {
                    'input_count': len(input_codes),
                    'unmatched_codes': unmatched_codes
                }
            else:
                st.session_state.unmatch_result = {'error': '자재코드를 입력해주세요.'}
        else:
            st.session_state.unmatch_result = {'error': '자재코드를 입력해주세요.'}

    # 결과 표시 (세션 상태에서 가져오기)
    if st.session_state.unmatch_result is not None:
        result = st.session_state.unmatch_result

        if 'error' in result:
            st.warning(result['error'])
        elif 'unmatched_codes' in result:
            if result['unmatched_codes']:
                st.error(f"**미마감 자재: {len(result['unmatched_codes'])}개 발견** (입력: {result['input_count']}개)")

                # DataFrame으로 변환
                unmatch_df = pd.DataFrame({
                    '자재코드': result['unmatched_codes'],
                    '상태': ['미마감'] * len(result['unmatched_codes'])
                })

                st.dataframe(unmatch_df, use_container_width=True, hide_index=True)

                # CSV 다운로드
                st.download_button(
                    "미마감 자재 CSV 다운로드",
                    unmatch_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                    file_name="unmatched_materials.csv",
                    mime="text/csv",
                    key="download_unmatch_csv"
                )
            else:
                st.success("✅ 모든 자재코드가 데이터에 존재합니다!")
                st.write(f"입력된 {result['input_count']}개 자재코드가 모두 확인되었습니다.")

    # 자재 점검 (단종 점검) 섹션
    st.markdown("---")
    st.header("자재 점검 (단종 점검)")
    st.info("검색된 자재의 공급업체 정보를 중복 제거하여 표시합니다. (월별 정보 제외)")

    col1, col2 = st.columns(2)
    with col1:
        check_material_name = st.text_area(
            "자재명 검색",
            placeholder="예시: *퍼퓸*, *로션*",
            key="check_material_name_input",
            height=100
        )
    with col2:
        check_material_code = st.text_area(
            "자재코드 검색",
            placeholder="예시: 1234567, 2345678",
            key="check_material_code_input",
            height=100
        )

    # 세션 상태 초기화
    if 'check_result' not in st.session_state:
        st.session_state.check_result = None

    if st.button("단종 점검", type="primary", key="check_material_btn"):
        # 검색 조건 생성
        check_conditions = []
        check_info = []

        # 자재명 검색
        if check_material_name and check_material_name.strip():
            name_patterns = []
            name_terms = [term.strip() for term in check_material_name.replace('\n', ',').replace(';', ',').split(',') if term.strip()]
            for term in name_terms:
                enhanced_patt = enhance_pattern(term)
                name_patterns.append(f"자재명 ILIKE '{enhanced_patt}'")

            if name_patterns:
                name_clause = " OR ".join(name_patterns)
                check_conditions.append(f"({name_clause})")
                check_info.append(f"자재명: {len(name_terms)}개 조건")

        # 자재코드 검색
        if check_material_code and check_material_code.strip():
            code_patterns = []
            code_terms = [term.strip() for term in check_material_code.replace('\n', ',').replace('\t', ',').replace(';', ',').split(',') if term.strip()]
            for term in code_terms:
                enhanced_patt = enhance_pattern(term)
                code_patterns.append(f"CAST(자재 AS VARCHAR) ILIKE '{enhanced_patt}'")

            if code_patterns:
                code_clause = " OR ".join(code_patterns)
                check_conditions.append(f"({code_clause})")
                check_info.append(f"자재코드: {len(code_terms)}개 조건")

        if check_conditions:
            check_where = " AND ".join(check_conditions)

            # 중복 제거된 자재-업체 조합 조회
            check_supplier_code_select = ""
            if "공급업체코드" in df.columns:
                check_supplier_code_select = """
                       CASE
                           WHEN 공급업체코드 = '' OR 공급업체코드 IS NULL THEN NULL
                           ELSE 공급업체코드
                       END AS 업체코드,
                """

            check_query = f"""
            SELECT DISTINCT
                   자재 AS 자재코드,
                   자재명,
                   {check_supplier_code_select}
                   공급업체명 AS 업체명
            FROM data
            {where_sql} AND ({check_where})
            ORDER BY 자재코드, 업체명
            """

            check_df = con.execute(check_query).fetchdf()

            # 결과를 세션 상태에 저장
            if not check_df.empty:
                check_info_text = ", ".join(check_info)
                st.session_state.check_result = {
                    'check_df': check_df,
                    'check_info_text': check_info_text
                }
            else:
                st.session_state.check_result = {'error': '검색 조건에 맞는 자재가 없습니다.'}
        else:
            st.session_state.check_result = {'error': '자재명 또는 자재코드를 입력해주세요.'}

    # 결과 표시 (세션 상태에서 가져오기)
    if st.session_state.check_result is not None:
        result = st.session_state.check_result

        if 'error' in result:
            st.warning(result['error'])
        elif 'check_df' in result:
            check_df = result['check_df']
            check_info_text = result['check_info_text']

            st.success(f"**검색 결과: {len(check_df):,}건** (중복 제거됨)")
            st.write(f"검색 조건: {check_info_text}")

            # 자재코드 개수 및 업체 개수 요약
            unique_materials = check_df['자재코드'].nunique()
            unique_suppliers = check_df['업체명'].nunique()

            col1, col2 = st.columns(2)
            with col1:
                st.metric("고유 자재 수", f"{unique_materials:,}개")
            with col2:
                st.metric("관련 업체 수", f"{unique_suppliers:,}개")

            st.subheader("단종 점검 결과")
            st.dataframe(
                check_df,
                use_container_width=True,
                hide_index=True
            )

            # CSV 다운로드
            st.download_button(
                "단종 점검 결과 CSV 다운로드",
                check_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                file_name="material_check_results.csv",
                mime="text/csv",
                key="download_check_csv"
            )
