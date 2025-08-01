# backdata_dashboard.py
"""구매 데이터 대시보드 (Streamlit + DuckDB)

📌 **2025‑08‑01 hotfix**

* 업로드 후 반응이 없던 문제 수정
  1. **마감월 Excel 일련번호 → 날짜** 변환 로직 복원 (숫자 → `unit="D", origin="1899‑12‑30"`)
  2. 업로드·로딩 과정에 **스피너 표시**
  3. 집계 결과가 비어 있을 때 친절한 안내 문구

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

# ════════════════════════════════════════════════════════════════════════
# 📚 데이터 로딩 & 전처리
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
    # 중복 컬럼 제거
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]
    return df


@st.cache_data(show_spinner=False)
def load_csv(upload: BytesIO) -> pd.DataFrame:
    df = pd.read_csv(upload, encoding="cp949", low_memory=False)
    df = _standardize_columns(df)

    if "마감월" not in df.columns:
        st.error("⚠️ '마감월' 컬럼을 찾을 수 없습니다. 헤더명을 확인해 주세요.")
        st.stop()

    # Excel 일련번호(숫자) → 날짜
    if pd.api.types.is_numeric_dtype(df["마감월"]):
        df["마감월"] = pd.to_datetime(
            df["마감월"], unit="D", origin="1899-12-30", errors="coerce"
        )
    else:
        df["마감월"] = pd.to_datetime(df["마감월"], errors="coerce")

    df["연도"] = df["마감월"].dt.year.astype("Int64")
    df["연월"] = df["마감월"].dt.to_period("M").dt.to_timestamp()

    num_cols: List[str] = [c for c in ["송장수량", "송장금액", "단가", "플랜트", "구매그룹"] if c in df.columns]
    if num_cols:
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    # 공급업체 표시 컬럼
    if "공급업체명" in df.columns:
        df["공급업체명"] = df["공급업체명"].astype(str).str.strip()
    if "공급업체코드" in df.columns:
        df["공급업체코드"] = df["공급업체코드"].astype(str).str.strip()
        df["업체표시"] = df["공급업체코드"].str.zfill(5) + "_" + df["공급업체명"].fillna("")
    elif "공급업체명" in df.columns:
        df["업체표시"] = df["공급업체명"]

    return df

# ════════════════════════════════════════════════════════════════════════
# 🔧 공통 함수
# ════════════════════════════════════════════════════════════════════════

def sql_list_num(vals: list[int]) -> str:
    return ",".join(map(str, vals)) if vals else "-1"


def sql_list_str(vals: list[str]) -> str:
    if not vals:
        return "''"
    return ",".join(f"'{v.replace("'", "''")}'" for v in vals)

# ---- 멀티셀렉트 with 전체/해제 ----

def _set_all(key: str, options: list):
    st.session_state[key] = options


def _clear_all(key: str):
    st.session_state[key] = []


def multiselect_with_toggle(label: str, options: list, key_prefix: str) -> list:
    ms_key = f"{key_prefix}_ms"
    if ms_key not in st.session_state:
        st.session_state[ms_key] = options
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        sel = st.multiselect(label, options, key=ms_key)
    with col2:
        st.button("전체", on_click=_set_all, args=(ms_key, options), key=f"{key_prefix}_all")
    with col3:
        st.button("해제", on_click=_clear_all, args=(ms_key,), key=f"{key_prefix}_none")
    return sel

# ════════════════════════════════════════════════════════════════════════
# 📂 파일 업로드
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
# 🖥️ 대시보드
# ════════════════════════════════════════════════════════════════════════
if df is not None and not df.empty:
    con = duckdb.connect(database=":memory:")
    con.register("data", df)

    # --- 필터 ---
    with st.sidebar:
        st.header("필터 조건")
        years_all = df["연도"].dropna().astype(int).unique().tolist()
        plants_all = df["플랜트"].dropna().astype(int).unique().tolist() if "플랜트" in df.columns else []
        groups_all = df["구매그룹"].dropna().astype(int).unique().tolist() if "구매그룹" in df.columns else []
        suppliers_all = df["업체표시"].dropna().unique().tolist() if "업체표시" in df.columns else []

        sel_years = multiselect_with_toggle("연도", sorted(years_all), "yr")
        sel_plants = multiselect_with_toggle("플랜트", sorted(plants_all), "pl") if plants_all else []
        sel_groups = multiselect_with_toggle("구매그룹", sorted(groups_all), "gr") if groups_all else []
        sel_suppliers = multiselect_with_toggle("공급업체", sorted(suppliers_all), "sp") if suppliers_all else []

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
    month_df = con.execute(
        f"""
        SELECT date_trunc('month', 마감월) AS 연월,
               SUM(송장수량)/1000    AS 송장수량_천EA,
               SUM(송장금액)/1000000 AS 송장금액_백만원
        FROM data
        {where_sql}
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchdf()

    month_df["연월표시"] = month_df["연월"].dt.strftime("%Y년%m월")

    st.title("📈 월별 구매 추이")
    if month_df.empty:
        st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
    else:
        st.dataframe(month_df[["연월표시", "송장수량_천EA", "송장금액_백만원"]], hide_index=True, use_container_width=True)
        chart = (
            alt.Chart(month_df)
            .transform_fold(["송장수량_천EA", "송장금액_백만원"], as_=["지표", "값"])
            .mark_line(point=True)
            .encode(
                x=alt.X("연월:T", title="연월"),
                y=alt.Y("값:Q", title="값"),
                color="지표:N",
                tooltip=["연월표시:N", "지표:N", "값:Q"],
            )
            .interactive()
        )
        st.altair_chart(chart, use_container_width=True)
    st.caption("단위: 송장수량 = 천 EA,   송장
