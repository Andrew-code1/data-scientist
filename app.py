import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import io
from typing import Dict, List, Tuple, Optional

# Page configuration
st.set_page_config(
    page_title="CSV Data Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }
</style>
""", unsafe_allow_html=True)

class DataAnalyzer:
    def __init__(self):
        self.conn = duckdb.connect(':memory:')
        self.df = None
        self.table_name = 'uploaded_data'
    
    def detect_encoding(self, uploaded_file):
        """Detect file encoding automatically"""
        import chardet
        
        # Reset file pointer to beginning
        uploaded_file.seek(0)
        # Read a sample of the file for encoding detection
        sample = uploaded_file.read(10000)
        uploaded_file.seek(0)  # Reset again
        
        # Detect encoding
        detection = chardet.detect(sample)
        encoding = detection['encoding']
        confidence = detection['confidence']
        
        return encoding, confidence
    
    def load_csv_data(self, uploaded_file) -> bool:
        """Load CSV data into DuckDB with automatic encoding detection"""
        try:
            # Try to detect encoding
            try:
                encoding, confidence = self.detect_encoding(uploaded_file)
                st.info(f"Detected encoding: {encoding} (confidence: {confidence:.2f})")
            except:
                encoding = None
                confidence = 0
            
            # List of encodings to try
            encodings_to_try = []
            
            # Add detected encoding first if confidence is high
            if encoding and confidence > 0.7:
                encodings_to_try.append(encoding)
            
            # Add common encodings
            common_encodings = ['utf-8', 'cp949', 'euc-kr', 'latin1', 'cp1252', 'iso-8859-1']
            for enc in common_encodings:
                if enc not in encodings_to_try:
                    encodings_to_try.append(enc)
            
            # Try each encoding
            last_error = None
            for encoding_attempt in encodings_to_try:
                try:
                    uploaded_file.seek(0)  # Reset file pointer
                    self.df = pd.read_csv(uploaded_file, encoding=encoding_attempt)
                    st.success(f"Successfully loaded CSV with encoding: {encoding_attempt}")
                    
                    # Create table in DuckDB
                    self.conn.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                    self.conn.execute(f"CREATE TABLE {self.table_name} AS SELECT * FROM df")
                    return True
                    
                except UnicodeDecodeError as e:
                    last_error = f"Encoding {encoding_attempt}: {str(e)}"
                    continue
                except Exception as e:
                    last_error = f"Encoding {encoding_attempt}: {str(e)}"
                    continue
            
            # If all encodings failed
            st.error(f"Failed to load CSV with any encoding. Last error: {last_error}")
            return False
            
        except Exception as e:
            st.error(f"Error loading CSV: {str(e)}")
            return False
    
    def get_data_overview(self) -> Dict:
        """Get basic data overview"""
        if self.df is None:
            return {}
        
        overview = {
            'rows': len(self.df),
            'columns': len(self.df.columns),
            'memory_usage': self.df.memory_usage(deep=True).sum(),
            'column_info': []
        }
        
        for col in self.df.columns:
            col_info = {
                'name': col,
                'dtype': str(self.df[col].dtype),
                'null_count': self.df[col].isnull().sum(),
                'null_percentage': (self.df[col].isnull().sum() / len(self.df)) * 100,
                'unique_count': self.df[col].nunique()
            }
            overview['column_info'].append(col_info)
        
        return overview
    
    def get_statistical_summary(self) -> pd.DataFrame:
        """Get statistical summary using DuckDB"""
        try:
            query = f"SELECT * FROM {self.table_name} LIMIT 0"
            result = self.conn.execute(query).fetchdf()
            numeric_cols = result.select_dtypes(include=[np.number]).columns.tolist()
            
            if not numeric_cols:
                return pd.DataFrame()
            
            summary_queries = []
            for col in numeric_cols:
                summary_queries.append(f"""
                SELECT 
                    '{col}' as column_name,
                    COUNT({col}) as count,
                    AVG({col}) as mean,
                    STDDEV({col}) as std,
                    MIN({col}) as min,
                    percentile_cont(0.25) WITHIN GROUP (ORDER BY {col}) as q25,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY {col}) as median,
                    percentile_cont(0.75) WITHIN GROUP (ORDER BY {col}) as q75,
                    MAX({col}) as max
                FROM {self.table_name}
                WHERE {col} IS NOT NULL
                """)
            
            if summary_queries:
                union_query = " UNION ALL ".join(summary_queries)
                return self.conn.execute(union_query).fetchdf()
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Error generating statistical summary: {str(e)}")
            return pd.DataFrame()
    
    def execute_custom_query(self, query: str) -> pd.DataFrame:
        """Execute custom DuckDB query"""
        try:
            # Replace common table references
            query = query.replace('data', self.table_name)
            query = query.replace('df', self.table_name)
            
            result = self.conn.execute(query).fetchdf()
            return result
        except Exception as e:
            st.error(f"Query error: {str(e)}")
            return pd.DataFrame()
    
    def get_correlation_matrix(self) -> pd.DataFrame:
        """Get correlation matrix for numeric columns"""
        if self.df is None:
            return pd.DataFrame()
        
        numeric_df = self.df.select_dtypes(include=[np.number])
        if numeric_df.empty:
            return pd.DataFrame()
        
        return numeric_df.corr()

def create_overview_tab(analyzer: DataAnalyzer):
    """Create data overview tab"""
    overview = analyzer.get_data_overview()
    
    if not overview:
        st.warning("No data loaded. Please upload a CSV file.")
        return
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Rows", f"{overview['rows']:,}")
    
    with col2:
        st.metric("Total Columns", overview['columns'])
    
    with col3:
        memory_mb = overview['memory_usage'] / (1024 * 1024)
        st.metric("Memory Usage", f"{memory_mb:.2f} MB")
    
    with col4:
        total_nulls = sum(col['null_count'] for col in overview['column_info'])
        st.metric("Total Null Values", f"{total_nulls:,}")
    
    # Column information table
    st.subheader("Column Information")
    col_df = pd.DataFrame(overview['column_info'])
    col_df['null_percentage'] = col_df['null_percentage'].round(2)
    
    st.dataframe(
        col_df,
        column_config={
            'name': 'Column Name',
            'dtype': 'Data Type',
            'null_count': 'Null Count',
            'null_percentage': st.column_config.NumberColumn(
                'Null %',
                format="%.2f%%"
            ),
            'unique_count': 'Unique Values'
        },
        hide_index=True,
        use_container_width=True
    )

def create_statistics_tab(analyzer: DataAnalyzer):
    """Create statistical summary tab"""
    stats_df = analyzer.get_statistical_summary()
    
    if stats_df.empty:
        st.warning("No numeric columns found for statistical analysis.")
        return
    
    st.subheader("Statistical Summary")
    
    # Format the statistics table
    formatted_stats = stats_df.round(4)
    st.dataframe(
        formatted_stats,
        column_config={
            'column_name': 'Column',
            'count': 'Count',
            'mean': 'Mean',
            'std': 'Std Dev',
            'min': 'Min',
            'q25': 'Q1 (25%)',
            'median': 'Median',
            'q75': 'Q3 (75%)',
            'max': 'Max'
        },
        hide_index=True,
        use_container_width=True
    )

def create_visualizations_tab(analyzer: DataAnalyzer):
    """Create visualizations tab"""
    if analyzer.df is None:
        st.warning("No data loaded.")
        return
    
    numeric_cols = analyzer.df.select_dtypes(include=[np.number]).columns.tolist()
    categorical_cols = analyzer.df.select_dtypes(include=['object', 'category']).columns.tolist()
    
    st.subheader("Data Visualizations")
    
    # Correlation heatmap
    if len(numeric_cols) > 1:
        st.write("### Correlation Heatmap")
        corr_matrix = analyzer.get_correlation_matrix()
        
        fig_corr = px.imshow(
            corr_matrix,
            text_auto=True,
            aspect="auto",
            color_continuous_scale="RdBu",
            title="Correlation Matrix"
        )
        fig_corr.update_layout(height=500)
        st.plotly_chart(fig_corr, use_container_width=True)
    
    # Distribution plots
    if numeric_cols:
        st.write("### Distribution Plots")
        
        col1, col2 = st.columns(2)
        
        with col1:
            selected_col = st.selectbox("Select column for histogram:", numeric_cols)
        
        with col2:
            bins = st.slider("Number of bins:", 10, 100, 30)
        
        if selected_col:
            fig_hist = px.histogram(
                analyzer.df,
                x=selected_col,
                nbins=bins,
                title=f"Distribution of {selected_col}"
            )
            st.plotly_chart(fig_hist, use_container_width=True)
    
    # Box plots
    if numeric_cols:
        st.write("### Box Plots (Outlier Detection)")
        selected_box_col = st.selectbox("Select column for box plot:", numeric_cols, key="boxplot")
        
        if selected_box_col:
            fig_box = px.box(
                analyzer.df,
                y=selected_box_col,
                title=f"Box Plot of {selected_box_col}"
            )
            st.plotly_chart(fig_box, use_container_width=True)
    
    # Scatter plots
    if len(numeric_cols) >= 2:
        st.write("### Scatter Plot")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            x_col = st.selectbox("X-axis:", numeric_cols)
        
        with col2:
            y_col = st.selectbox("Y-axis:", numeric_cols, index=1 if len(numeric_cols) > 1 else 0)
        
        with col3:
            color_col = None
            if categorical_cols:
                color_col = st.selectbox("Color by (optional):", [None] + categorical_cols)
        
        if x_col and y_col:
            fig_scatter = px.scatter(
                analyzer.df,
                x=x_col,
                y=y_col,
                color=color_col,
                title=f"{y_col} vs {x_col}"
            )
            st.plotly_chart(fig_scatter, use_container_width=True)

def create_query_tab(analyzer: DataAnalyzer):
    """Create custom query tab"""
    st.subheader("Custom SQL Queries")
    st.write("Write custom DuckDB SQL queries to analyze your data. Use 'uploaded_data' as the table name.")
    
    # Example queries
    with st.expander("Example Queries"):
        st.code("""
-- Basic data exploration
SELECT * FROM uploaded_data LIMIT 10;

-- Count records by category
SELECT column_name, COUNT(*) as count 
FROM uploaded_data 
GROUP BY column_name 
ORDER BY count DESC;

-- Statistical summary
SELECT 
    AVG(numeric_column) as avg_value,
    MIN(numeric_column) as min_value,
    MAX(numeric_column) as max_value
FROM uploaded_data;

-- Find duplicates
SELECT *, COUNT(*) as duplicate_count
FROM uploaded_data
GROUP BY ALL
HAVING COUNT(*) > 1;
        """)
    
    # Query input
    query = st.text_area(
        "Enter your SQL query:",
        height=150,
        placeholder="SELECT * FROM uploaded_data LIMIT 10;"
    )
    
    if st.button("Execute Query", type="primary"):
        if query.strip():
            with st.spinner("Executing query..."):
                result = analyzer.execute_custom_query(query)
                
                if not result.empty:
                    st.success(f"Query executed successfully! Returned {len(result)} rows.")
                    st.dataframe(result, use_container_width=True)
                    
                    # Download option
                    csv_buffer = io.StringIO()
                    result.to_csv(csv_buffer, index=False)
                    st.download_button(
                        "Download Results as CSV",
                        csv_buffer.getvalue(),
                        file_name="query_results.csv",
                        mime="text/csv"
                    )
                else:
                    st.info("Query executed but returned no results.")
        else:
            st.warning("Please enter a query.")

def create_data_quality_tab(analyzer: DataAnalyzer):
    """Create data quality assessment tab"""
    if analyzer.df is None:
        st.warning("No data loaded.")
        return
    
    st.subheader("Data Quality Assessment")
    
    # Missing values analysis
    st.write("### Missing Values Analysis")
    missing_data = analyzer.df.isnull().sum()
    missing_percentage = (missing_data / len(analyzer.df)) * 100
    
    missing_df = pd.DataFrame({
        'Column': missing_data.index,
        'Missing Count': missing_data.values,
        'Missing Percentage': missing_percentage.values
    }).sort_values('Missing Count', ascending=False)
    
    # Filter out columns with no missing values for the chart
    missing_for_chart = missing_df[missing_df['Missing Count'] > 0]
    
    if not missing_for_chart.empty:
        fig_missing = px.bar(
            missing_for_chart,
            x='Column',
            y='Missing Count',
            title="Missing Values by Column"
        )
        st.plotly_chart(fig_missing, use_container_width=True)
    else:
        st.success("No missing values found in the dataset!")
    
    st.dataframe(missing_df, hide_index=True, use_container_width=True)
    
    # Duplicate rows analysis
    st.write("### Duplicate Rows Analysis")
    duplicate_count = analyzer.df.duplicated().sum()
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Duplicate Rows", duplicate_count)
    with col2:
        duplicate_percentage = (duplicate_count / len(analyzer.df)) * 100
        st.metric("Duplicate Percentage", f"{duplicate_percentage:.2f}%")
    
    if duplicate_count > 0:
        st.warning(f"Found {duplicate_count} duplicate rows in the dataset.")
        if st.button("Show Duplicate Rows"):
            duplicates = analyzer.df[analyzer.df.duplicated(keep=False)].sort_values(by=list(analyzer.df.columns))
            st.dataframe(duplicates, use_container_width=True)

def main():
    # Header
    st.markdown('<h1 class="main-header">📊 CSV Data Analytics Dashboard</h1>', unsafe_allow_html=True)
    st.markdown("Upload your CSV file and explore your data with powerful analytics powered by DuckDB")
    
    # Initialize analyzer
    if 'analyzer' not in st.session_state:
        st.session_state.analyzer = DataAnalyzer()
    
    analyzer = st.session_state.analyzer
    
    # Sidebar for file upload
    with st.sidebar:
        st.header("📁 Data Upload")
        uploaded_file = st.file_uploader(
            "Choose a CSV file",
            type=['csv'],
            help="Upload a CSV file to begin analysis"
        )
        
        if uploaded_file is not None:
            if st.button("Load Data", type="primary"):
                with st.spinner("Loading data..."):
                    success = analyzer.load_csv_data(uploaded_file)
                    if success:
                        st.success("Data loaded successfully!")
                        st.rerun()
        
        # File info
        if uploaded_file is not None:
            st.write("**File Details:**")
            st.write(f"- **Name:** {uploaded_file.name}")
            st.write(f"- **Size:** {uploaded_file.size:,} bytes")
    
    # Main content
    if analyzer.df is not None:
        # Sample data preview
        st.subheader("📋 Data Preview")
        with st.expander("Show first 10 rows", expanded=False):
            st.dataframe(analyzer.df.head(10), use_container_width=True)
        
        # Tabs for different analyses
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📊 Overview", 
            "📈 Statistics", 
            "📉 Visualizations", 
            "🔍 Custom Queries",
            "🔧 Data Quality"
        ])
        
        with tab1:
            create_overview_tab(analyzer)
        
        with tab2:
            create_statistics_tab(analyzer)
        
        with tab3:
            create_visualizations_tab(analyzer)
        
        with tab4:
            create_query_tab(analyzer)
        
        with tab5:
            create_data_quality_tab(analyzer)
    
    else:
        # Welcome message
        st.info("👆 Please upload a CSV file using the sidebar to get started with data analysis.")
        
        # Feature highlights
        st.subheader("🚀 Features")
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **📊 Data Overview**
            - Basic statistics and data types
            - Missing values analysis  
            - Memory usage information
            
            **📈 Statistical Analysis**
            - Descriptive statistics
            - Distribution analysis
            - Correlation matrices
            """)
        
        with col2:
            st.markdown("""
            **📉 Interactive Visualizations**
            - Correlation heatmaps
            - Distribution plots
            - Box plots for outliers
            - Scatter plots
            
            **🔍 Custom Analysis**
            - SQL query interface
            - Data quality assessment
            - Export capabilities
            """)

if __name__ == "__main__":
    main()