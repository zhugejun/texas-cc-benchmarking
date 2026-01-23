"""
Texas Community College Benchmarking Dashboard
Multi-college comparison with fixed years (2020-2024)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from pathlib import Path
import yaml
from urllib.parse import quote_plus

# Page config
st.set_page_config(
    page_title="Texas CC Benchmarking",
    page_icon="graduation_cap",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main { padding-top: 1rem; }
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    h1 { color: #1f1f1f; font-weight: 700; }
    h2, h3 { color: #444; }
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
    }
    [data-testid="stSidebar"] .stMarkdown { color: white; }
    [data-testid="stSidebar"] .stExpander {
        background-color: rgba(255,255,255,0.1);
        border-radius: 8px;
        margin-bottom: 10px;
    }
    </style>
    """, unsafe_allow_html=True)


# Snowflake connection
@st.cache_resource
def get_engine():
    profiles_path = Path.home() / '.dbt' / 'profiles.yml'
    with open(profiles_path) as f:
        profiles = yaml.safe_load(f)
    profile = profiles['texas_cc_benchmarking']['outputs']['dev']
    connection_string = (
        f"snowflake://{profile['user']}:{quote_plus(profile['password'])}@{profile['account']}/"
        f"{profile.get('database', 'TEXAS_CC')}/{profile.get('schema', 'DBT_GEJUN')}"
        f"?warehouse={profile.get('warehouse', 'COMPUTE_WH')}"
    )
    if profile.get('role'):
        connection_string += f"&role={profile['role']}"
    return create_engine(connection_string)


@st.cache_data(ttl=600)
def load_data(query: str) -> pd.DataFrame:
    engine = get_engine()
    df = pd.read_sql(query, engine)
    df.columns = df.columns.str.upper()
    return df


@st.cache_data(ttl=600)
def load_outcomes():
    return load_data("SELECT * FROM fct_student_outcomes ORDER BY YEAR, INSTITUTION_NAME")


@st.cache_data(ttl=600)
def load_institutions():
    return load_data("SELECT * FROM dim_texas_institutions")


# Load data
try:
    outcomes = load_outcomes()
    institutions = load_institutions()
    data_loaded = True
except Exception as e:
    st.error(f"Could not connect to Snowflake: {e}")
    st.stop()

# Get available colleges
available_colleges = sorted(outcomes['INSTITUTION_NAME'].unique().tolist())
fixed_years = [2020, 2021, 2022, 2023, 2024]


# ===================
# SIDEBAR
# ===================
st.sidebar.title("Texas CC Benchmarking")
st.sidebar.markdown("---")

# Fixed years display
st.sidebar.info("Years: 2020-2024 (all years included)")

st.sidebar.markdown("---")

# College selector with checkboxes
st.sidebar.subheader("Select Colleges")

# Quick action buttons - modify the actual checkbox widget keys
col1, col2 = st.sidebar.columns(2)
with col1:
    if st.button("Select All", key="sel_all", use_container_width=True):
        for c in available_colleges:
            st.session_state[f"chk_{c}"] = True
        st.rerun()
with col2:
    if st.button("Clear All", key="clr_all", use_container_width=True):
        for c in available_colleges:
            st.session_state[f"chk_{c}"] = False
        st.rerun()

# Count selected (based on actual checkbox widget states)
selected_count = sum(1 for c in available_colleges if st.session_state.get(f"chk_{c}", True))
st.sidebar.caption(f"{selected_count} of {len(available_colleges)} selected")

# Search filter
search_term = st.sidebar.text_input("Search colleges", placeholder="Type to filter...")

# Filter colleges based on search
if search_term:
    display_colleges = [c for c in available_colleges if search_term.lower() in c.lower()]
else:
    display_colleges = available_colleges

# Display checkboxes in a scrollable container
with st.sidebar.container(height=400):
    for college in display_colleges:
        # Initialize to True (selected) if not already in session state
        if f"chk_{college}" not in st.session_state:
            st.session_state[f"chk_{college}"] = True
        st.checkbox(college, key=f"chk_{college}")

# Get selected colleges list from widget states
selected_colleges = [college for college in available_colleges if st.session_state.get(f"chk_{college}", True)]


st.sidebar.markdown("---")
if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.caption("Data Source: IPEDS 2020-2024")

# Validate selection
if not selected_colleges:
    st.warning("Please select at least one college from the sidebar")
    st.stop()

# Filter data
filtered_data = outcomes[outcomes['INSTITUTION_NAME'].isin(selected_colleges)]


# ===================
# MAIN CONTENT
# ===================
st.title("Texas Community College Benchmarking")
st.markdown(f"**Comparing {len(selected_colleges)} colleges across years 2020-2024**")

# Main tabs for different metrics
tab_overview, tab_graduation, tab_retention, tab_completion, tab_equity = st.tabs([
    "Overview", "Graduation Rate", "Retention Rate", "Completions", "Equity"
])


# ===================
# TAB: Overview
# ===================
with tab_overview:
    st.header("Overview - All Selected Colleges")
    
    # Latest year summary
    latest_year = 2024
    latest_data = filtered_data[filtered_data['YEAR'] == latest_year]
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Colleges", len(selected_colleges))
    with col2:
        avg_grad = latest_data['GRADUATION_RATE_150'].mean()
        st.metric("Avg Graduation Rate (2024)", f"{avg_grad:.1f}%" if pd.notna(avg_grad) else "N/A")
    with col3:
        avg_ret = latest_data['FULL_TIME_RETENTION_RATE'].mean()
        st.metric("Avg Retention Rate (2024)", f"{avg_ret:.1f}%" if pd.notna(avg_ret) else "N/A")
    with col4:
        total_deg = latest_data['ASSOCIATE_DEGREES'].sum()
        st.metric("Total Degrees (2024)", f"{total_deg:,.0f}" if pd.notna(total_deg) else "N/A")
    
    st.divider()
    
    # Data table with all colleges and years
    st.subheader("All Colleges Data (2024)")
    
    display_cols = ['INSTITUTION_NAME', 'GRADUATION_RATE_150', 'SUCCESS_RATE', 
                    'FULL_TIME_RETENTION_RATE', 'ASSOCIATE_DEGREES', 'TOTAL_COMPLETIONS']
    display_data = latest_data[[c for c in display_cols if c in latest_data.columns]].copy()
    display_data.columns = ['College', 'Graduation Rate', 'Success Rate', 'Retention Rate', 
                           'Associate Degrees', 'Total Completions']
    display_data = display_data.sort_values('Graduation Rate', ascending=False)
    
    st.dataframe(display_data, width="stretch", hide_index=True)
    
    # Quick comparison bar chart
    st.subheader("Graduation Rate Comparison (2024)")
    chart_data = latest_data[['INSTITUTION_NAME', 'GRADUATION_RATE_150']].dropna()
    chart_data = chart_data.sort_values('GRADUATION_RATE_150', ascending=True)
    
    fig = px.bar(
        chart_data,
        x='GRADUATION_RATE_150',
        y='INSTITUTION_NAME',
        orientation='h',
        labels={'GRADUATION_RATE_150': 'Graduation Rate (%)', 'INSTITUTION_NAME': ''},
        color='GRADUATION_RATE_150',
        color_continuous_scale='Purples'
    )
    fig.update_layout(height=max(400, len(chart_data) * 20), showlegend=False)  # Bar chart doesn't need legend
    fig.update_yaxes(rangemode='tozero')
    st.plotly_chart(fig, width="stretch")


# ===================
# TAB: Graduation Rate
# ===================
with tab_graduation:
    st.header("Graduation Rate Analysis")
    
    # Trend chart - all colleges
    st.subheader("Graduation Rate Trend (2020-2024)")
    
    trend_data = filtered_data.sort_values(['INSTITUTION_NAME', 'YEAR'])
    
    fig = px.line(
        trend_data,
        x='YEAR',
        y='GRADUATION_RATE_150',
        color='INSTITUTION_NAME',
        markers=True,
        labels={
            'GRADUATION_RATE_150': 'Graduation Rate (%)',
            'YEAR': 'Year',
            'INSTITUTION_NAME': 'College'
        }
    )
    fig.update_layout(
        height=500,
        showlegend=(len(selected_colleges) <= 10)
    )
    fig.update_xaxes(dtick=1)
    fig.update_yaxes(rangemode='tozero')
    st.plotly_chart(fig, width="stretch")

    # Year-over-year comparison bar chart
    st.subheader("By Year Comparison")

    fig = px.bar(
        trend_data.sort_values(['YEAR', 'INSTITUTION_NAME']),
        x='INSTITUTION_NAME',
        y='GRADUATION_RATE_150',
        color='YEAR',
        barmode='group',
        labels={'INSTITUTION_NAME': '', 'GRADUATION_RATE_150': 'Graduation Rate (%)', 'YEAR': 'Year'},
        color_discrete_sequence=px.colors.sequential.Purples_r
    )
    fig.update_layout(height=450, xaxis_tickangle=-45)
    fig.update_yaxes(rangemode='tozero')
    st.plotly_chart(fig, width="stretch")

    # Data table
    st.subheader("Data Table")
    pivot = filtered_data.pivot_table(
        index='INSTITUTION_NAME', columns='YEAR', values='GRADUATION_RATE_150', aggfunc='first'
    ).round(1)
    pivot.columns = [str(int(c)) for c in pivot.columns]
    if '2020' in pivot.columns and '2024' in pivot.columns:
        pivot['Change (2020-24)'] = (pivot['2024'] - pivot['2020']).round(1)
    st.dataframe(pivot, width="stretch")


# ===================
# TAB: Retention Rate
# ===================
with tab_retention:
    st.header("Retention Rate Analysis")
    
    # Trend chart
    st.subheader("Retention Rate Trend (2020-2024)")
    
    trend_data = filtered_data.sort_values(['INSTITUTION_NAME', 'YEAR'])
    
    fig = px.line(
        trend_data,
        x='YEAR',
        y='FULL_TIME_RETENTION_RATE',
        color='INSTITUTION_NAME',
        markers=True,
        labels={
            'FULL_TIME_RETENTION_RATE': 'Retention Rate (%)',
            'YEAR': 'Year',
            'INSTITUTION_NAME': 'College'
        }
    )
    fig.update_layout(
        height=500,
        showlegend=(len(selected_colleges) <= 10)
    )
    fig.update_xaxes(dtick=1)
    fig.update_yaxes(rangemode='tozero')
    st.plotly_chart(fig, width="stretch")

    # Bar chart comparison
    st.subheader("By Year Comparison")

    fig = px.bar(
        trend_data.sort_values(['YEAR', 'INSTITUTION_NAME']),
        x='INSTITUTION_NAME',
        y='FULL_TIME_RETENTION_RATE',
        color='YEAR',
        barmode='group',
        labels={'INSTITUTION_NAME': '', 'FULL_TIME_RETENTION_RATE': 'Retention Rate (%)', 'YEAR': 'Year'},
        color_discrete_sequence=px.colors.sequential.Blues_r
    )
    fig.update_layout(height=450, xaxis_tickangle=-45)
    fig.update_yaxes(rangemode='tozero')
    st.plotly_chart(fig, width="stretch")

    # Data table
    st.subheader("Data Table")
    pivot = filtered_data.pivot_table(
        index='INSTITUTION_NAME', columns='YEAR', values='FULL_TIME_RETENTION_RATE', aggfunc='first'
    ).round(1)
    pivot.columns = [str(int(c)) for c in pivot.columns]
    if '2020' in pivot.columns and '2024' in pivot.columns:
        pivot['Change (2020-24)'] = (pivot['2024'] - pivot['2020']).round(1)
    st.dataframe(pivot, width="stretch")


# ===================
# TAB: Completions
# ===================
with tab_completion:
    st.header("Completions Analysis")
    
    # Trend chart
    st.subheader("Associate Degrees Trend (2020-2024)")
    
    trend_data = filtered_data.sort_values(['INSTITUTION_NAME', 'YEAR'])
    
    fig = px.line(
        trend_data,
        x='YEAR',
        y='ASSOCIATE_DEGREES',
        color='INSTITUTION_NAME',
        markers=True,
        labels={
            'ASSOCIATE_DEGREES': 'Degrees Awarded',
            'YEAR': 'Year',
            'INSTITUTION_NAME': 'College'
        }
    )
    fig.update_layout(
        height=500,
        showlegend=(len(selected_colleges) <= 10)
    )
    fig.update_xaxes(dtick=1)
    fig.update_yaxes(rangemode='tozero')
    st.plotly_chart(fig, width="stretch")

    # Bar chart comparison
    st.subheader("By Year Comparison")

    fig = px.bar(
        trend_data.sort_values(['YEAR', 'INSTITUTION_NAME']),
        x='INSTITUTION_NAME',
        y='ASSOCIATE_DEGREES',
        color='YEAR',
        barmode='group',
        labels={'INSTITUTION_NAME': '', 'ASSOCIATE_DEGREES': 'Degrees', 'YEAR': 'Year'},
        color_discrete_sequence=px.colors.sequential.Greens_r
    )
    fig.update_layout(height=450, xaxis_tickangle=-45)
    fig.update_yaxes(rangemode='tozero')
    st.plotly_chart(fig, width="stretch")

    # Total completions section
    st.subheader("Total Completions Trend")
    
    fig = px.line(
        trend_data,
        x='YEAR',
        y='TOTAL_COMPLETIONS',
        color='INSTITUTION_NAME',
        markers=True,
        labels={
            'TOTAL_COMPLETIONS': 'Total Completions',
            'YEAR': 'Year',
            'INSTITUTION_NAME': 'College'
        }
    )
    fig.update_layout(
        height=500,
        showlegend=(len(selected_colleges) <= 10)
    )
    fig.update_xaxes(dtick=1)
    fig.update_yaxes(rangemode='tozero')
    st.plotly_chart(fig, width="stretch")

    # Data table
    st.subheader("Data Table - Associate Degrees")
    pivot = filtered_data.pivot_table(
        index='INSTITUTION_NAME', columns='YEAR', values='ASSOCIATE_DEGREES', aggfunc='first'
    ).round(0)
    pivot.columns = [str(int(c)) for c in pivot.columns]
    if '2020' in pivot.columns and '2024' in pivot.columns:
        pivot['Change (2020-24)'] = (pivot['2024'] - pivot['2020']).round(0)
    st.dataframe(pivot, width="stretch")


# ===================
# TAB: Equity
# ===================
with tab_equity:
    st.header("Equity Analysis")
    
    latest_data = filtered_data[filtered_data['YEAR'] == 2024]
    
    # Summary metrics
    col1, col2 = st.columns(2)
    with col1:
        avg_gap_h = latest_data['EQUITY_GAP_HISPANIC'].mean()
        st.metric(
            "Avg Hispanic Graduation Gap (2024)",
            f"{avg_gap_h:.1f}pp" if pd.notna(avg_gap_h) else "N/A",
            help="Difference from white student graduation rate"
        )
    with col2:
        avg_gap_b = latest_data['EQUITY_GAP_BLACK'].mean()
        st.metric(
            "Avg Black Graduation Gap (2024)",
            f"{avg_gap_b:.1f}pp" if pd.notna(avg_gap_b) else "N/A",
            help="Difference from white student graduation rate"
        )
    
    st.divider()
    
    # Graduation rates by demographic
    st.subheader("Graduation Rate by Race/Ethnicity (2024)")
    
    demo_data = latest_data[['INSTITUTION_NAME', 'GRAD_RATE_HISPANIC', 'GRAD_RATE_BLACK', 'GRAD_RATE_WHITE']].melt(
        id_vars=['INSTITUTION_NAME'],
        var_name='Demographic',
        value_name='Graduation Rate'
    )
    demo_data['Demographic'] = demo_data['Demographic'].replace({
        'GRAD_RATE_HISPANIC': 'Hispanic',
        'GRAD_RATE_BLACK': 'Black',
        'GRAD_RATE_WHITE': 'White'
    })
    
    fig = px.bar(
        demo_data,
        x='INSTITUTION_NAME',
        y='Graduation Rate',
        color='Demographic',
        barmode='group',
        labels={'INSTITUTION_NAME': ''},
        color_discrete_map={'Hispanic': '#667eea', 'Black': '#764ba2', 'White': '#a0aec0'}
    )
    fig.update_layout(height=450, xaxis_tickangle=-45)
    fig.update_yaxes(rangemode='tozero')
    st.plotly_chart(fig, width="stretch")

    # Equity gaps scatter
    st.subheader("Equity Gaps by College (2024)")
    
    gap_data = latest_data[['INSTITUTION_NAME', 'EQUITY_GAP_HISPANIC', 'EQUITY_GAP_BLACK']].dropna()
    
    if not gap_data.empty:
        fig = px.scatter(
            gap_data,
            x='EQUITY_GAP_HISPANIC',
            y='EQUITY_GAP_BLACK',
            hover_name='INSTITUTION_NAME',
            labels={
                'EQUITY_GAP_HISPANIC': 'Hispanic Gap (pp)',
                'EQUITY_GAP_BLACK': 'Black Gap (pp)'
            },
            color_discrete_sequence=['#667eea']
        )
        fig.add_hline(y=0, line_dash="dash", line_color="gray")
        fig.add_vline(x=0, line_dash="dash", line_color="gray")
        fig.update_layout(height=450)
        st.plotly_chart(fig, width="stretch")
        
        st.caption("Positive = white students have higher graduation rate. Zero = equity achieved.")
    
    # Data table
    st.subheader("Equity Data Table (2024)")
    equity_display = latest_data[[
        'INSTITUTION_NAME', 'GRAD_RATE_HISPANIC', 'GRAD_RATE_BLACK', 'GRAD_RATE_WHITE',
        'EQUITY_GAP_HISPANIC', 'EQUITY_GAP_BLACK'
    ]].copy()
    equity_display.columns = ['College', 'Hispanic Rate', 'Black Rate', 'White Rate', 
                              'Hispanic Gap', 'Black Gap']
    st.dataframe(equity_display, width="stretch", hide_index=True)
