"""
Texas Community College Benchmarking Dashboard
Streamlit app for visualizing IPEDS data from dbt marts
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
    page_icon="🎓",
    layout="wide"
)

# Snowflake connection - reads credentials from dbt profiles.yml
@st.cache_resource
def get_engine():
    # Read credentials from dbt profile
    profiles_path = Path.home() / '.dbt' / 'profiles.yml'
    with open(profiles_path) as f:
        profiles = yaml.safe_load(f)

    profile = profiles['texas_cc_benchmarking']['outputs']['dev']

    # Build SQLAlchemy connection string
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
    # Normalize column names to uppercase (Snowflake returns lowercase via SQLAlchemy)
    df.columns = df.columns.str.upper()
    return df

# Load mart data
@st.cache_data(ttl=600)
def load_institutions():
    return load_data("SELECT * FROM dim_texas_institutions")

@st.cache_data(ttl=600)
def load_outcomes():
    return load_data("SELECT * FROM fct_student_outcomes")

@st.cache_data(ttl=600)
def load_peer_comparison():
    return load_data("SELECT * FROM rpt_peer_comparison")

@st.cache_data(ttl=600)
def load_equity():
    return load_data("SELECT * FROM rpt_equity_dashboard")


# Sidebar
st.sidebar.title("🎓 Texas CC Benchmarking")
page = st.sidebar.radio(
    "Navigate",
    ["Overview", "Peer Comparison", "Equity Dashboard", "Institution Detail"]
)

# Cache clear button
if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Try to load data
try:
    institutions = load_institutions()
    outcomes = load_outcomes()
    peer_comparison = load_peer_comparison()
    equity = load_equity()
    data_loaded = True
except Exception as e:
    data_loaded = False
    st.error(f"Could not connect to Snowflake: {e}")
    st.info("Set environment variables: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD")
    st.stop()


# Page: Overview
if page == "Overview":
    st.title("Texas Community College Overview")

    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Institutions", len(institutions))
    with col2:
        avg_grad = outcomes['GRADUATION_RATE_150'].mean()
        st.metric("Avg Graduation Rate", f"{avg_grad:.1f}%" if pd.notna(avg_grad) else "N/A")
    with col3:
        avg_ret = institutions['OVERALL_RETENTION_RATE'].mean()
        st.metric("Avg Retention Rate", f"{avg_ret:.1f}%" if pd.notna(avg_ret) else "N/A")
    with col4:
        hsi_count = institutions['IS_HSI'].sum() if 'IS_HSI' in institutions.columns else 0
        st.metric("HSI Institutions", hsi_count)

    st.divider()

    # Charts
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Institutions by Size Tier")
        size_counts = institutions['SIZE_TIER'].value_counts().reset_index()
        size_counts.columns = ['Size Tier', 'Count']
        fig = px.pie(size_counts, values='Count', names='Size Tier', hole=0.4)
        st.plotly_chart(fig, width='stretch')

    with col2:
        st.subheader("Graduation vs Retention Rate")
        # Merge outcomes with institution info (outcomes already has INSTITUTION_NAME)
        scatter_data = outcomes.merge(institutions[['UNITID', 'SIZE_TIER']], on='UNITID')
        fig = px.scatter(
            scatter_data,
            x='FULL_TIME_RETENTION_RATE',
            y='GRADUATION_RATE_150',
            color='SIZE_TIER',
            hover_name='INSTITUTION_NAME',
            labels={
                'FULL_TIME_RETENTION_RATE': 'FT Retention Rate (%)',
                'GRADUATION_RATE_150': 'Graduation Rate 150% (%)'
            }
        )
        st.plotly_chart(fig, width='stretch')

    # Map visualization
    st.subheader("Institution Locations")
    if 'LATITUDE' in institutions.columns and 'LONGITUDE' in institutions.columns:
        map_data = institutions[['INSTITUTION_NAME', 'LATITUDE', 'LONGITUDE', 'SIZE_TIER', 'CITY']].dropna(subset=['LATITUDE', 'LONGITUDE'])
        if not map_data.empty:
            fig = px.scatter_map(
                map_data,
                lat='LATITUDE',
                lon='LONGITUDE',
                hover_name='INSTITUTION_NAME',
                hover_data=['CITY', 'SIZE_TIER'],
                color='SIZE_TIER',
                zoom=5,
                center={'lat': 31.0, 'lon': -99.5},  # Center on Texas
                map_style='carto-positron'
            )
            fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=500)
            st.plotly_chart(fig, width='stretch')
        else:
            st.warning("No institutions with valid coordinates found")
    else:
        st.info("Run `dbt run` to update models with latitude/longitude data")


# Page: Peer Comparison
elif page == "Peer Comparison":
    st.title("Peer Group Comparison")

    # Institution selector
    selected_institution = st.selectbox(
        "Select Institution",
        options=peer_comparison['INSTITUTION_NAME'].sort_values().unique()
    )

    inst_data = peer_comparison[peer_comparison['INSTITUTION_NAME'] == selected_institution].iloc[0]

    # Show peer group
    st.subheader(f"Peer Group: {inst_data['SIZE_TIER']} | {'HSI' if inst_data['IS_HSI'] else 'Non-HSI'} | {inst_data['PELL_TIER']} | {inst_data['URBANICITY']}")
    st.caption(f"Peer group size: {inst_data['PEER_GROUP_COUNT']} institutions")

    # Comparison metrics
    col1, col2, col3 = st.columns(3)

    with col1:
        delta = inst_data['GRAD_RATE_VS_PEERS']
        st.metric(
            "Graduation Rate",
            f"{inst_data['GRADUATION_RATE_150']:.1f}%",
            delta=f"{delta:+.1f}% vs peers" if pd.notna(delta) else None
        )

    with col2:
        delta = inst_data['SUCCESS_RATE_VS_PEERS']
        st.metric(
            "Success Rate",
            f"{inst_data['SUCCESS_RATE']:.1f}%" if pd.notna(inst_data['SUCCESS_RATE']) else "N/A",
            delta=f"{delta:+.1f}% vs peers" if pd.notna(delta) else None
        )

    with col3:
        delta = inst_data['FT_RETENTION_VS_PEERS']
        st.metric(
            "FT Retention Rate",
            f"{inst_data['FULL_TIME_RETENTION_RATE']:.1f}%" if pd.notna(inst_data['FULL_TIME_RETENTION_RATE']) else "N/A",
            delta=f"{delta:+.1f}% vs peers" if pd.notna(delta) else None
        )

    st.divider()

    # Bar chart comparison
    st.subheader("Institution vs Peer Average")

    metrics = ['GRADUATION_RATE_150', 'SUCCESS_RATE', 'FULL_TIME_RETENTION_RATE']
    peer_metrics = ['PEER_AVG_GRAD_RATE', 'PEER_AVG_SUCCESS_RATE', 'PEER_AVG_FT_RETENTION']
    labels = ['Graduation Rate', 'Success Rate', 'FT Retention']

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name=selected_institution,
        x=labels,
        y=[inst_data[m] for m in metrics],
        marker_color='#1f77b4'
    ))
    fig.add_trace(go.Bar(
        name='Peer Average',
        x=labels,
        y=[inst_data[m] for m in peer_metrics],
        marker_color='#d3d3d3'
    ))
    fig.update_layout(barmode='group', yaxis_title='Percentage (%)')
    st.plotly_chart(fig, width='stretch')


# Page: Equity Dashboard
elif page == "Equity Dashboard":
    st.title("Equity Dashboard (HB8 Metrics)")

    # Summary stats
    col1, col2, col3 = st.columns(3)
    with col1:
        large_gap = (equity['HISPANIC_GAP_SEVERITY'] == 'Large Gap (10%+)').sum()
        st.metric("Large Hispanic Gaps", large_gap)
    with col2:
        large_gap = (equity['BLACK_GAP_SEVERITY'] == 'Large Gap (10%+)').sum()
        st.metric("Large Black Gaps", large_gap)
    with col3:
        no_gap = ((equity['HISPANIC_GAP_SEVERITY'] == 'No Gap / Outperforming') |
                  (equity['BLACK_GAP_SEVERITY'] == 'No Gap / Outperforming')).sum()
        st.metric("No Gap / Outperforming", no_gap)

    st.divider()

    # Equity gap chart
    st.subheader("Equity Gaps by Institution")

    gap_data = equity[['INSTITUTION_NAME', 'EQUITY_GAP_HISPANIC', 'EQUITY_GAP_BLACK']].dropna()
    gap_data = gap_data.melt(
        id_vars=['INSTITUTION_NAME'],
        var_name='Group',
        value_name='Gap'
    )
    gap_data['Group'] = gap_data['Group'].map({
        'EQUITY_GAP_HISPANIC': 'Hispanic',
        'EQUITY_GAP_BLACK': 'Black'
    })

    fig = px.bar(
        gap_data.sort_values('Gap', ascending=False).head(30),
        x='INSTITUTION_NAME',
        y='Gap',
        color='Group',
        barmode='group',
        labels={'Gap': 'Equity Gap (pp)', 'INSTITUTION_NAME': 'Institution'},
        title='Top 30 Institutions by Equity Gap'
    )
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, width='stretch')

    # Completion equity index
    st.subheader("Completion Equity Index")
    st.caption("Values > 1.0 = group overrepresented in completions; < 1.0 = underrepresented")

    cei_cols = ['INSTITUTION_NAME', 'HISPANIC_COMPLETION_EQUITY_INDEX',
                'BLACK_COMPLETION_EQUITY_INDEX', 'WHITE_COMPLETION_EQUITY_INDEX']
    cei_data = equity[cei_cols].dropna()

    fig = px.scatter(
        cei_data,
        x='HISPANIC_COMPLETION_EQUITY_INDEX',
        y='BLACK_COMPLETION_EQUITY_INDEX',
        hover_name='INSTITUTION_NAME',
        labels={
            'HISPANIC_COMPLETION_EQUITY_INDEX': 'Hispanic Completion Equity Index',
            'BLACK_COMPLETION_EQUITY_INDEX': 'Black Completion Equity Index'
        }
    )
    fig.add_hline(y=1.0, line_dash="dash", line_color="gray")
    fig.add_vline(x=1.0, line_dash="dash", line_color="gray")
    st.plotly_chart(fig, width='stretch')


# Page: Institution Detail
elif page == "Institution Detail":
    st.title("Institution Detail")

    selected = st.selectbox(
        "Select Institution",
        options=institutions['INSTITUTION_NAME'].sort_values().unique()
    )

    inst = institutions[institutions['INSTITUTION_NAME'] == selected].iloc[0]
    outcome = outcomes[outcomes['UNITID'] == inst['UNITID']].iloc[0] if inst['UNITID'] in outcomes['UNITID'].values else None
    eq = equity[equity['UNITID'] == inst['UNITID']].iloc[0] if inst['UNITID'] in equity['UNITID'].values else None

    # Institution info
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Institution Info")
        st.write(f"**City:** {inst['CITY']}")
        st.write(f"**Size:** {inst['SIZE_CATEGORY_NAME']}")
        st.write(f"**Locale:** {inst['LOCALE_TYPE']}")
        st.write(f"**HSI Status:** {'Yes' if inst['IS_HSI'] else 'No'}")
        st.write(f"**Pell Tier:** {inst['PELL_TIER']}")

    with col2:
        st.subheader("Demographics")
        demo_data = pd.DataFrame({
            'Group': ['Hispanic', 'Black', 'White', 'Other'],
            'Percentage': [
                inst['PCT_HISPANIC'] or 0,
                inst['PCT_BLACK'] or 0,
                inst['PCT_WHITE'] or 0,
                100 - (inst['PCT_HISPANIC'] or 0) - (inst['PCT_BLACK'] or 0) - (inst['PCT_WHITE'] or 0)
            ]
        })
        fig = px.pie(demo_data, values='Percentage', names='Group', hole=0.4)
        st.plotly_chart(fig, width='stretch')

    st.divider()

    # Outcomes
    if outcome is not None:
        st.subheader("Student Outcomes")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Graduation Rate", f"{outcome['GRADUATION_RATE_150']:.1f}%" if pd.notna(outcome['GRADUATION_RATE_150']) else "N/A")
        with col2:
            st.metric("Success Rate", f"{outcome['SUCCESS_RATE']:.1f}%" if pd.notna(outcome['SUCCESS_RATE']) else "N/A")
        with col3:
            st.metric("FT Retention", f"{inst['FULL_TIME_RETENTION_RATE']:.1f}%" if pd.notna(inst['FULL_TIME_RETENTION_RATE']) else "N/A")
        with col4:
            st.metric("Associate Degrees", f"{outcome['ASSOCIATE_DEGREES']:,.0f}" if pd.notna(outcome['ASSOCIATE_DEGREES']) else "N/A")
