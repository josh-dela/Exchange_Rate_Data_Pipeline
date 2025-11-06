"""Streamlit dashboard for visualizing exchange rate trends."""
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, date, timedelta
from typing import Optional
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from src.dashboard.data_generator import generate_dummy_data, generate_dummy_dataframe
from src.load.supabase_loader import SupabaseLoader
from src.utils.config import Config
from src.utils.logger import get_logger

logger = get_logger("dashboard.app")

# Page configuration
st.set_page_config(
    page_title="Exchange Rate Dashboard",
    page_icon="📊",
    layout="wide"
)

# Initialize Supabase loader
supabase_loader = SupabaseLoader()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data_from_supabase(days: int = 30) -> Optional[pd.DataFrame]:
    """Load data from Supabase.
    
    Args:
        days: Number of days of data to load
        
    Returns:
        DataFrame with exchange rate data or None if not available
    """
    if not supabase_loader.is_configured():
        return None
    
    try:
        # Calculate date range
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        
        # Query Supabase
        response = supabase_loader.client.table("exchange_rates").select("*").gte(
            "date", start_date.isoformat()
        ).lte("date", end_date.isoformat()).order("date").execute()
        
        if response.data:
            df = pd.DataFrame(response.data)
            df['date'] = pd.to_datetime(df['date'])
            return df
        
        return None
        
    except Exception as e:
        logger.error(f"Error loading data from Supabase: {e}")
        return None


def main():
    """Main dashboard function."""
    st.title("📊 Exchange Rate Dashboard")
    st.markdown("Visualize currency exchange rate trends (USD, EUR, GBP → GHS)")
    
    # Sidebar configuration
    st.sidebar.header("Configuration")
    
    # Data source selection
    use_dummy_data = st.sidebar.checkbox(
        "Use Dummy Data",
        value=not supabase_loader.is_configured(),
        help="Use dummy data if Supabase is not configured"
    )
    
    # Date range selection
    days_back = st.sidebar.slider(
        "Days of History",
        min_value=7,
        max_value=365,
        value=30,
        help="Number of days of historical data to display"
    )
    
    # Currency selection
    selected_currencies = st.sidebar.multiselect(
        "Select Currencies",
        options=Config.BASE_CURRENCIES,
        default=Config.BASE_CURRENCIES,
        help="Choose which currencies to display"
    )
    
    # Load data
    if use_dummy_data or not supabase_loader.is_configured():
        st.sidebar.info("ℹ️ Using dummy data. Configure Supabase to use real data.")
        df = generate_dummy_dataframe(days=days_back, currencies=selected_currencies)
        data_source = "Dummy Data"
    else:
        df = load_data_from_supabase(days=days_back)
        if df is None:
            st.warning("⚠️ Could not load data from Supabase. Falling back to dummy data.")
            df = generate_dummy_dataframe(days=days_back, currencies=selected_currencies)
            data_source = "Dummy Data (Fallback)"
        else:
            # Filter by selected currencies
            if selected_currencies:
                df = df[df['base_currency'].isin(selected_currencies)]
            data_source = "Supabase"
    
    if df is None or df.empty:
        st.error("No data available to display.")
        return
    
    # Filter by selected currencies if needed
    if selected_currencies:
        df = df[df['base_currency'].isin(selected_currencies)]
    
    # Main metrics
    st.header("📈 Key Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        latest_date = df['date'].max()
        st.metric("Latest Data Date", latest_date.strftime("%Y-%m-%d"))
    
    with col2:
        total_records = len(df)
        st.metric("Total Records", f"{total_records:,}")
    
    with col3:
        unique_currencies = df['base_currency'].nunique()
        st.metric("Currencies", unique_currencies)
    
    with col4:
        avg_rate = df['rate'].mean()
        st.metric("Average Rate", f"{avg_rate:.4f}")
    
    st.markdown("---")
    
    # Time series visualization
    st.header("📉 Exchange Rate Trends Over Time")
    
    # Line chart for all currencies
    fig_line = px.line(
        df,
        x='date',
        y='rate',
        color='currency_pair',
        title='Exchange Rate Trends',
        labels={'rate': 'Exchange Rate (GHS)', 'date': 'Date'},
        markers=True
    )
    fig_line.update_layout(
        hovermode='x unified',
        height=500,
        xaxis_title="Date",
        yaxis_title="Rate (GHS)"
    )
    st.plotly_chart(fig_line, use_container_width=True)
    
    # Currency comparison
    if len(selected_currencies) > 1:
        st.subheader("Currency Comparison")
        
        # Calculate latest rates for each currency
        latest_rates = df.groupby('currency_pair')['rate'].last().reset_index()
        latest_rates = latest_rates.sort_values('rate', ascending=False)
        
        fig_bar = px.bar(
            latest_rates,
            x='currency_pair',
            y='rate',
            title='Latest Exchange Rates by Currency',
            labels={'rate': 'Rate (GHS)', 'currency_pair': 'Currency Pair'},
            color='rate',
            color_continuous_scale='Viridis'
        )
        fig_bar.update_layout(height=400)
        st.plotly_chart(fig_bar, use_container_width=True)
    
    # Data table
    st.header("📋 Data Table")
    
    # Filters
    col1, col2 = st.columns(2)
    with col1:
        currency_filter = st.selectbox(
            "Filter by Currency",
            options=['All'] + list(df['base_currency'].unique()),
            index=0
        )
    
    with col2:
        date_range = st.date_input(
            "Date Range",
            value=(df['date'].min().date(), df['date'].max().date()),
            min_value=df['date'].min().date(),
            max_value=df['date'].max().date()
        )
    
    # Apply filters
    filtered_df = df.copy()
    if currency_filter != 'All':
        filtered_df = filtered_df[filtered_df['base_currency'] == currency_filter]
    
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
        filtered_df = filtered_df[
            (filtered_df['date'].dt.date >= start_date) &
            (filtered_df['date'].dt.date <= end_date)
        ]
    
    # Display table
    display_df = filtered_df[['date', 'currency_pair', 'rate', 'base_currency', 'target_currency']].copy()
    display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')
    display_df = display_df.sort_values('date', ascending=False)
    
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )
    
    # Statistics
    st.header("📊 Statistics")
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Rate Statistics by Currency")
        stats_df = df.groupby('currency_pair')['rate'].agg([
            'mean', 'std', 'min', 'max'
        ]).round(4)
        stats_df.columns = ['Mean', 'Std Dev', 'Min', 'Max']
        st.dataframe(stats_df, use_container_width=True)
    
    with col2:
        st.subheader("Data Source Information")
        st.info(f"**Data Source:** {data_source}")
        st.info(f"**Records:** {len(df):,}")
        st.info(f"**Date Range:** {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}")
    
    # Footer
    st.markdown("---")
    st.markdown(
        f"<div style='text-align: center; color: gray;'>"
        f"Exchange Rate ETL Pipeline Dashboard | Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        f"</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()

