import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from interface.components import fmt_inr, render_custom_card

def render_analytics_dashboard(data):
    """
    NK Protein CoPilot — Executive Analytics Dashboard
    ==================================================
    Handles high-fidelity KPI cards and Plotly-based charts 
    for real-time business health monitoring.
    """
    st.header("Executive Analytics Dashboard")
    
    # 1. KPI Strategy Row (Top-line Metrics)
    k1, k2, k3, k4, k5 = st.columns(5)
    sales_forecast = data['sales']['forecast_next_3_months']
    
    with k1: render_custom_card("Sales Forecast", fmt_inr(sales_forecast[0]['yhat']))
    with k2: render_custom_card("Total Overdue", fmt_inr(data['cashflow']['total_overdue']))
    with k3: render_custom_card("DSO Current", f"{data['cashflow']['dso_days']} Days")
    with k4: render_custom_card("Dead Stock", data['inventory']['dead_stock_count'])
    with k5: render_custom_card("ITC Compliance", fmt_inr(data['gst']['total_itc_at_risk']))
    
    st.divider()
    
    # 2. Charts Row 1: Sales & Revenue Dynamics
    ch1, ch2 = st.columns(2)
    with ch1:
        st.markdown("**Sales Forecast (Next 3 Months)**")
        f_df = pd.DataFrame(sales_forecast)
        f_df['ds'] = pd.to_datetime(f_df['ds'])
        
        fig1 = go.Figure()
        # Main Forecast Line (Emerald/Teal)
        fig1.add_trace(go.Scatter(x=f_df['ds'], y=f_df['yhat'], mode='lines+markers', name='Forecast', line=dict(color='#2dd4bf', width=3)))
        # Confidence Bands (Translucent)
        fig1.add_trace(go.Scatter(x=f_df['ds'], y=f_df['yhat_upper'], mode='lines', name='Upper', line=dict(color='rgba(45,212,191,0.1)', dash='dot')))
        fig1.add_trace(go.Scatter(x=f_df['ds'], y=f_df['yhat_lower'], mode='lines', name='Lower', line=dict(color='rgba(45,212,191,0.1)', dash='dot'), fill='tonexty'))
        
        _apply_monochrome_layout(fig1)
        st.plotly_chart(fig1, use_container_width=True)

    with ch2:
        st.markdown("**Top 5 Products by Revenue**")
        top_p = pd.DataFrame(data['sales']['top_5_products'])
        if not top_p.empty:
            fig2 = px.bar(top_p, x='revenue', y='product_name', orientation='h', color_discrete_sequence=['#2dd4bf'])
            _apply_monochrome_layout(fig2)
            st.plotly_chart(fig2, use_container_width=True)

    # 3. Charts Row 2: Cashflow & GST Risk
    ch3, ch4 = st.columns(2)
    with ch3:
        st.markdown("**Top Slow Payers**")
        payers_df = pd.DataFrame(data['cashflow']['top_slow_payers'])
        if not payers_df.empty:
            fig3 = px.bar(payers_df, y='customer_name', x='total_overdue', orientation='h', color_discrete_sequence=['#f59e0b'])
            _apply_monochrome_layout(fig3)
            st.plotly_chart(fig3, use_container_width=True)

    with ch4:
        st.markdown("**GST Mismatch Types**")
        gst_df = pd.DataFrame(list(data['gst']['mismatch_type_breakdown'].items()), columns=['Type', 'Count'])
        if not gst_df.empty:
            # High-Contrast Donut Chart
            fig4 = px.pie(gst_df, values='Count', names='Type', hole=0.4, color_discrete_sequence=['#ef4444','#f59e0b','#2dd4bf','#8b5cf6'])
            fig4.update_layout(margin=dict(l=0,r=0,t=10,b=0), plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='#ffffff')
            st.plotly_chart(fig4, use_container_width=True)

    # 4. Charts Row 3: Inventory & Margin Analysis
    ch5, ch6 = st.columns(2)
    with ch5:
        st.markdown("**Top Dead Stock (Capital Locked)**")
        dead_df = pd.DataFrame(data['inventory']['top_dead_skus'])
        if not dead_df.empty:
            fig5 = px.bar(dead_df, x='sku', y='total_value_inr', color_discrete_sequence=['#8b5cf6'])
            _apply_monochrome_layout(fig5)
            st.plotly_chart(fig5, use_container_width=True)

    with ch6:
        st.markdown("**Low Margin Customers**")
        margin_df = pd.DataFrame(data['profitability']['low_margin_customers'])
        if not margin_df.empty:
            fig6 = px.bar(margin_df, x='avg_margin', y='customer_name', orientation='h', color_discrete_sequence=['#ef4444'])
            _apply_monochrome_layout(fig6)
            st.plotly_chart(fig6, use_container_width=True)
    
    st.divider()

def _apply_monochrome_layout(fig):
    """
    Internal helper to apply consistent B&W layout themes across all executive charts.
    """
    fig.update_layout(
        height=280, 
        margin=dict(l=0,r=0,t=10,b=0), 
        plot_bgcolor='rgba(0,0,0,0)', 
        paper_bgcolor='rgba(0,0,0,0)', 
        font_color='#ffffff', 
        showlegend=False
    )
    fig.update_xaxes(showgrid=False, zeroline=False, color='#71717a')
    fig.update_yaxes(showgrid=True, gridcolor='#1f1f1f', zeroline=False, color='#71717a')
