import streamlit as st

def fmt_inr(value):
    """
    Format numeric values to Indian Rupee (INR) representation.
    Handles Crores (Cr), Lakhs (L), and standard comma-separated formats.
    """
    if value is None or not isinstance(value, (int, float)):
        return "N/A"
    if abs(value) >= 10000000:
        return f"₹{value/10000000:.2f} Cr"
    elif abs(value) >= 100000:
        return f"₹{value/100000:.2f} L"
    else:
        return f"₹{int(value):,}"

def render_custom_card(label, value):
    """
    Renders a standalone premium executive metric card with CSS binding.
    """
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
    </div>
    """, unsafe_allow_html=True)

def render_multi_pillar_cards(raw_metrics):
    """
    Renders a 4-column layout showing key KPIs from all business pillars
    (Sales, Cashflow, GST, Inventory) directly within the chat stream.
    """
    sales = raw_metrics.get('sales', {})
    cashflow = raw_metrics.get('cashflow', {})
    gst = raw_metrics.get('gst', {})
    inventory = raw_metrics.get('inventory', {})
    
    st.markdown("---")
    st.caption("LIVE EXECUTIVE INSIGHTS — PARALLEL ML ENGINE VERIFIED")
    
    c1, c2, c3, c4 = st.columns(4)
    
    with c1:
        forecast = sales.get('forecast_next_3_months', [{}])
        next_val = forecast[0].get('yhat', 0) if forecast else 0
        render_custom_card("Sales Forecast", fmt_inr(next_val))
        render_custom_card("Trend Direction", (sales.get('trend') or 'N/A').upper())
    
    with c2:
        render_custom_card("Total Overdue", fmt_inr(cashflow.get('total_overdue', 0)))
        render_custom_card("DSO Status", f"{cashflow.get('dso_days', 0)} Days")
    
    with c3:
        render_custom_card("ITC at Risk", fmt_inr(gst.get('total_itc_at_risk', 0)))
        render_custom_card("Anomaly Flags", gst.get('isolation_forest_flags', 0))
    
    with c4:
        render_custom_card("Capital Locked", fmt_inr(inventory.get('total_capital_locked', 0)))
        render_custom_card("Inventory Risk", f"{inventory.get('dead_stock_pct', 0)}% Dead")

def render_bot_response(bot_msg):
    """
    Orchestrates the total rendering of an AI response.
    Handles confidence-based filtering (Optimal/Caution/Blocked), 
    markdown narrative, dashboard cards, and telemetry expanders.
    """
    if not isinstance(bot_msg, dict) or 'narrative' not in bot_msg:
        st.markdown(bot_msg)
        return
    
    confidence = bot_msg.get("_confidence", "high")
    warnings   = bot_msg.get("_warnings", [])

    # 1. Handle Blocked State (Safety first: suppress narrative if accuracy is low)
    if confidence == "blocked":
        st.error("ACCURACY ALERT — Forecast suppressed due to high variance (>25% MAPE).")
        for w in warnings:
            st.warning(w)
        _render_telemetry_and_preview(bot_msg)
        return

    # 2. Handle Caution State (Show warnings alongside content)
    if confidence == "caution":
        for w in warnings:
            st.warning(w)

    # 3. Main Narrative
    st.markdown(bot_msg['narrative'])
    
    # 4. Contextual Dashboards (Triggered by multi-pillar intent)
    if bot_msg.get('intent') == 'multi_pillar' and 'raw_metrics' in bot_msg:
        render_multi_pillar_cards(bot_msg['raw_metrics'])
    
    # 5. Internal Telemetry (Expandable technical audit)
    _render_telemetry_and_preview(bot_msg)

def _render_telemetry_and_preview(bot_msg):
    """
    Internal helper to render SQL, Row counts, and Dataframes in an expander.
    """
    with st.expander("Telemetry and Verification", expanded=False):
        t1, t2, t3 = st.columns(3)
        t1.metric("Execution Intent", (bot_msg.get('intent') or 'N/A').upper())
        t2.metric("Rows Analyzed", bot_msg.get('rows', 0))
        t3.metric("Model", bot_msg.get('model', 'N/A'))
        
        sql_text = bot_msg.get('sql', 'N/A')
        if sql_text and sql_text != 'N/A':
            st.code(sql_text, language="sql")
        
        if bot_msg.get('df_preview') is not None:
            st.caption("**Data Sample (first 5 rows)**")
            st.dataframe(bot_msg['df_preview'], use_container_width=True, hide_index=True)
        elif bot_msg.get('df_markdown'):
            st.caption("**Data Sample**")
            st.markdown(bot_msg['df_markdown'])
