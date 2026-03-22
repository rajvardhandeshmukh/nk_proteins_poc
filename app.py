import streamlit as st

st.set_page_config(
    page_title="NK Proteins CMD CoPilot",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# app.py
# run with: streamlit run app.py

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from chatbot import ask, PROVIDERS, LLM_MODELS, DEFAULT_LLM_MODEL, DEFAULT_PROVIDER, save_chat_history, load_chat_history
from models import load_all

# --- MAIN APP LOGIC ------------------------------------------------------

st.sidebar.title("CoPilot Controls")
show_dashboard = st.sidebar.toggle("Show Analytics Dashboard", value=False)

st.sidebar.divider()
st.sidebar.subheader("AI Orchestration")

# Select Provider
provider_labels = [p["label"] for p in PROVIDERS]
default_p_idx = next((i for i, p in enumerate(PROVIDERS) if p["value"] == DEFAULT_PROVIDER), 0)
selected_provider_label = st.sidebar.selectbox("Select Provider", provider_labels, index=default_p_idx)
selected_provider_value = next(p["value"] for p in PROVIDERS if p["label"] == selected_provider_label)

# Filter Models
filtered_models = [m for m in LLM_MODELS if m["provider"] == selected_provider_value]
model_labels = [m["label"] for m in filtered_models]
model_values = [m["value"] for m in filtered_models]

# Default model
try:
    default_m_idx = model_values.index(DEFAULT_LLM_MODEL) if DEFAULT_LLM_MODEL in model_values else 0
except ValueError:
    default_m_idx = 0

selected_model_label = st.sidebar.selectbox("Select Model", model_labels, index=default_m_idx)
selected_model_value = next(m["value"] for m in filtered_models if m["label"] == selected_model_label)

st.title("NK Proteins CMD CoPilot")

# ── Load all model data once and cache ──────────────────────────────
if 'data' not in st.session_state:
    with st.spinner("Training models on your data... this takes ~60 seconds"):
        st.session_state.data    = load_all()
        # Fresh chat history on every refresh
        st.session_state.history = []
        st.session_state.pending = None

d = st.session_state.data
# ── ANALYTICS DASHBOARD (CONDITIONAL - AT THE TOP) ────────────────────
if show_dashboard:
    st.header("Analytics Deep-Dive Dashboard")
    
    # --- CAPABILITIES OVERVIEW -------------------------------------------
    with st.expander("Analytical Capabilities", expanded=False):
        cap_c1, cap_c2, cap_c3, cap_c4 = st.columns(4)
        with cap_c1:
            st.info("**1. Sales Analysis**")
            st.warning("**5. Cash Flow**")
        with cap_c2:
            st.success("**2. AI Insights**")
            st.error("**6. GST Support**")
        with cap_c3:
            st.info("**3. Reports**")
            st.warning("**7. Profitability**")
        with cap_c4:
            st.success("**4. Chat Interaction**")
            st.error("**8. Inventory Opt**")

    # ── ROW 1 — Sales metrics ────────────────────────────────────────────
    st.subheader("1. Predictive sales analysis")
    c1, c2, c3, c4 = st.columns(4)
    next3 = d['sales']['forecast_next_3_months']
    c1.metric("Next month forecast", f"₹{int(next3[0]['yhat']):,}")
    c2.metric("Last month actual", f"₹{d['sales']['total_revenue_last_month']:,}")
    c3.metric("Trend", d['sales']['trend'].title())
    c4.metric("Anomaly months", len(d['sales']['anomaly_months']))

    st.subheader("Key Performance Overview")
    c5, c6, c7, c8, c9 = st.columns(5)
    c5.metric("Overdue", f"₹{d['cashflow']['total_overdue']:,}")
    c6.metric("DSO", f"{d['cashflow']['dso_days']} days")
    c7.metric("Dead stock", d['inventory']['dead_stock_count'])
    c8.metric("Reorder alerts", d['inventory']['reorder_alerts'])
    c9.metric("GST risk", f"₹{d['gst']['total_itc_at_risk']:,}")

    # ── CHARTS ──────────────────────────────────────────────────────────
    st.divider()
    row3_c1, row3_c2 = st.columns(2)
    with row3_c1:
        st.markdown("**Sales Forecast Insights**")
        forecast_df = pd.DataFrame(next3)
        forecast_df['ds'] = pd.to_datetime(forecast_df['ds'])
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=forecast_df['ds'], y=forecast_df['yhat'], mode='lines+markers', name='Forecast'))
        fig1.update_layout(height=250, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig1, use_container_width=True)

    with row3_c2:
        st.markdown("**Top Profitable Products**")
        top_p = pd.DataFrame(d['sales']['top_5_products'])
        if not top_p.empty:
            fig2 = px.bar(top_p, x='revenue', y='product_name', orientation='h', color_discrete_sequence=['#1D9E75'])
            fig2.update_layout(height=250, margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig2, use_container_width=True)

    row4_c1, row4_c2 = st.columns(2)
    with row4_c1:
        st.markdown("**5. Cash flow: Top Slow Payers**")
        payers_df = pd.DataFrame(d['cashflow']['top_slow_payers'])
        if not payers_df.empty:
            fig3 = px.bar(payers_df, y='customer_name', x='total_overdue', orientation='h', color_discrete_sequence=['#F05D5E'])
            fig3.update_layout(height=250, margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig3, use_container_width=True)

    with row4_c2:
        st.markdown("**6. GST & tax support: Mismatch Types**")
        gst_df = pd.DataFrame(list(d['gst']['mismatch_type_breakdown'].items()), columns=['Type', 'Count'])
        if not gst_df.empty:
            fig4 = px.pie(gst_df, values='Count', names='Type', hole=0.4)
            fig4.update_layout(height=250, margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig4, use_container_width=True)

    row5_c1, row5_c2 = st.columns(2)
    with row5_c1:
        st.markdown("**8. Inventory optimization: Capital Locked**")
        ice_df = pd.DataFrame(d['inventory']['top_dead_skus'])
        if not ice_df.empty:
            fig5 = px.bar(ice_df, x='sku', y='total_value_inr', color_discrete_sequence=['#FFC107'])
            fig5.update_layout(height=250, margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig5, use_container_width=True)

    with row5_c2:
        st.markdown("**7. Profitability intelligence: Customer Risk**")
        margin_df = pd.DataFrame(d['profitability']['low_margin_customers'])
        if not margin_df.empty:
            fig6 = px.bar(margin_df, x='avg_margin', y='customer_name', orientation='h', color_discrete_sequence=['#9C27B0'])
            fig6.update_layout(height=250, margin=dict(l=0,r=0,t=10,b=0))
            st.plotly_chart(fig6, use_container_width=True)
    st.divider()

# ── CHAT INTERFACE (ALWAYS BELOW DASHBOARD) ───────────────────────────
st.markdown("**Executive Guidance Tools**")
cols = st.columns(4)
quick_qs = [
    "Predict next quarter sales with exact numbers",
    "Who are my top 5 slow paying customers?",
    "Which products should I discontinue and why?",
    "Generate a full executive report",
]
for i, q in enumerate(quick_qs):
    if cols[i].button(q, key=f"btn1_{i}", use_container_width=True):
        st.session_state.pending = q

cols2 = st.columns(4)
quick_qs2 = [
    "What is my cash flow risk this month?",
    "Show me all GST mismatches and ITC at risk",
    "Which dead stock items should I liquidate first?",
    "Which customers are in the low margin risk segment?",
]
for i, q in enumerate(quick_qs2):
    if cols2[i].button(q, key=f"btn2_{i}", use_container_width=True):
        st.session_state.pending = q

st.divider()

# ── AI chat interaction ────────────────────────────────────────────
st.markdown("**CoPilot Interaction**")

# Use a container for messages to keep them above the input
message_container = st.container()
with message_container:
    for user_msg, bot_msg in st.session_state.history:
        with st.chat_message("user"):
            st.write(user_msg)
        with st.chat_message("assistant"):
            st.write(bot_msg)

user_input = st.chat_input("Ask your CoPilot anything about NK Protein's data...")
question   = user_input or st.session_state.pop('pending', None)

if question:
    with message_container:
        with st.chat_message("user"):
            st.write(question)
        with st.chat_message("assistant"):
            with st.spinner("Analysing your data..."):
                answer = ask(question, st.session_state.history, d, selected_model_value, selected_provider_value)
            st.write(answer)
    st.session_state.history.append((question, answer))
    st.rerun()
