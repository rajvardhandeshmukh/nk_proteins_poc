"""
NK Protein CoPilot — Executive Dashboard (v6.0 Modular)
======================================================
This is the main entry point for the Streamlit application.
Logic is modularised across the 'interface/' package.
"""

import streamlit as st

# 1. Global Page Configuration
st.set_page_config(
    page_title="NKP Executive CoPilot",
    page_icon="https://www.google.com/s2/favicons?domain=nkproteins.com",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# 2. Package Imports
from chatbot import (
    ask, PROVIDERS, LLM_MODELS, 
    DEFAULT_LLM_MODEL, DEFAULT_PROVIDER
)
from models import load_all
from interface.styles import inject_custom_css
from interface.sidebar import render_sidebar
from interface.components import render_bot_response
from interface.dashboard import render_analytics_dashboard

# 3. Inject CSS & Elite Typography
inject_custom_css()

# 4. Sidebar Orchestration
show_dashboard, selected_model_value, selected_provider_value = render_sidebar(
    PROVIDERS, LLM_MODELS, DEFAULT_PROVIDER, DEFAULT_LLM_MODEL
)

# 5. Main Content Header
st.title("NK Proteins Executive CoPilot")
st.markdown(
    "<p style='color: #a1a1aa; font-size: 0.95rem; font-weight: 500;'>"
    "Institutional Grade Intelligence Layer — Sales, Receivables, Compliance, Supply Chain"
    "</p>", 
    unsafe_allow_html=True
)

# 6. Data & History Initialisation
if 'data' not in st.session_state:
    with st.spinner("Training models on your data... this takes ~60 seconds"):
        # Load unified analytics state from backend models
        st.session_state.data    = load_all()
        st.session_state.history = []
        st.session_state.pending = None

# Local references to session state
d = st.session_state.data

# 7. Rendering Logic: Executive Dashboard
if show_dashboard:
    render_analytics_dashboard(d)

# 8. Rendering Logic: Quick Action Buttons
st.markdown("Recommended Insights")
quick_qs_top = [
    "What are the biggest risks across my business right now?",
    "Predict next quarter sales with exact numbers",
    "Who are my top 5 slow paying customers?",
    "Which dead stock items should I liquidate first?",
]
quick_qs_bot = [
    "How is the North region performing?",
    "What is my cash flow risk this month?",
    "Show me all GST mismatches and ITC at risk",
    "Which products should I discontinue and why?",
]

# Grid Layout for Buttons
for q_list in [quick_qs_top, quick_qs_bot]:
    cols = st.columns(4)
    for i, q in enumerate(q_list):
        if cols[i].button(q, key=f"btn_{q[:10]}", use_container_width=True):
            st.session_state.pending = q

st.divider()

# 9. Rendering Logic: Chat Interface
st.markdown("**CoPilot Interaction**")

# Container for message history scroll
message_container = st.container()
with message_container:
    for user_msg, bot_msg in st.session_state.history:
        with st.chat_message("user"):
            st.markdown(user_msg)
        with st.chat_message("assistant"):
            render_bot_response(bot_msg)

# Input handling (Normal input or Quick Action button trigger)
user_input = st.chat_input("Ask your CoPilot anything about NK Protein's data...")
question   = user_input or st.session_state.pop('pending', None)

if question:
    with message_container:
        with st.chat_message("user"):
            st.markdown(question)
        with st.chat_message("assistant"):
            with st.spinner("Analysing your data..."):
                # Call AI Orchestration Hub
                answer = ask(
                    question, 
                    st.session_state.history, 
                    d, 
                    selected_model_value, 
                    selected_provider_value
                )
            render_bot_response(answer)

    # Persistence & State Refresh
    st.session_state.history.append((question, answer))
    st.rerun()

# 10. Institutional Compliance Footer
st.markdown("---")
st.caption(
    "Security: Institutional cloud compute (OpenAI/Google). "
    "Encrypted external processing. "
    "Select Local mode for on-premise air-gapped operation."
)
