import streamlit as st
from datetime import datetime

def render_sidebar(PROVIDERS, LLM_MODELS, DEFAULT_PROVIDER, DEFAULT_LLM_MODEL):
    """
    NK Protein CoPilot — Sidebar Orchestrator
    ========================================
    Returns:
        (tuple): (show_dashboard_bool, selected_model_val, selected_provider_val)
    """
    st.sidebar.title("Operational Controls")
    st.sidebar.divider()
    
    # 1. Main UI Toggles
    show_dashboard = st.sidebar.toggle("Analytics Dashboard", value=False)
    st.sidebar.divider()
    
    # 2. AI Orchestration Selection
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
    
    # Selection logic for specific Model
    try:
        default_m_idx = model_values.index(DEFAULT_LLM_MODEL) if DEFAULT_LLM_MODEL in model_values else 0
    except ValueError:
        default_m_idx = 0
    
    selected_model_label = st.sidebar.selectbox("Select Model", model_labels, index=default_m_idx)
    selected_model_value = next(m["value"] for m in filtered_models if m["label"] == selected_model_label)
    
    # 3. Model Health Panel (Audit Logs)
    st.sidebar.divider()
    st.sidebar.subheader("Model Integrity Check")
    
    _render_health_check()
    
    return show_dashboard, selected_model_value, selected_provider_value

def _render_health_check():
    """
    Internal helper to fetch and display MAPE health stats.
    Prioritises OPTIMAL models at the top.
    """
    try:
        from prediction_logger import get_latest_training_stats
        stats = get_latest_training_stats()
        
        if stats:
            # Sort: Optimal (lowest MAPE) first, Active-only models last
            sorted_stats = sorted(
                stats.items(), 
                key=lambda x: (x[1].get('mape') is None, x[1].get('mape') or 0)
            )
            
            for i, (model_name, info) in enumerate(sorted_stats):
                mape_val = info.get('mape')
                ts       = info.get('timestamp', 'N/A')
                
                # Format Timestamp for Executive View
                try:
                    dt = datetime.fromisoformat(ts)
                    ts_display = dt.strftime("%b %d, %H:%M")
                except Exception:
                    ts_display = ts[:16] if len(ts) > 16 else ts
                
                if mape_val is not None:
                    if mape_val <= 10:
                        st_color, st_label = "#10b981", "● OPTIMAL"
                    elif mape_val <= 25:
                        st_color, st_label = "#f59e0b", "● STABLE"
                    else:
                        st_color, st_label = "#ef4444", "● DEGRADED"
                    
                    st.sidebar.markdown(f"**{model_name}**")
                    st.sidebar.markdown(
                        f"<span style='color: {st_color}; font-size: 0.8rem; font-weight: 600;'>{st_label}</span> — "
                        f"<span style='color: #ffffff; font-size: 0.85rem;'>MAPE: {mape_val:.1f}%</span>", 
                        unsafe_allow_html=True
                    )
                else:
                    st.sidebar.markdown(f"**{model_name}**")
                    st.sidebar.markdown(
                        f"<span style='color: #3b82f6; font-size: 0.8rem; font-weight: 600;'>● ACTIVE</span>", 
                        unsafe_allow_html=True
                    )
                
                st.sidebar.caption(f"Sync: {ts_display}")
                
                # Sub-dividers for clarity
                if i < len(sorted_stats) - 1:
                    st.sidebar.markdown("<div style='margin: 0.5rem 0; border-top: 1px solid #1f1f1f;'></div>", unsafe_allow_html=True)
        else:
            st.sidebar.caption("System initializing...")
    except Exception:
        st.sidebar.caption("Monitoring offline.")
