import streamlit as st

def inject_custom_css():
    """
    NK Protein CoPilot — Executive Monochrome Design System (v6.0)
    ============================================================
    Injects high-contrast pure black and white styling, elite serif 
    typography (Newsreader), and ultra-minimalist UI components.
    """
    st.markdown(
        """
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Newsreader:ital,opsz,wght@0,6..72,200..800;1,6..72,200..800&display=swap" rel="stylesheet">
        <style>
            /* 1. Global Reset & Monochrome Core */
            html, body, [data-testid="stAppViewContainer"], .stApp {
                font-family: 'Inter', sans-serif !important;
                background-color: #000000 !important;
                color: #ffffff !important;
            }

            /* 2. Executive Typography (Claude-style Narrative) */
            h1, h2, h3, h4, .stMarkdown h1, .stMarkdown h2, .stMarkdown h3, 
            [data-testid="stChatMessage"] [data-testid="stMarkdownContainer"] p,
            [data-testid="stChatMessage"] [data-testid="stMarkdownContainer"] li,
            .metric-value, .narrative-text {
                font-family: 'Newsreader', serif !important;
                font-weight: 500 !important;
            }

            /* 3. High-Fidelity Narrative Specifics */
            [data-testid="stChatMessage"] [data-testid="stMarkdownContainer"] p {
                font-size: 1.18rem !important;
                line-height: 1.7 !important;
                color: #ffffff !important;
                letter-spacing: -0.01em !important;
            }

            /* 4. Branding & Title Treatment */
            h1 { 
                font-size: 2.4rem !important; 
                font-weight: 600 !important; 
                letter-spacing: -0.03em !important;
            }

            /* 5. Minimalist UI Cleanup (Header/Toolbar Restoration) */
            [data-testid="stMetric"], .stDeployButton, #MainMenu, [data-testid="stDecoration"] {
                display: none !important;
            }
            [data-testid="stHeader"] {
                background-color: rgba(0,0,0,0) !important;
            }
            
            /* Sidebar Toggle (White on Black) */
            button[data-testid="stExpandSidebarButton"] {
                display: flex !important;
                visibility: visible !important;
                color: #ffffff !important;
            }

            /* 6. Sharp Executive Cards */
            .metric-card {
                background: #0a0a0a;
                border: 1px solid #1f1f1f;
                border-radius: 8px;
                padding: 1.5rem;
                margin-bottom: 1.2rem;
                transition: border-color 0.2s ease;
            }
            .metric-card:hover {
                border-color: #ffffff;
            }
            .metric-label {
                text-transform: uppercase;
                font-size: 0.65rem;
                font-weight: 700;
                letter-spacing: 0.15em;
                color: #71717a;
                margin-bottom: 0.4rem;
            }
            .metric-value {
                font-size: 1.9rem;
                font-weight: 500;
                color: #ffffff;
            }

            /* 7. Sidebar & Control Overrides */
            [data-testid="stSidebar"] {
                background-color: #050505 !important;
                border-right: 1px solid #1f1f1f !important;
            }
            .stSidebar [data-testid="stMarkdownContainer"] p {
                 font-family: 'Inter', sans-serif !important;
                 font-size: 0.85rem !important;
                 font-weight: 500 !important;
                 color: #a1a1aa !important;
            }

            /* 8. Interactions: Inputs & Buttons */
            [data-testid="stChatInput"] {
                border-radius: 8px !important;
                background: #0a0a0a !important;
                border: 1px solid #1f1f1f !important;
            }
            [data-testid="stChatInput"]:focus-within {
                border-color: #ffffff !important;
            }
            
            .stButton > button {
                border: 1px solid #1f1f1f !important;
                background: transparent !important;
                color: #a1a1aa !important;
                border-radius: 6px !important;
                font-weight: 500 !important;
                transition: all 0.2s ease;
            }
            .stButton > button:hover {
                border-color: #ffffff !important;
                color: #ffffff !important;
                background: #0a0a0a !important;
            }

            /* 9. Utility */
            hr { border-color: #1f1f1f !important; }
            ::-webkit-scrollbar { width: 6px; }
            ::-webkit-scrollbar-track { background: #000000; }
            ::-webkit-scrollbar-thumb { background: #1f1f1f; border-radius: 10px; }
            ::-webkit-scrollbar-thumb:hover { background: #52525b; }
        </style>
        """,
        unsafe_allow_html=True
    )
