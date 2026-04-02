"""
NK Protein Agentic Hub (v4.8 - Modular Facade)
This file acts as a clean entry point. All logic is now moved to the /hub/ directory.
"""

import hub

# Expose core functions for the Streamlit app and other modules
ask_agentic = hub.ask_agentic
load_entities_from_db = hub.load_entities_from_db
get_mssql_engine = hub.get_mssql_engine
log_pipeline_telemetry = hub.log_pipeline_telemetry

# Self-initialize on import
try:
    load_entities_from_db()
except Exception as e:
    print(f"[*] Hub Facade: Entity load deferred ({str(e)})")
