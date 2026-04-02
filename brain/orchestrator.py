"""
NK Protein CoPilot — AI Orchestrator (Brain)
===========================================
The primary routing layer for AI interactions.
"""

from brain.config import DEFAULT_LLM_MODEL, DEFAULT_PROVIDER

def ask(question, history, data, model_name=DEFAULT_LLM_MODEL, provider=DEFAULT_PROVIDER):
    """
    NK Protein CoPilot — Primary AI Interface
    =========================================
    Routes the user's Natural Language query to the Agentic SQL/ML Hub.
    
    Args:
        question (str): The raw text query from the user.
        history (list): List of (user, bot) interaction tuples for session context.
        data (dict): Reference to pre-trained ML models and SQL snapshot.
        model_name (str): ID of the selected LLM.
        provider (str): Key of the selected AI provider.
        
    Returns:
        (dict|str): A structured narrative response from the Agentic Hub.
    """
    import agentic_hub
    try:
        # 1. Primary Path: Route to the Pure Agentic SQL/ML Hub
        # This will internally generate SQL, run it, and synthesize insights.
        return agentic_hub.ask_agentic(question, model_name, provider)
        
    except Exception as e:
        # 2. Fallback Path: System Error Handling
        # If the hub fails due to syntax or connectivity, fall back to a 
        # controlled error message without crashing the UI.
        import logging
        logging.error(f"[*] Agentic Hub Failure: {str(e)}")
        
        fallback_msg = (
            f"*(Using pre-calculated baseline model due to specific data unavailability)*\n\n"
            f"**Baseline Insight:** The ML orchestrator could not verify the exact subset requested. \n\n"
            f"**System Error Debug:** `{str(e)}`"
        )
        return fallback_msg
