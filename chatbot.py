"""
NK Protein CoPilot — AI Chatbot Service (v6.0 Modular)
======================================================
This file acts as a facade/gateway to the 'brain/' package.
Maintains backward compatibility for imports from other modules.
"""

# 1. AI Orchestration
from brain.orchestrator import ask

# 2. System Configuration
from brain.config import (
    PROVIDERS, LLM_MODELS, 
    DEFAULT_LLM_MODEL, DEFAULT_PROVIDER
)

# 3. Persistence & Interaction Logging
from brain.logger import (
    log_interaction, 
    save_chat_history, 
    load_chat_history
)

# 4. Provider Call Logic (Internal Implementations)
from brain.providers import (
    call_ai_provider_orchestration,
    call_ollama_local,
    call_watsonx
)
