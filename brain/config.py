"""
NK Protein CoPilot — AI Configuration (Brain)
=============================================
Defines the available LLM providers and models supported by the system.
"""

PROVIDERS = [
    {"label": "OpenAI", "value": "openai"},
    {"label": "Google", "value": "google"},
    {"label": "Claude", "value": "claude"},
    {"label": "Ollama (Local)", "value": "ollama"},
    {"label": "IBM Watsonx", "value": "watsonx"}
]

LLM_MODELS = [
    # OpenAI Models
    {"provider": "openai", "label": "GPT-4o", "value": "gpt-4o"},
    {"provider": "openai", "label": "GPT-4o Mini", "value": "gpt-4o-mini"},
    {"provider": "openai", "label": "GPT-4 Turbo", "value": "gpt-4-turbo"},
    {"provider": "openai", "label": "GPT-3.5 Turbo", "value": "gpt-3.5-turbo"},
    
    # Google Gemini Models
    {"provider": "google", "label": "Gemini 1.5 Pro", "value": "gemini-1.5-pro"},
    {"provider": "google", "label": "Gemini 1.5 Flash", "value": "gemini-1.5-flash"},
    
    # Anthropic Claude Models
    {"provider": "claude", "label": "Claude 3.5 Sonnet", "value": "claude-3-5-sonnet-20240620"},
    {"provider": "claude", "label": "Claude 3 Opus", "value": "claude-3-opus-20240229"},
    {"provider": "claude", "label": "Claude 3 Haiku", "value": "claude-3-haiku-20240307"},
    
    # Local Ollama Models (for air-gapped/privacy-first operation)
    {"provider": "ollama", "label": "Qwen 3 VL 4B", "value": "qwen3-vl:4b"},
    {"provider": "ollama", "label": "Phi-4 Mini", "value": "phi4-mini:latest"},
    {"provider": "ollama", "label": "Llama 3.2 3B", "value": "llama3.2:3b"},
    
    # IBM Watsonx.ai Models (Enterprise Grade)
    {"provider": "watsonx", "label": "Granite 3 8B Instruct", "value": "ibm/granite-3-8b-instruct"},
    {"provider": "watsonx", "label": "Llama 3.3 70B Instruct", "value": "meta-llama/llama-3-3-70b-instruct"},
    {"provider": "watsonx", "label": "Granite 4 H Small", "value": "ibm/granite-4-h-small"},
    {"provider": "watsonx", "label": "Mistral Small 3.1 24B", "value": "mistralai/mistral-small-3-1-24b-instruct-2503"},
]

# System Defaults
DEFAULT_LLM_MODEL = "gpt-4o"
DEFAULT_PROVIDER = "openai"
