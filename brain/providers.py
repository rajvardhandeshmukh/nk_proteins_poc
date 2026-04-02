"""
NK Protein CoPilot — AI Provider Implementations (Brain)
========================================================
Contains the specific calling logic for cloud and local AI providers.
"""

import os
import requests
import time
from dotenv import load_dotenv
from brain.logger import log_interaction

# Ensure .env is always available to provider calls
load_dotenv()

def call_ai_provider_orchestration(user_prompt, system_prompt, model_name, provider):
    """
    Calls the central AI Provider Orchestration API.
    This routes to cloud providers (OpenAI, Google, Claude) 
    through a secure, institutional intelligence gateway.
    """
    api_url = os.environ.get("AI_PROVIDER_ORCHESTRATION_API_URL", "")
    api_key = os.environ.get("AI_PROVIDER_ORCHESTRATION_API_KEY", "")

    if not api_url or not api_key:
        return {"error": "Missing AI_PROVIDER_ORCHESTRATION_API configuration in .env"}
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    payload = {
        "model": model_name,
        "provider": provider,
        "prompts": [system_prompt, user_prompt]
    }
    
    try:
        base_url = api_url.rstrip('/')
        target_url = f"{base_url}/ai/generate"
        response = requests.post(target_url, headers=headers, json=payload, timeout=45)
        
        # Log transaction for audit
        log_interaction(payload, {"status": response.status_code})
        
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log_interaction(payload, None, error=str(e))
        return {"error": f"Orchestration Error: {str(e)}"}

def call_ollama_local(user_prompt, system_prompt, model_name):
    """
    Calls the local Ollama instance (on-premise).
    Used for sensitive data analysis where cloud processing is prohibited.
    """
    start_time = time.time()
    payload = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "stream": False
    }
    try:
        url = "http://localhost:11434/api/chat"
        # 300s timeout to allow for slower CPU-only local inference
        response = requests.post(url, json=payload, timeout=300)
        response.raise_for_status()
        
        duration = round(time.time() - start_time, 2)
        content = response.json().get('message', {}).get('content', "Error: No response from Ollama")
        
        log_interaction(payload, {"duration_sec": duration, "model": model_name})
        return f"*(Processed locally by {model_name} in {duration}s)*\n\n{content}"
    except Exception as e:
        duration = round(time.time() - start_time, 2)
        log_interaction(payload, None, error=f"Ollama Error after {duration}s: {str(e)}")
        return f"Error connecting to Ollama: {str(e)}. (Is the local Ollama service running?)"

def call_watsonx(user_prompt, system_prompt, model_id):
    """
    Calls IBM Watsonx Cloud API (Enterprise Grade).
    Specifically optimised for IBM Granite and Meta Llama-3 series.
    """
    start_time = time.time()
    try:
        from ibm_watsonx_ai import Credentials
        from ibm_watsonx_ai.foundation_models import ModelInference
        from ibm_watsonx_ai.metanames import GenTextParamsMetaNames as GenParams

        credentials = Credentials(
            url=os.environ.get("WATSONX_URL", "https://eu-de.ml.cloud.ibm.com"),
            api_key=os.environ.get("WATSONX_API_KEY")
        )
        project_id = os.environ.get("WATSONX_PROJECT_ID")
        
        if not credentials.api_key or not project_id:
            return "Error: Missing WATSONX_API_KEY or WATSONX_PROJECT_ID in .env"

        params = {
            GenParams.DECODING_METHOD: "greedy",
            GenParams.MIN_NEW_TOKENS: 1,
            GenParams.MAX_NEW_TOKENS: 1024,
            GenParams.TEMPERATURE: 0.7
        }

        model = ModelInference(model_id=model_id, params=params, credentials=credentials, project_id=project_id)

        # Custom Prompt Template for IBM Granite/Llama
        full_prompt = f"<|system|>\n{system_prompt}\n<|user|>\n{user_prompt}\n<|assistant|>\n"
        raw_response = model.generate(prompt=full_prompt)
        
        duration = round(time.time() - start_time, 2)
        result = raw_response.get('results', [{}])[0]
        response_text = result.get('generated_text', 'No response text.')

        log_interaction(
            {"provider": "watsonx", "model": model_id},
            {"duration_sec": duration, "tokens": result.get('generated_token_count')}
        )
        return response_text
    except Exception as e:
        log_interaction({"provider": "watsonx", "model": model_id}, None, error=str(e))
        return f"Error connecting to IBM Watsonx: {str(e)}"
