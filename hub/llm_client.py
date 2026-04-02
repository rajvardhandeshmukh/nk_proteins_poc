import chatbot

def call_llm_light(prompt_text, model_name, provider):
    """Lightweight LLM call for classification/extraction."""
    usr = {"role": "user", "content": prompt_text}
    sys = {"role": "system", "content": "You are a precise classifier. Return ONLY the requested value, nothing else."}
    
    if provider == "watsonx":
        return chatbot.call_watsonx(prompt_text, sys["content"], model_name).strip().lower()
    
    resp = chatbot.call_ai_provider_orchestration(usr, sys, model_name, provider)
    if isinstance(resp, dict):
        if "choices" in resp and len(resp["choices"]) > 0:
            return resp["choices"][0].get("message", {}).get("content", "").strip().lower()
        elif "response" in resp:
            return resp["response"].strip().lower()
    return "aggregation"

def call_llm_narrate(user_prompt, system_prompt, model_name, provider):
    """Full-weight LLM call for narration."""
    usr = {"role": "user", "content": user_prompt}
    sys = {"role": "system", "content": system_prompt}
    
    if provider == "watsonx":
        return chatbot.call_watsonx(user_prompt, system_prompt, model_name).strip()
    
    resp = chatbot.call_ai_provider_orchestration(usr, sys, model_name, provider)
    if isinstance(resp, dict):
        if "choices" in resp and len(resp["choices"]) > 0:
            return resp[ "choices"][0].get("message", {}).get("content", "").strip()
        elif "response" in resp:
            return resp["response"].strip()
    return None
