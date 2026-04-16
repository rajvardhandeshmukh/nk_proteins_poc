"""
LLM Planner - Granite Fallback for Query Intent
===============================================
Used ONLY when the rule-based planner has low confidence (<0.7).
It does one thing: map the natural language query to an intent,
extract parameters, and return JSON.
"""

import json
import logging
from .llm_client import call_granite
from .sql_templates import VALID_INTENTS
from .telemetry import log_plan

logger = logging.getLogger(__name__)

def extract_json(response: str) -> str:
    """Manually extract JSON block from text if formats are mixed."""
    start = response.find('{')
    end = response.rfind('}')
    if start != -1 and end != -1 and end > start:
        return response[start:end+1]
    return response

def safe_parse(response: str) -> dict:
    """Safely parse JSON with fallback cleanup."""
    try:
        return json.loads(response)
    except Exception:
        cleaned = extract_json(response)
        return json.loads(cleaned)

PLANNER_SYSTEM_PROMPT = f"""You are the CMD CoPilot for NK Proteins, a Gujarat-based edible oil and castor oil manufacturer.
IDENTITY LOCKDOWN: You are a machine-only dispatcher. You DO NOT have a personality. You DO NOT say "Hello" or "Sure". 
DATA CONSTRAINT: Operational records ONLY exist from October 1st, 2025 to March 31st, 2026.

STRICT FORMATTING RULE: 
- OUTPUT RAW JSON ONLY. 
- NO PREAMBLE. NO CONVERSATIONAL TEXT. NO MARKDOWN CODES. 
- DO NOT ADD ANY TEXT OUTSIDE THE BRACES.
- If you add any text outside the JSON, you will cause a SYSTEM FAILURE.

INTENT CLASSIFICATION RULES:
1. A question asking for "Which product", "who", or "top/best/highest/worst/lowest" MUST be mapped to a RANKING intent: 
   - `top_margins`, `worst_margins`, or `top_products`.
2. A question asking for "How much", "total", or "trend" should be mapped to `revenue_trend`.

Allowed intents:
{VALID_INTENTS}

Format your output ONLY as RAW JSON:
{{
  "intent": "top_margins",
  "params": {{"month": "2026-02"}},
  "mode": "template"
}}
"""

def plan_query_llm(user_input: str) -> dict:
    """Fallback planner using Granite."""
    logger.info("Using Granite LLM for planning fallback...")
    
    response = call_granite(
        user_prompt=user_input,
        system_prompt=PLANNER_SYSTEM_PROMPT,
        temperature=0.0,
        max_tokens=300
    )
    
    if response["status"] == "error":
        return {
            "intent": "unknown",
            "params": {},
            "mode": "dynamic",
            "confidence": 0.0,
            "original_query": user_input,
            "error": response["error"]
        }
        
    raw_text = response["text"]
    
    try:
        # Strip simple markdown before trying safe_parse
        if "```json" in raw_text:
            raw_text = raw_text.split("```json")[1].split("```")[0].strip()
        elif "```" in raw_text:
            raw_text = raw_text.split("```")[1].split("```")[0].strip()
            
        parsed_plan = safe_parse(raw_text)
        
        intent = parsed_plan.get("intent", "unknown")
        if intent not in VALID_INTENTS:
            parsed_plan["intent"] = "unknown"
            parsed_plan["mode"] = "dynamic"
            
        parsed_plan["confidence"] = 0.8
        parsed_plan["original_query"] = user_input
        
        if parsed_plan.get("mode") not in ["template", "dynamic"]:
            parsed_plan["mode"] = "dynamic"
            
        log_plan(
            user_question=user_input,
            extracted_intent=parsed_plan["intent"],
            extracted_params=parsed_plan.get("params", {}),
            confidence=0.8
        )
        
        return parsed_plan
        
    except Exception as e:
        logger.error("Failed to parse LLM planner output: %s. Raw: %s", e, raw_text)
        return {
            "intent": "unknown",
            "params": {},
            "mode": "dynamic",
            "confidence": 0.0,
            "original_query": user_input,
            "error": "json_parse_error"
        }
