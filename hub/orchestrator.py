import time
import json
import hashlib
import pandas as pd
from sqlalchemy import text
from .config import FULL_TABLE_QUERIES, PILLAR_DATE_COL
from .prompts import (
    INTENT_CLASSIFIER_PROMPT, PILLAR_CLASSIFIER_PROMPT, 
    ENTITY_EXTRACTION_PROMPT, build_aggregation_sql_prompt
)
from .utils import (
    get_mssql_engine, log_pipeline_telemetry, get_smart_preview, 
    compute_provenance, extract_sql_from_response, 
    fuzzy_resolve_entity, validate_and_constrain_sql
)
from .llm_client import call_llm_light, call_llm_narrate
from .governance import (
    validate_ml_data_contract, apply_confidence_gate, sanitize_ml_output
)
from .router import run_ml_for_pillar, handle_multi_pillar

# =============================================================================
# 1. CACHING & CONSTANTS
# =============================================================================

SQL_CACHE = {}

# =============================================================================
# 2. NARRATOR
# =============================================================================

def narrate(ml_result, df, intent, pillar, question, model_name, provider):
    """Calls the Narrator LLM with the locked-down prompt for the given intent."""
    rows = len(df)
    date_range = compute_provenance(df, pillar)
    
    if intent == "aggregation":
        data_payload = df.head(50).to_dict('records')
        system_prompt = f"""You are the Executive Data Analyst for NK Proteins.
Answer the user's question using ONLY the provided data.

OUTPUT FORMAT — follow exactly, no deviations:
### Summary
1–2 sentences. Direct answer only. NO emojis.

### Key Findings
- **Finding 1:** ...
- **Finding 2:** ...
- **Finding 3:** ...

### Recommended Action
One specific action. Who should do it, by when.

---
RULES: NEVER use emojis or icons. Use plain text only.
*Based on {rows} records from {date_range}.*"""
        user_prompt = f"User Question: {question}\n\nData Result:\n{json.dumps(data_payload, default=str)}"
    else:
        confidence = ml_result.get('_confidence', 'high')
        warnings = ml_result.get('_warnings', [])
        mape_used = ml_result.get('_mape_used', 25.0)
        sanitized = sanitize_ml_output(ml_result)
        
        warning_block = ""
        if confidence == "caution":
            warning_block = f"\n> **Caution: Approximate Prediction.** {'; '.join(warnings)}\n"
        elif confidence == "blocked":
            warning_block = f"\n> **ERROR: Forecast Blocked.** {'; '.join(warnings)}\n"

        ACCURACY_DISCLAIMER = ""
        if confidence == "caution" and mape_used:
             ACCURACY_DISCLAIMER = f"\nInclude this at end of Summary section: 'Model accuracy: {mape_used:.1f}% average error — treat as directional guidance.'\n"
        
        system_prompt = f"""You are a business analyst for NK Proteins.
The team ran a **{intent}** analysis on **{rows} rows** of **{pillar}** covering **{date_range}**.
{warning_block}
{ACCURACY_DISCLAIMER}

OUTPUT FORMAT — follow exactly, no deviations:
### Summary
1–2 sentences. Direct answer only. NO emojis.

### Key Findings
- **Finding 1:** ...
- **Finding 2:** ...
- **Finding 3:** ...

### Recommended Action
One specific action. Who should do it, by when.

---
RULES: Every number must come from the ML output. Do NOT fabricate information. NEVER use emojis.
*Analysis: {intent} | {rows} records | {date_range}*"""

        user_prompt = f"User Question: {question}\n\nML OUTPUT:\n{json.dumps(sanitized, default=str)}"
    
    return call_llm_narrate(user_prompt, system_prompt, model_name, provider)

# =============================================================================
# 3. ZERO-RESULT FEEDBACK
# =============================================================================

def build_zero_result_response(query, sql_query, intent):
    return {
        "narrative": "No matching records found. Try broadening your criteria.",
        "intent": intent,
        "sql": sql_query,
        "rows": 0,
        "model": "N/A",
        "df_preview": None
    }

# =============================================================================
# 4. MAIN ORCHESTRATOR
# =============================================================================

def ask_agentic(query, model_name, provider):
    start_time = time.time()
    
    # ── Step 1: LLM-based Intent Classification ──
    intent_raw = call_llm_light(INTENT_CLASSIFIER_PROMPT.format(question=query), model_name, provider)
    valid_intents = {'forecasting', 'anomaly_detection', 'segmentation', 'aggregation', 'hybrid', 'multi_pillar'}
    intent = intent_raw.strip().replace(' ', '_') if intent_raw.strip().replace(' ', '_') in valid_intents else 'aggregation'
    
    # ── Step 2: LLM-based Pillar Detection ──
    pillar_raw = call_llm_light(PILLAR_CLASSIFIER_PROMPT.format(question=query), model_name, provider)
    valid_pillars = {'sales', 'cashflow', 'gst', 'inventory'}
    pillar = pillar_raw.strip() if pillar_raw.strip() in valid_pillars else 'sales'

    # PATH A: MULTI-PILLAR
    if intent == 'multi_pillar':
        combined_result, total_rows = handle_multi_pillar()
        combined_ml = {k: sanitize_ml_output(v) for k, v in combined_result.items()}
        nar_sys = "You are a professional business analyst presenting an executive health check. NEVER use emojis. Use clean, text-based formatting only."
        nar_usr = f"User Question: {query}\n\nCOMBINED ML OUTPUT:\n{json.dumps(combined_ml, default=str)}"
        final_narrative = call_llm_narrate(nar_usr, nar_sys, model_name, provider)
        log_pipeline_telemetry(query, intent, "MULTI-PILLAR", total_rows, time.time() - start_time, model_name)
        return {
            "narrative": final_narrative, "intent": intent, "sql": "Parallel Multi-Pillar", 
            "rows": total_rows, "raw_metrics": combined_ml
        }

    # PATH B: FORECASTING / ANOMALY / SEGMENTATION
    if intent in ('forecasting', 'anomaly_detection', 'segmentation'):
        sql_query = FULL_TABLE_QUERIES.get(pillar)
        engine = get_mssql_engine()
        with engine.connect() as conn:
            df = pd.read_sql(sql_query, conn)
        
        if df.empty: return build_zero_result_response(query, sql_query, intent)
        
        # Sufficiency check
        if intent == 'forecasting':
            date_col = PILLAR_DATE_COL.get(pillar)
            if date_col in df.columns:
                unique_months = pd.to_datetime(df[date_col]).dt.to_period('M').nunique()
                if unique_months < 12:
                    return {"intent": "conversational", "narrative": f"**Insufficient Data:** Need 12 months, found {unique_months}."}

        is_valid, df, err = validate_ml_data_contract(df, pillar)
        if not is_valid: return {"intent": "conversational", "narrative": f"**Data Contract Error:** {err}"}
        
        ml_result = run_ml_for_pillar(df, pillar, intent)
        ml_result = apply_confidence_gate(ml_result)
        
        final_narrative = narrate(ml_result, df, intent, pillar, query, model_name, provider)
        log_pipeline_telemetry(query, intent, sql_query, len(df), time.time() - start_time, model_name)
        
        return {
            "narrative": final_narrative, "intent": intent, "sql": sql_query, "rows": len(df), 
            "df_preview": get_smart_preview(df, pillar, intent), "raw_metrics": ml_result,
            "_confidence": ml_result.get('_confidence'), "_warnings": ml_result.get('_warnings')
        }

    # PATH C: HYBRID
    if intent == 'hybrid':
        raw_entity = call_llm_light(ENTITY_EXTRACTION_PROMPT.format(question=query), model_name, provider)
        canonical, col, score = fuzzy_resolve_entity(raw_entity)
        if not col:
            return {"intent": "conversational", "narrative": f"**Not Found:** '{raw_entity}' (Score: {score:.2f})"}
        
        # SQL Injection Shield: Use Parameterized text()
        base_sql = FULL_TABLE_QUERIES.get(pillar)
        sql_query = text(base_sql.replace("WHERE ", f"WHERE {col} = :entity_val AND ", 1))
        
        engine = get_mssql_engine()
        with engine.connect() as conn:
            df = pd.read_sql(sql_query, conn, params={"entity_val": canonical})
        
        if df.empty: return build_zero_result_response(query, sql_query, intent)
        
        ml_result = run_ml_for_pillar(df, pillar, 'forecasting')
        ml_result = apply_confidence_gate(ml_result)
        final_narrative = narrate(ml_result, df, intent, pillar, query, model_name, provider)
        log_pipeline_telemetry(query, intent, sql_query, len(df), time.time() - start_time, model_name)
        
        return {
            "narrative": final_narrative, "intent": intent, "sql": sql_query, "rows": len(df),
            "df_preview": get_smart_preview(df, pillar, intent), "raw_metrics": ml_result
        }

    # PATH D: AGGREGATION
    sys_prompt = build_aggregation_sql_prompt()
    raw_resp = call_llm_narrate(query, sys_prompt, model_name, provider)
    sql_query = extract_sql_from_response(raw_resp)
    if not sql_query.upper().startswith("SELECT"): return raw_resp
    
    sql_query = validate_and_constrain_sql(sql_query)
    cache_key = hashlib.md5(sql_query.encode()).hexdigest()
    if cache_key in SQL_CACHE:
        df = SQL_CACHE[cache_key]
    else:
        engine = get_mssql_engine()
        with engine.connect() as conn:
            df = pd.read_sql(sql_query, conn)
        SQL_CACHE[cache_key] = df
        
    if df.empty: return build_zero_result_response(query, sql_query, intent)
    
    final_narrative = narrate({"status": "aggregated"}, df, "aggregation", pillar, query, model_name, provider)
    log_pipeline_telemetry(query, intent, sql_query, len(df), time.time() - start_time, model_name)
    
    return {
        "narrative": final_narrative, "intent": intent, "sql": sql_query, "rows": len(df),
        "df_preview": df.head(5)
    }
