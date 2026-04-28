"""
Dynamic SQL Generator - Floor 1 Fallback
================================================
Takes a user query that failed high confidence AND failed LLM template mapping.
Generates an ad-hoc SQL query against the database schema.
MUST pass through validation before execution.
"""

import sys
import os
import logging
from .llm_client import call_llm

from .config import SQL_SCHEMA
from .validators import validate_and_constrain_sql

logger = logging.getLogger(__name__)

SQL_GEN_SYSTEM_PROMPT = f"""You are an expert MS SQL Developer for an ERP system.
Your job is to generate a read-only SQL query for an internal user's business question.

DATABASE SCHEMA:
{SQL_SCHEMA}

RULES:
1. ONLY return the raw MS SQL Server query string. 
2. Use standard MS SQL functions (e.g. GETDATE() not NOW(), DATEADD/DATEDIFF).
3. Do NOT include markdown formatting, just the query text.
4. Do NOT include any explanations, greetings, or other text.
5. NEVER generate INSERT, UPDATE, DELETE, DROP or EXEC statements. Read ONLY.
6. Make sure column names exactly match the schema.
7. Always include TOP 100 if there is no explicit limit requested to prevent huge loads.
8. NEVER estimate, approximate, or fabricate data. Only return SQL that retrieves exact database records.
9. If the user asks for a specific month and year, use exact WHERE filters: MONTH(date) = X AND YEAR(date) = Y. Do NOT use DATEADD rolling windows for point-in-time queries.
"""

def generate_sql_dynamic(user_input: str) -> dict:
    """Generate ad-hoc MS SQL query from LLM and validate it."""
    logger.warning("Falling back to DYNAMIC SQL GENERATION for query: %s", user_input)
    
    response = call_llm(
        user_prompt=user_input,
        system_prompt=SQL_GEN_SYSTEM_PROMPT,
        temperature=0.0,
        max_tokens=600
    )
    
    if response["status"] == "error":
        return {"status": "error", "error": response["error"], "sql": ""}
        
    raw_sql = response["text"].strip()
    
    if "```sql" in raw_sql:
        raw_sql = raw_sql.split("```sql")[1].split("```")[0].strip()
    elif "```" in raw_sql:
        raw_sql = raw_sql.split("```")[1].split("```")[0].strip()
        
    # Validate the generated SQL
    try:
        validated_sql = validate_and_constrain_sql(raw_sql)
    except ValueError as e:
        msg = f"LLM generated unsafe SQL: {str(e)}"
        logger.error(msg)
        return {"status": "error", "error": msg, "sql": raw_sql, "latency_ms": response.get("latency_ms", 0)}
    except Exception as e:
        msg = f"SQL Validator crashed: {str(e)}"
        logger.error(msg)
        return {"status": "error", "error": msg, "sql": raw_sql, "latency_ms": response.get("latency_ms", 0)}
        
    return {"status": "success", "sql": validated_sql, "latency_ms": response.get("latency_ms", 0)}
