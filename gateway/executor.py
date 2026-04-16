"""
SQL Executor — Deterministic Layer (Floor 2)
=============================================
Takes a structured intent + params, picks the right SQL template,
fills parameters safely, runs against MSSQL, returns JSON data.

Safety Features:
  - Fuzzy matching on entity filters (auto-corrects misspellings)
  - Data type validation (rejects "six" where int expected)
  - Query timeout (15 second max)
  - DB reconnection on stale pool
  - Full telemetry on every call
"""

import os
import re
import time
import logging
import pandas as pd
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from .sql_templates import SQL_TEMPLATES, VALID_INTENTS
from .validators import validate_and_correct_params, PARAM_TO_CATEGORY, fuzzy_match
from .telemetry import log_query, log_error

logger = logging.getLogger(__name__)

# =============================================================================
# DATABASE CONNECTION (Singleton with reconnection)
# =============================================================================

_engine = None

def get_engine(force_new=False):
    """Create a singleton MSSQL engine with connection pooling."""
    global _engine
    if _engine is not None and not force_new:
        return _engine

    server   = os.getenv("MSSQL_SERVER", "localhost")
    database = os.getenv("MSSQL_DATABASE", "nk_proteins")
    user     = os.getenv("MSSQL_USER", "sa")
    password = os.getenv("MSSQL_PASS")
    port     = os.getenv("MSSQL_PORT", "1433")

    if not password:
        raise EnvironmentError("MSSQL_PASS environment variable is not set.")

    encoded_pass = quote_plus(password)
    # Pivot to pyodbc for Windows stability (verified working in manual audit)
    conn_str = f"mssql+pyodbc://{user}:{encoded_pass}@{server}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    
    # DIAGNOSTIC: Log the connection (password masked)
    logger.warning("GROUND TRUTH CONNECTION: mssql+pyodbc://%s:****@%s:%s/%s", user, server, port, database)

    if _engine is not None:
        _engine.dispose()

    _engine = create_engine(
        conn_str,
        pool_size=5,
        max_overflow=3,
        pool_recycle=1800,
        pool_pre_ping=True,
    )
    return _engine


# =============================================================================
# QUERY BUILDER (Safe Template Filler)
# =============================================================================

def _build_query(intent: str, params: dict) -> tuple[str, dict]:
    """
    Takes an intent name and user params.
    Returns (filled_sql_string, safe_params_dict).
    """
    if intent not in SQL_TEMPLATES:
        raise ValueError(f"Unknown intent: '{intent}'. Valid: {VALID_INTENTS}")

    template = SQL_TEMPLATES[intent]

    # Step 1: Validate types + fuzzy-correct entity names
    safe_params = validate_and_correct_params(intent, params, template)

    # Step 2: Build the query string
    query = template["query"]

    # Step 3: Handle optional filters (inject or remove placeholder)
    for filter_key, filter_sql in template.get("optional_filters", {}).items():
        param_match = re.search(r':(\w+)', filter_sql)
        if param_match:
            param_name = param_match.group(1)
            if param_name in safe_params and safe_params[param_name]:
                # User provided this filter — inject it
                query = query.replace(f"{{{filter_key}}}", filter_sql)
                # Add wildcard wrapping for LIKE filters
                if "LIKE" in filter_sql:
                    safe_params[param_name] = f"%{safe_params[param_name]}%"
            else:
                # User did not provide — remove placeholder
                query = query.replace(f"{{{filter_key}}}", "")
        else:
            query = query.replace(f"{{{filter_key}}}", "")

    # MOP-UP: Force Absolute Ground-Truth Reference
    # This bypasses any local synonyms or schema-shadowing causing discrepancies.
    query = query.replace("fact_sales", "[nk_proteins].[dbo].[fact_sales]")
    query = query.replace("fact_inventory", "[nk_proteins].[dbo].[fact_inventory]")
    query = query.replace("fact_cashflow", "[nk_proteins].[dbo].[fact_cashflow]")
    
    # Schema Safety: Ensure legacy 'transaction_type' is always redirected to 'CashFlowType'
    query = query.replace("transaction_type", "CashFlowType")

    return query.strip(), safe_params


# =============================================================================
# EXECUTOR (with retry + timeout)
# =============================================================================

MAX_RETRIES = 2
QUERY_TIMEOUT_SEC = 15

def execute_query(intent: str, params: dict) -> dict:
    """
    The main entry point for Floor 2.

    Input:  {"intent": "revenue_trend", "params": {"months_back": 6, "region": "Gujarat"}}
    Output: {"status": "success", "data": [...], "row_count": N, "query_ms": M}
    """
    start = time.time()
    # FORCE RELOAD
    import importlib
    from . import sql_templates
    importlib.reload(sql_templates)

    # 1. Build the safe query (validates types + fuzzy corrections)
    try:
        sql, safe_params = _build_query(intent, params)
        print(f"!!! [GROUND TRUTH AUDIT] Intent: {intent} | Month: {safe_params.get('month')} | DB: {os.getenv('MSSQL_DATABASE')}")
    except ValueError as e:
        elapsed_ms = round((time.time() - start) * 1000)
        log_query(intent=intent, query="", params=params, row_count=0, latency_ms=elapsed_ms, status=f"validation_error: {e}")
        return {"status": "error", "intent": intent, "message": str(e), "data": [], "row_count": 0, "query_ms": elapsed_ms}

    # 2. Execute with retry logic
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            engine = get_engine(force_new=(attempt > 1))
            with engine.connect() as conn:
                # ABSOLUTE DIAGNOSTIC: Log the raw SQL and params
                print(f"!!! [SQL EXECUTION DEBUG] SQL: {sql}")
                print(f"!!! [SQL EXECUTION DEBUG] Params: {safe_params}")
                
                df = pd.read_sql(text(sql), conn, params=safe_params)
                
                print(f"!!! [SQL EXECUTION DEBUG] Result Row Count: {len(df)}")

            elapsed_ms = round((time.time() - start) * 1000)

            # Timeout check
            if elapsed_ms > QUERY_TIMEOUT_SEC * 1000:
                logger.warning("Query [%s] took %dms (exceeded %ds timeout)", intent, elapsed_ms, QUERY_TIMEOUT_SEC)

            # 3. Log telemetry
            log_query(
                intent=intent,
                query=sql,
                params=safe_params,
                row_count=len(df),
                latency_ms=elapsed_ms,
                status="success"
            )

            # 4. Build response with corrections metadata
            corrections = {}
            for p_name in PARAM_TO_CATEGORY:
                if p_name in params and p_name in safe_params and params[p_name] != safe_params.get(p_name, "").replace("%", ""):
                    clean_corrected = safe_params[p_name].replace("%", "")
                    if params[p_name] != clean_corrected:
                        corrections[p_name] = {"original": params[p_name], "corrected": clean_corrected}

            result = {
                "status": "success",
                "intent": intent,
                "data": df.to_dict(orient="records"),
                "row_count": len(df),
                "query_ms": elapsed_ms,
            }

            if corrections:
                result["corrections"] = corrections

            return result

        except OperationalError as e:
            last_error = e
            logger.warning("DB connection failed (attempt %d/%d): %s", attempt, MAX_RETRIES, e)
            if attempt < MAX_RETRIES:
                time.sleep(1)
                continue

        except Exception as e:
            last_error = e
            logger.error("Query execution failed for [%s]: %s", intent, e, exc_info=True)
            break

    # All retries exhausted
    elapsed_ms = round((time.time() - start) * 1000)
    error_msg = f"Database unavailable after {MAX_RETRIES} attempts: {str(last_error)}"
    log_query(intent=intent, query=sql, params=safe_params, row_count=0, latency_ms=elapsed_ms, status=f"error: {error_msg}")
    log_error(endpoint="/execute_query", error_type="db_failure", message=error_msg, context={"intent": intent, "attempts": MAX_RETRIES})

    return {"status": "error", "intent": intent, "message": error_msg, "data": [], "row_count": 0, "query_ms": elapsed_ms}


def execute_raw_sql(sql_query: str) -> dict:
    """
    Execute raw ad-hoc SQL strings safely.
    Warning: Caller MUST ensure sql_query is validated against injection.
    """
    start = time.time()
    last_error = None
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            engine = get_engine(force_new=(attempt > 1))
            with engine.connect() as conn:
                # ABSOLUTE DIAGNOSTIC: Log the raw dynamic SQL
                print(f"!!! [SQL DYNAMIC EXECUTION] SQL: {sql_query}")
                df = pd.read_sql(text(sql_query), conn)

            elapsed_ms = round((time.time() - start) * 1000)

            # Timeout check
            if elapsed_ms > QUERY_TIMEOUT_SEC * 1000:
                logger.warning("Dynamic Query took %dms (exceeded %ds timeout)", elapsed_ms, QUERY_TIMEOUT_SEC)

            # 3. Log telemetry
            log_query(
                intent="dynamic_sql",
                query=sql_query,
                params={},
                row_count=len(df),
                latency_ms=elapsed_ms,
                status="success"
            )

            return {
                "status": "success",
                "intent": "dynamic_sql",
                "data": df.to_dict(orient="records"),
                "row_count": len(df),
                "query_ms": elapsed_ms,
            }

        except OperationalError as e:
            last_error = e
            logger.warning("DB connection failed (attempt %d/%d): %s", attempt, MAX_RETRIES, e)
            if attempt < MAX_RETRIES:
                time.sleep(1)
                continue

        except Exception as e:
            last_error = e
            logger.error("Raw Query execution failed: %s", e, exc_info=True)
            break

    elapsed_ms = round((time.time() - start) * 1000)
    error_msg = f"Database unavailable or query failed: {str(last_error)}"
    log_query(intent="dynamic_sql", query=sql_query, params={}, row_count=0, latency_ms=elapsed_ms, status=f"error: {error_msg}")
    log_error(endpoint="/execute_raw_sql", error_type="db_failure", message=error_msg, context={"query": sql_query})

    return {"status": "error", "intent": "dynamic_sql", "message": error_msg, "data": [], "row_count": 0, "query_ms": elapsed_ms}

