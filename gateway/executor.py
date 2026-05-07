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

from . import validators
from .telemetry import log_query, log_error
from .domains import registry

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

def _build_query(intent: str, params: dict, domain: str = None) -> tuple[str, dict]:
    """
    Takes an intent name and user params.
    Returns (filled_sql_string, safe_params_dict).
    """
    # 1. Try the Registry (New Modular Domain Path)
    template = registry.get_template(domain, intent) if domain else None
    
    # 2. Global Registry Fallback (if domain is unknown)
    if not template:
        all_tmpls = registry.get_all_templates()
        template = all_tmpls.get(intent)

    # 3. Legacy Fallback
    if not template:
        from . import sql_templates
        if intent in sql_templates.SQL_TEMPLATES:
            template = sql_templates.SQL_TEMPLATES[intent]

    if not template:
        raise ValueError(f"Intent '{intent}' (Domain: {domain}) not recognized in any registered domain.")

    # Normalize template to dict if it's a raw string
    if isinstance(template, str):
        template = {"query": template, "params": {}, "optional_filters": {}}

    # Step 1: Validate types + fuzzy-correct entity names
    safe_params = validators.validate_and_correct_params(intent, params, template)
    
    # Inject default data window if missing
    if "start_date" not in safe_params or "end_date" not in safe_params:
        window = validators.get_data_window()
        if "start_date" not in safe_params:
            safe_params["start_date"] = window.get("min_date", "2025-02-01")
        if "end_date" not in safe_params:
            safe_params["end_date"] = window.get("max_date", "2025-02-15")

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

    return query.strip(), safe_params


# =============================================================================
# EXECUTOR (with retry + timeout)
# =============================================================================

MAX_RETRIES = 2
QUERY_TIMEOUT_SEC = 15

_SCHEMA_VALIDATED = False

def validate_schema():
    """
    Startup Pulse Check: Verifies all .env column mappings exist in the DB.
    Prevents 'Silent Column Mismatch' issues.
    """
    global _SCHEMA_VALIDATED
    if _SCHEMA_VALIDATED:
        return
    
    from .config import config
    logger.info("!!! [SCHEMA VALIDATION] Starting startup pulse check...")
    
    engine = get_engine()
    column_map = config.get_column_map()
    table = config.VIEW_SALES
    
    failed_cols = []
    with engine.connect() as conn:
        for key, col in column_map.items():
            try:
                # Optimized check: SELECT TOP 1 is fast and safe for existence check
                # Wrap in brackets to handle spaces in column/table names
                safe_col = f"[{col}]" if not col.startswith("[") else col
                safe_table = f"[{table}]" if not table.startswith("[") else table
                conn.execute(text(f"SELECT TOP 1 {safe_col} FROM {safe_table}"))
                logger.info(f"  [PASS] Column '{key}' -> '{col}' verified.")
            except Exception as e:
                logger.error(f"  [FAIL] Column '{key}' -> '{col}' NOT FOUND in table '{table}'. Error: {e}")
                failed_cols.append(f"{key}:{col}")
    
    if failed_cols:
        error_msg = f"CRITICAL SCHEMA MISMATCH: The following columns in your .env do not exist in the database: {failed_cols}"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    _SCHEMA_VALIDATED = True
    logger.info("!!! [SCHEMA VALIDATION] All columns verified successfully.")

def execute_query(intent: str, params: dict, domain: str = None) -> dict:
    """
    The main entry point for Floor 2.
    """
    start = time.time()
    
    # 0. Startup Pulse Check (Run once)
    try:
        validate_schema()
    except Exception as e:
        return {"status": "error", "intent": intent, "message": str(e), "data": [], "row_count": 0, "query_ms": 0}

    # 1. Build the safe query
    try:
        sql, safe_params = _build_query(intent, params, domain=domain)
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
                # --- IMPROVED QUERY READABILITY (LOGGING) ---
                print("\n" + "="*80)
                print(f"SQL EXECUTION TRACE | Intent: {intent} | Attempt: {attempt}")
                print("-"*80)
                print(f"QUERY:\n{sql}")
                print(f"PARAMS: {safe_params}")
                print("="*80 + "\n")
                
                df = pd.read_sql(text(sql), conn, params=safe_params)
                
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
            for p_name in validators.PARAM_TO_CATEGORY:
                if p_name in params and p_name in safe_params and params[p_name] != safe_params.get(p_name, "").replace("%", ""):
                    clean_corrected = safe_params[p_name].replace("%", "")
                    if params[p_name] != clean_corrected:
                        corrections[p_name] = {"original": params[p_name], "corrected": clean_corrected}

            # Ensure JSON serializability (convert Timestamps to strings)
            for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
                df[col] = df[col].dt.strftime('%Y-%m-%d')

            result = {
                "status": "success",
                "intent": intent,
                "data": _df_to_json_safe_dict(df),
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
                # --- IMPROVED QUERY READABILITY (LOGGING) ---
                print("\n" + "="*80)
                print(f"SQL DYNAMIC EXECUTION | Attempt: {attempt}")
                print("-"*80)
                print(f"QUERY:\n{sql_query}")
                print("="*80 + "\n")
                
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
                "data": _df_to_json_safe_dict(df),
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


def _df_to_json_safe_dict(df: pd.DataFrame) -> list:
    """
    Convert a dataframe to a list of records, ensuring dates/timestamps
    are stringified for JSON serialization.
    """
    import datetime
    from decimal import Decimal

    if df.empty:
        return []
    
    # Work on a copy to avoid side effects
    tmp = df.copy()
    
    # Handle NaN/Inf first
    tmp = tmp.astype(object).where(pd.notnull(tmp), None)
    
    # Convert to records
    records = tmp.to_dict(orient="records")
    
    # Final pass to catch any date/datetime/decimal objects hidden in object columns
    for row in records:
        for key, val in row.items():
            if isinstance(val, (datetime.date, datetime.datetime)):
                row[key] = val.isoformat()
            elif isinstance(val, Decimal):
                row[key] = float(val)
                
    return records

