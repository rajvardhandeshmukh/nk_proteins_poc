"""
Validators — Fuzzy Matching + Type Safety
==========================================
Catches misspelled filters and wrong data types BEFORE they hit SQL.
"""

import os
import logging
import pandas as pd
import re
from difflib import SequenceMatcher
from urllib.parse import quote_plus
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

# =============================================================================
# ENTITY CACHE (Loaded once from DB at startup)
# =============================================================================

ENTITY_CACHE = {}

def load_entity_cache():
    """Pull all known entity values from the database once."""
    global ENTITY_CACHE

    server   = os.getenv("MSSQL_SERVER", "localhost")
    database = os.getenv("MSSQL_DATABASE", "nk_proteins")
    user     = os.getenv("MSSQL_USER", "sa")
    password = os.getenv("MSSQL_PASS")
    port     = os.getenv("MSSQL_PORT", "1433")

    if not password:
        logger.warning("MSSQL_PASS not set — entity cache disabled.")
        return

    encoded_pass = quote_plus(password)
    # Synchronized with executor.py for Windows stability
    conn_str = f"mssql+pyodbc://{user}:{encoded_pass}@{server}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(conn_str)

    try:
        with engine.connect() as conn:
            ENTITY_CACHE = {
                "region": set(
                    pd.read_sql("SELECT DISTINCT CustomerRegionName FROM fact_sales", conn)["CustomerRegionName"].dropna()
                ),
                "customer": set([]), # Customer names not available in core fact tables yet
                "product": set(
                    pd.read_sql("SELECT DISTINCT ProductName FROM fact_sales", conn)["ProductName"].dropna()
                ),
                "warehouse": set(
                    pd.read_sql("SELECT DISTINCT location_name FROM fact_inventory", conn)["location_name"].dropna()
                ),
            }
        total = sum(len(v) for v in ENTITY_CACHE.values())
        logger.info("Entity cache loaded: %d entities across %d categories", total, len(ENTITY_CACHE))
    except Exception as e:
        logger.error("Failed to load entity cache: %s", e)
        ENTITY_CACHE = {}
    finally:
        engine.dispose()


def fuzzy_match(value: str, category: str, cutoff: float = 0.75) -> tuple[str, float]:
    """
    Match a user-provided value against known DB entities.
    Returns (corrected_value, confidence_score).
    
    Example: fuzzy_match("gujrat", "region") → ("Gujarat", 0.92)
    """
    if not ENTITY_CACHE or category not in ENTITY_CACHE:
        return value, 0.0

    candidates = ENTITY_CACHE[category]
    best_match = value
    best_score = 0.0

    for candidate in candidates:
        score = SequenceMatcher(None, value.lower().strip(), candidate.lower()).ratio()
        if score > best_score:
            best_score = score
            best_match = candidate

    if best_score >= cutoff:
        if best_match != value:
            logger.info("Fuzzy corrected: '%s' → '%s' (%.0f%% confidence)", value, best_match, best_score * 100)
        return best_match, best_score
    else:
        logger.warning("No fuzzy match for '%s' in [%s] (best: '%s' at %.0f%%)", value, category, best_match, best_score * 100)
        return value, best_score


# =============================================================================
# TYPE VALIDATION
# =============================================================================

# Which params map to which entity category for fuzzy matching
PARAM_TO_CATEGORY = {
    "region": "region",
    "customer": "customer",
    "product": "product",
    "warehouse": "warehouse",
}

def validate_and_correct_params(intent: str, params: dict, template_config: dict) -> dict:
    """
    Validate data types and fuzzy-correct entity values.
    Returns cleaned params dict.
    Raises ValueError for unrecoverable type errors.
    """
    corrected = {}

    for param_name, param_config in template_config.get("params", {}).items():
        # Safely get value or default
        if param_name in params:
            value = params[param_name]
        elif "default" in param_config:
            value = param_config["default"]
        else:
            # Required parameter missing
            raise ValueError(f"Missing required parameter: '{param_name}'")

        expected_type = param_config["type"]

        # Type coercion
        if expected_type == "int" and value is not None:
            try:
                value = int(value)
            except (ValueError, TypeError):
                raise ValueError(
                    f"Parameter '{param_name}' must be a number, got: '{value}'"
                )
            # Enforce max
            if "max" in param_config:
                value = min(value, param_config["max"])

        corrected[param_name] = value

    # Fuzzy-correct string filters
    for param_name in list(params.keys()):
        if param_name in PARAM_TO_CATEGORY and params[param_name]:
            category = PARAM_TO_CATEGORY[param_name]
            original = params[param_name]
            corrected_val, score = fuzzy_match(original, category)
            corrected[param_name] = corrected_val

    return corrected

def validate_and_constrain_sql(sql: str) -> str:
    """
    Ensures generated SQL is read-only and limited.
    Blocks: DROP, DELETE, INSERT, UPDATE, TRUNCATE, EXEC.
    """
    sql_upper = sql.upper().strip()
    
    # 1. Block dangerous keywords
    forbidden = ["DROP", "DELETE", "INSERT", "UPDATE", "TRUNCATE", "EXEC", "ALTER"]
    for word in forbidden:
        if re.search(rf"\b{word}\b", sql_upper):
            raise ValueError(f"Unsafe keyword detected: {word}")

    # 2. Enforce READ-ONLY (must start with SELECT or WITH)
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        raise ValueError("Queries must start with SELECT or WITH.")

    # 3. Strip trailing semicolon
    sql = sql.strip().rstrip(";")
    
    return sql
