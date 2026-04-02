import os
import json
import re
import time
import pandas as pd
import logging
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from difflib import SequenceMatcher
from datetime import datetime

logger = logging.getLogger(__name__)

# =============================================================================
# 1. DB ENGINE & CACHING
# =============================================================================

_engine = None

def get_mssql_engine():
    global _engine
    if _engine is not None:
        return _engine
        
    server = os.getenv("MSSQL_SERVER", "localhost")
    database = os.getenv("MSSQL_DATABASE", "nk_proteins")
    user = os.getenv("MSSQL_USER", "sa")
    password = os.getenv("MSSQL_PASS")
    
    if not password:
        raise EnvironmentError("[!] CRITICAL: MSSQL_PASS environment variable is not set. System halted for security.")
        
    port = os.getenv("MSSQL_PORT", "1433")
    encoded_pass = quote_plus(password)
    conn_str = f"mssql+pymssql://{user}:{encoded_pass}@{server}:{port}/{database}"
    
    # Create singleton engine with connection pooling
    _engine = create_engine(conn_str, pool_size=10, max_overflow=5, pool_recycle=3600)
    return _engine

# =============================================================================
# 2. DYNAMIC ENTITY LOADING
# =============================================================================

ENTITY_CACHE = {}

def load_entities_from_db():
    """Queries DB once to build entity lookup sets. Zero hardcoding."""
    global ENTITY_CACHE
    try:
        engine = get_mssql_engine()
        with engine.connect() as conn:
            ENTITY_CACHE = {
                'regions':    set(pd.read_sql("SELECT DISTINCT region FROM fact_sales", conn)['region'].dropna()),
                'customers':  set(pd.read_sql("SELECT DISTINCT customer_name FROM fact_sales", conn)['customer_name'].dropna()),
                'products':   set(pd.read_sql("SELECT DISTINCT product_name FROM fact_sales", conn)['product_name'].dropna()),
                'warehouses': set(pd.read_sql("SELECT DISTINCT warehouse FROM fact_inventory", conn)['warehouse'].dropna()),
                'suppliers':  set(pd.read_sql("SELECT DISTINCT supplier FROM fact_inventory", conn)['supplier'].dropna()),
            }
        ENTITY_CACHE['all'] = set()
        for k, v in ENTITY_CACHE.items():
            if k != 'all' and isinstance(v, set):
                ENTITY_CACHE['all'] |= {e.lower() for e in v}
        print(f"[*] Entity Cache loaded: {len(ENTITY_CACHE['all'])} entities from DB.")
    except Exception as e:
        print(f"[*] Entity cache load failed: {e}")
        ENTITY_CACHE = {'all': set()}

ENTITY_CATEGORY_TO_COLUMN = {
    'regions': 'region',
    'customers': 'customer_name',
    'products': 'product_name',
    'warehouses': 'warehouse',
    'suppliers': 'supplier',
}

def fuzzy_resolve_entity(user_input, cutoff=0.90):
    """Matches user input against all known DB entities."""
    user_input = user_input.lower().strip()
    best_match = None
    best_score = 0
    actual_category = None
    
    for category, entities in ENTITY_CACHE.items():
        if category == 'all': continue
        for e in entities:
            score = SequenceMatcher(None, user_input, e.lower()).ratio()
            if score > best_score:
                best_score = score
                best_match = e
                actual_category = category
                
    if best_score >= cutoff and best_match:
        column = ENTITY_CATEGORY_TO_COLUMN.get(actual_category, 'region')
        return best_match, column, best_score
        
    return user_input, None, best_score

# =============================================================================
# 3. TELEMETRY & OBSERVABILITY
# =============================================================================

def log_pipeline_telemetry(question, intent, sql, rows, latency, model_name):
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    telemetry = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "question": question,
        "intent": intent,
        "sql_query": sql,
        "rows_analyzed": rows,
        "latency_sec": round(latency, 2),
        "model_used": model_name
    }
    with open(os.path.join(log_dir, 'pipeline_telemetry.jsonl'), 'a', encoding='utf-8') as f:
        f.write(json.dumps(telemetry) + "\n")

# =============================================================================
# 4. SMART PREVIEW & FORMATTING
# =============================================================================

def get_smart_preview(df, pillar, intent):
    if df is None or df.empty: return None
    
    preview_cols = {
        'sales':     ['date', 'product_name', 'region', 'revenue', 'margin_pct'],
        'cashflow':  ['invoice_no', 'invoice_date', 'customer_name', 'invoice_amount', 'days_overdue', 'aging_bucket'],
        'gst':       ['invoice_no', 'counterparty_name', 'taxable_value', 'total_tax_amount', 'mismatch_flag'],
        'inventory': ['sku', 'snapshot_date', 'warehouse', 'current_stock_kg', 'is_dead_stock', 'total_value_inr']
    }.get(pillar, df.columns.tolist()[:8])
    
    sort_config = {
        ('cashflow', 'anomaly_detection'): (['days_overdue', 'invoice_amount'], [False, False]),
        ('gst', 'anomaly_detection'):      (['mismatch_flag', 'total_tax_amount'], [False, False]),
        ('inventory', 'anomaly_detection'):(['is_dead_stock', 'total_value_inr'], [False, False]),
        ('sales', 'forecasting'):          (['date'], [False]),
    }.get((pillar, intent), (['date' if 'date' in df.columns else df.columns[0]], [False]))
    
    try:
        f_sort = [c for c in sort_config[0] if c in df.columns]
        if f_sort: 
            df = df.sort_values(by=f_sort, ascending=sort_config[1][:len(f_sort)])
    except Exception as e:
        logger.error("Smart preview failed for pillar %s: %s", pillar, e, exc_info=True)
        
    display_cols = [col for col in preview_cols if col in df.columns]
    return df[display_cols].head(5)

def compute_provenance(df, pillar):
    from .config import PILLAR_DATE_COL
    date_col = PILLAR_DATE_COL.get(pillar)
    if date_col and date_col in df.columns:
        try:
            dates = pd.to_datetime(df[date_col])
            return f"{dates.min().strftime('%b %Y')} to {dates.max().strftime('%b %Y')}"
        except: pass
    return "current snapshot"

def extract_sql_from_response(response_text):
    pattern = r"```(?:sql)?\s+(.*?)\s+```"
    match = re.search(pattern, response_text, re.DOTALL | re.IGNORECASE)
    if match: return match.group(1).strip()
    return response_text.replace('```sql', '').replace('```', '').strip()

def validate_and_constrain_sql(sql):
    sql_upper = sql.upper().strip()
    if any(kw in sql_upper for kw in ["DROP ", "DELETE ", "UPDATE ", "INSERT ", "ALTER ", "TRUNCATE "]):
        raise ValueError("Destructive SQL commands are strictly forbidden.")
    if not sql_upper.startswith("SELECT"):
        raise ValueError("Query must begin with SELECT.")
    if " WHERE " not in sql_upper and " TOP " not in sql_upper:
        sql = re.sub(r'(?i)^SELECT\s+', 'SELECT TOP 5000 ', sql)
    return sql
