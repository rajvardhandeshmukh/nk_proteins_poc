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

def fuzzy_resolve_entity(user_input, cutoff=0.90, type_hint=None):
    """Matches user input against all known DB entities. Supports type_hint for de-ambiguation."""
    user_input = user_input.lower().strip()
    best_match = None
    best_score = 0
    actual_category = None
    
    # If type_hint is provided (e.g. "region"), we primarily look in that category.
    categories_to_check = [type_hint + 's'] if type_hint and (type_hint + 's') in ENTITY_CACHE else ENTITY_CACHE.keys()
    
    for category in categories_to_check:
        if category == 'all': continue
        entities = ENTITY_CACHE.get(category, [])
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
    # Dynamic Limit: Audits (anomalies) get higher visibility (15 rows) vs 5 for trends.
    limit = 15 if intent == 'anomaly_detection' else 5
    return df[display_cols].head(limit)

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
    if response_text is None: return ""
    pattern = r"```(?:sql)?\s+(.*?)\s+```"
    match = re.search(pattern, response_text, re.DOTALL | re.IGNORECASE)
    if match: return match.group(1).strip()
    return response_text.replace('```sql', '').replace('```', '').strip()

def validate_and_constrain_sql(sql):
    """v7.1 Security & Sandbox Validator: Restricts joins, enforces dates, and blocks scans."""
    if not sql: return ""
    sql_upper = sql.upper().strip()
    
    # 1. Block Destructive or Unauthorized Commands
    blocked = ["DROP ", "DELETE ", "UPDATE ", "INSERT ", "ALTER ", "TRUNCATE ", "EXEC ", "UNION ", "INTO ", "SYS."]
    if any(kw in sql_upper for kw in blocked):
        raise ValueError(f"Security Alert: Blocked keyword detected.")
    
    # 2. Join Validation
    if " JOIN " in sql_upper:
        # Check if join is on an authorized key
        if not re.search(r'ON\s+.*\.(CUSTOMER_ID|PRODUCT_ID)', sql_upper):
             raise ValueError("Join Security: Aggregation joins are only permitted on 'customer_id' or 'product_id'.")
    
    # 3. Date Enforcement (Prevent Full Table Scans)
    # Refined Check: Ensure the date filter is both present AND syntactically complete
    has_date_filter = any(pat in sql_upper for pat in [" DATE ", "WEEK ", "MONTH ", "YEAR ", " BETWEEN ", "DATE_COL"])
    # If the LLM tried to use DATEADD but left it dangling, we treat it as missing to trigger a clean injection
    if "DATEADD" in sql_upper and not sql_upper.count("(") == sql_upper.count(")"):
        has_date_filter = False

    if not has_date_filter:
        # Infer the date column if it's a known table
        table_match = re.search(r'FROM\s+(\w+)', sql_upper)
        if table_match:
            table_name = table_match.group(1).lower()
            from .config import PILLAR_DATE_COL
            # Match pillar by searching substring or direct map
            date_col = next((v for k,v in PILLAR_DATE_COL.items() if k in table_name), None)
            
            if date_col:
                print(f"[*] Validator: Injecting 12-month date filter on '{date_col}'")
                # Clean up any partial date filters first
                sql = re.sub(r'(?i)AND\s+\w+\s*[>=<]+\s*DATEADD.*$', '', sql).strip()
                
                # Regex-based Case-Insensitive Injection
                if re.search(r'(?i)\bWHERE\b', sql):
                     sql = re.sub(r'(?i)\bWHERE\b', f"WHERE {date_col} >= DATEADD(month, -12, GETDATE()) AND ", sql, count=1)
                elif re.search(r'(?i)\bGROUP BY\b', sql):
                     sql = re.sub(r'(?i)\bGROUP BY\b', f"WHERE {date_col} >= DATEADD(month, -12, GETDATE()) GROUP BY ", sql, count=1)
                elif re.search(r'(?i)\bORDER BY\b', sql):
                     sql = re.sub(r'(?i)\bORDER BY\b', f"WHERE {date_col} >= DATEADD(month, -12, GETDATE()) ORDER BY ", sql, count=1)
                else:
                     sql += f" WHERE {date_col} >= DATEADD(month, -12, GETDATE())"

    # 4. Syntactic Self-Healing (Parenthesis Balance)
    # Fix instances of 'GETDATE()' or 'DATEADD(...' missing final brackets
    if sql.count("(") > sql.count(")"):
        sql += ")" * (sql.count("(") - sql.count(")"))
    if " TOP " not in sql_upper:
        sql = re.sub(r'(?i)^SELECT\s+', 'SELECT TOP 5000 ', sql)
        
    return sql
