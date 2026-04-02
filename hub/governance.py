import pandas as pd
from .config import PILLAR_DATE_COL

# Strict constraint maps
PILLAR_CONTRACTS = {
    'sales': {
        'required_cols': ['date', 'product_id', 'product_name', 'region', 'revenue', 'quantity_sold'],
        'numeric_cols': ['revenue', 'quantity_sold']
    },
    'cashflow': {
        'required_cols': ['invoice_date', 'received_date', 'invoice_amount', 'days_overdue', 'aging_bucket'],
        'numeric_cols': ['invoice_amount', 'days_overdue']
    },
    'gst': {
        'required_cols': ['invoice_no', 'return_period', 'mismatch_flag', 'total_tax_amount'],
        'numeric_cols': ['total_tax_amount']
    },
    'inventory': {
        'required_cols': ['snapshot_date', 'sku', 'current_stock_kg', 'is_dead_stock', 'total_value_inr'],
        'numeric_cols': ['current_stock_kg', 'total_value_inr']
    }
}

def validate_ml_data_contract(df, pillar):
    """Enforces mathematical guarantees on DataFrames."""
    contract = PILLAR_CONTRACTS.get(pillar)
    if not contract: return True, df, ""
    
    missing_cols = [col for col in contract['required_cols'] if col not in df.columns]
    if missing_cols: return False, df, f"Schema mismatch: Missing columns {missing_cols}."
        
    date_col = PILLAR_DATE_COL.get(pillar)
    if date_col and date_col in df.columns:
        if df[date_col].dtype == 'object': df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
            return False, df, f"Data Type Error: Column '{date_col}' format is invalid."
            
    for num_col in contract['numeric_cols']:
        if num_col in df.columns:
            valid_pct = df[num_col].notnull().mean()
            if valid_pct < 0.95:
                return False, df, f"Data Quality Error: Column '{num_col}' contains {(1-valid_pct)*100:.1f}% NULL values."
                
    return True, df, ""

import json
from pathlib import Path

_cfg_cache = {}
_cfg_mtime = None

def _load_cfg():
    global _cfg_cache, _cfg_mtime
    p = Path(__file__).parent / "governance_config.json"
    try:
        mtime = p.stat().st_mtime
        if not _cfg_cache or mtime != _cfg_mtime:
            with open(p, 'r') as f:
                _cfg_cache = json.load(f)
            _cfg_mtime = mtime
    except Exception:
        # Fallback to hardcoded defaults if file missing/unreadable
        return {"mape_gates": {"high_threshold": 10.0, "caution_threshold": 25.0}}
    return _cfg_cache

def apply_confidence_gate(ml_result):
    """Adds reliability warnings back to the ML result based on dynamic config."""
    cfg = _load_cfg().get("mape_gates", {"high_threshold": 10.0, "caution_threshold": 25.0})
    high_t = cfg["high_threshold"]
    blk_t  = cfg["caution_threshold"]
    
    confidence = "high"
    warnings = []
    
    mape_values = [v for k, v in ml_result.items() if 'mape' in k.lower() and isinstance(v, (int, float))]
    mape = max(mape_values) if mape_values else None

    if mape is not None:
        if mape > blk_t:
            confidence = "blocked"
            warnings.append(f"Forecast reliability below threshold (MAPE: {mape:.1f}%).")
        elif mape > high_t:
            confidence = "caution"
            warnings.append(f"Caution: Model accuracy is {mape:.1f}% off (target: ≤{high_t}%).")

    ml_result["_confidence"] = confidence
    ml_result["_warnings"]   = warnings
    ml_result["_mape_used"]  = mape
    return ml_result

def sanitize_ml_output(ml_result):
    """Clean the payload before sending to LLM."""
    sanitized = {}
    skip_keys = {'historical_monthly_revenue', '_confidence', '_warnings'}
    for k, v in ml_result.items():
        if k in skip_keys: continue
        if "customer_share" in k and isinstance(v, (int, float)) and v > 0.60:
            sanitized[k] = "High Concentration Detected (>60%)"
        else:
            sanitized[k] = v
    return sanitized
