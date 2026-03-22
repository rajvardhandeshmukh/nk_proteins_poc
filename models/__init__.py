import os
import json

from .sales import run_sales
from .cashflow import run_cashflow
from .inventory import run_inventory
from .gst import run_gst
from .profitability import run_profitability

def load_all():
    CACHE_FILE = "cached_model_output.json"
    
    if os.path.exists(CACHE_FILE):
        print("\n [INFO] Instantly loading pre-trained AI calculations from local disk cache...")
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
            
    print("\n Running all modules on custom SAP dataset...")
    print(" ─────────────────────────────────────────")

    print(" [1/5] Sales forecast (XGBoost + Prophet)...")
    s = run_sales()
    print(f"       XGBoost MAPE: {s['xgboost_mape']}%  |  Trend: {s['trend']}")

    print(" [2/5] Cash flow & AR aging...")
    c = run_cashflow()
    print(f"       Overdue: {c['total_overdue']:,}  |  DSO: {c['dso_days']} days")

    print(" [3/5] Inventory optimization...")
    i = run_inventory()
    print(f"       Dead stock: {i['dead_stock_count']} SKUs  |  Capital locked: {i['total_capital_locked']:,}")

    print(" [4/5] GST reconciliation...")
    g = run_gst()
    print(f"       Mismatches: {g['total_mismatches']}  |  ITC at risk: {g['total_itc_at_risk']:,}")

    print(" [5/5] Profitability & customer segments...")
    p = run_profitability()
    print(f"       Promote: {p['promote_count']}  |  Discontinue: {p['discontinue_count']}")

    print(" ─────────────────────────────────────────")
    print(" All models complete. Ready for chatbot.\n")

    final_data = {
        "sales":         s,
        "cashflow":      c,
        "inventory":     i,
        "gst":           g,
        "profitability": p
    }
    
    with open(CACHE_FILE, 'w') as f:
        json.dump(final_data, f, default=str)
        
    return final_data
