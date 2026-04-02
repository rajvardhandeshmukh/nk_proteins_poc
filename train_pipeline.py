"""
NK Protein CoPilot — Model Train/Retrain Pipeline
===================================================
Run this script to retrain all ML models on the latest data.

Usage:
    python train_pipeline.py              # Retrain all models
    python train_pipeline.py --pillar sales   # Retrain sales only
    python train_pipeline.py --pillar gst     # Retrain GST only

Schedule (Windows Task Scheduler):
    Program: python
    Arguments: D:\\projects\\nk_protein_poc\\train_pipeline.py
    Trigger: Weekly, Monday 06:00 AM

Schedule (Linux cron):
    0 6 * * 1 cd /app && python train_pipeline.py >> logs/retrain.log 2>&1
"""

import os
import sys
import time
import argparse
import pandas as pd
from datetime import datetime

# Ensure project root is in path
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()


def retrain_sales():
    """Pull latest data from SQL Server and retrain XGBoost + Prophet."""
    print(f"\n{'='*60}")
    print(f"[{datetime.now()}] SALES RETRAIN STARTED")
    print(f"{'='*60}")

    start = time.time()

    try:
        from hub.utils import get_mssql_engine
        from hub.config import FULL_TABLE_QUERIES

        engine = get_mssql_engine()
        with engine.connect() as conn:
            df = pd.read_sql(FULL_TABLE_QUERIES['sales'], conn)

        print(f"[*] Loaded {len(df)} rows from SQL Server.")
        print(f"[*] Date range: {df['date'].min()} to {df['date'].max()}")

        from models.sales import run_sales
        result = run_sales(df=df, mode="train")

        duration = time.time() - start
        print(f"\n[✓] SALES RETRAIN COMPLETE in {duration:.1f}s")
        print(f"    XGBoost MAPE:  {result.get('xgb_mape', 'N/A')}%")
        print(f"    Prophet MAPE:  {result.get('prophet_mape', 'N/A')}%")
        print(f"    Max MAPE:      {result.get('mape', 'N/A')}%")
        print(f"    Winner Model:  {result.get('winner_model', 'N/A')}")
        return result

    except Exception as e:
        print(f"\n[✗] SALES RETRAIN FAILED: {e}")
        import traceback
        traceback.print_exc()
        return None


def retrain_gst():
    """Pull latest data from SQL Server and retrain IsolationForest."""
    print(f"\n{'='*60}")
    print(f"[{datetime.now()}] GST RETRAIN STARTED")
    print(f"{'='*60}")

    start = time.time()

    try:
        from hub.utils import get_mssql_engine
        from hub.config import FULL_TABLE_QUERIES

        engine = get_mssql_engine()
        with engine.connect() as conn:
            df = pd.read_sql(FULL_TABLE_QUERIES['gst'], conn)

        print(f"[*] Loaded {len(df)} rows from SQL Server.")

        from models.gst import run_gst
        result = run_gst(df=df, mode="train")

        duration = time.time() - start
        print(f"\n[✓] GST RETRAIN COMPLETE in {duration:.1f}s")
        print(f"    Total Invoices:        {result.get('total_invoices', 'N/A')}")
        print(f"    IsolationForest Flags: {result.get('isolation_forest_flags', 'N/A')}")
        return result

    except Exception as e:
        print(f"\n[✗] GST RETRAIN FAILED: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    parser = argparse.ArgumentParser(description="NK Protein CoPilot — Model Retrain Pipeline")
    parser.add_argument('--pillar', type=str, choices=['sales', 'gst', 'all'],
                        default='all', help='Which pillar to retrain (default: all)')
    args = parser.parse_args()

    print(f"\n{'#'*60}")
    print(f"# NK Protein CoPilot — Scheduled Retrain")
    print(f"# Timestamp: {datetime.now().isoformat()}")
    print(f"# Target:    {args.pillar}")
    print(f"{'#'*60}")

    total_start = time.time()

    if args.pillar in ('sales', 'all'):
        retrain_sales()

    if args.pillar in ('gst', 'all'):
        retrain_gst()

    total_duration = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"[✓] ALL RETRAINS COMPLETE — Total time: {total_duration:.1f}s")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
