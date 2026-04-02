import logging
import importlib
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from .utils import get_mssql_engine
from .config import FULL_TABLE_QUERIES

logger = logging.getLogger(__name__)


def _log_prediction(pillar, result, row_count):
    """Log prediction event for ground-truth tracking."""
    try:
        from prediction_logger import log_prediction
        model_name = result.get('module', pillar)
        mape = result.get('mape', result.get('xgb_mape', None))
        
        # Build a lightweight summary (not the full result)
        pred_summary = {}
        for k in ('forecast_next_3_months', 'total_overdue', 'total_itc_at_risk',
                   'dead_stock_count', 'isolation_forest_flags', 'trend'):
            if k in result:
                pred_summary[k] = result[k]
        
        log_prediction(
            model_name=model_name,
            pillar=pillar,
            input_summary={"rows": row_count},
            prediction_summary=pred_summary,
            mape=mape
        )
    except Exception as e:
        logger.warning("Prediction logging failed: %s", e)


def run_ml_for_pillar(df, pillar, context_intent):
    """Routes rowset payload into native ML models."""
    if len(df) < 15:
        return {"status": "ml_bypass", "error": "Insufficient data (min 15 rows).", "_confidence": "blocked"}
    
    try:
        if pillar == 'sales' and context_intent == 'segmentation':
            import models.profitability
            result = models.profitability.run_profitability(df=df)
        elif pillar == 'sales':
            import models.sales
            result = models.sales.run_sales(df=df, mode="infer")
        elif pillar == 'cashflow':
            import models.cashflow
            result = models.cashflow.run_cashflow(df=df)
        elif pillar == 'gst':
            import models.gst
            result = models.gst.run_gst(df=df, mode="infer")
        elif pillar == 'inventory':
            import models.inventory
            result = models.inventory.run_inventory(df=df)
        else:
            return {"status": "ml_bypass", "error": f"No model for pillar: {pillar}", "_confidence": "blocked"}
        
        # Log the prediction for ground-truth tracking
        _log_prediction(pillar, result, len(df))
        return result
        
    except Exception as e:
        logger.error("ML model failed — pillar: %s, error: %s", pillar, e, exc_info=True)
        return {"status": "ml_bypass", "error": str(e), "_confidence": "blocked"}


def handle_multi_pillar():
    """Parallel execution of all 4 ML pillars."""
    engine = get_mssql_engine()
    pillar_config = {
        'sales':     (FULL_TABLE_QUERIES['sales'],     'models.sales',       'run_sales'),
        'cashflow':  (FULL_TABLE_QUERIES['cashflow'],  'models.cashflow',    'run_cashflow'),
        'gst':       (FULL_TABLE_QUERIES['gst'],       'models.gst',         'run_gst'),
        'inventory': (FULL_TABLE_QUERIES['inventory'], 'models.inventory',   'run_inventory'),
    }
    
    def run_one(name, sql, mod_path, fn_name):
        mod = importlib.import_module(mod_path)
        fn = getattr(mod, fn_name)
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)
        return name, fn(df=df), len(df)
    
    results, total_rows = {}, 0
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(run_one, n, s, m, f): n for n, (s, m, f) in pillar_config.items()}
        for f in as_completed(futures):
            try:
                name, res, rows = f.result()
                results[name] = res
                total_rows += rows
            except Exception as e:
                logger.error("Multi-pillar worker failed for domain %s: %s", futures[f], e, exc_info=True)
    
    return results, total_rows
