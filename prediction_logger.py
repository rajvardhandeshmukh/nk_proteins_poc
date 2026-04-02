"""
Prediction Logger — Ground Truth Foundation
============================================
Logs every ML prediction with input metadata and output summary.
When actuals arrive, the error_pct column enables drift detection.

Usage:
    from prediction_logger import log_prediction
    log_prediction("XGBoost", "sales", {"rows": 450, "date_range": "..."}, {"forecast": 1200000}, mape=8.4)
"""

import os
import json
from datetime import datetime

LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
PREDICTION_LOG = os.path.join(LOG_DIR, 'predictions.jsonl')
TRAINING_LOG   = os.path.join(LOG_DIR, 'training_history.jsonl')


def log_prediction(model_name, pillar, input_summary, prediction_summary, mape=None):
    """Log a single prediction event for future ground-truth comparison."""
    os.makedirs(LOG_DIR, exist_ok=True)
    entry = {
        "timestamp":          datetime.now().isoformat(),
        "model":              model_name,
        "pillar":             pillar,
        "input_summary":      input_summary,
        "prediction_summary": prediction_summary,
        "mape_at_prediction": mape,
        "actual":             None,       # Filled later when ground truth arrives
        "error_pct":          None,       # Calculated: abs(actual - predicted) / actual * 100
    }
    with open(PREDICTION_LOG, 'a', encoding='utf-8') as f:
        f.write(json.dumps(entry, default=str) + '\n')


def log_training_event(model_name, pillar, mape, row_count, duration_sec, model_path):
    """Log a model training/retraining event."""
    os.makedirs(LOG_DIR, exist_ok=True)
    entry = {
        "timestamp":    datetime.now().isoformat(),
        "model":        model_name,
        "pillar":       pillar,
        "mape":         mape,
        "rows_trained": row_count,
        "duration_sec": round(duration_sec, 2),
        "model_path":   model_path,
    }
    with open(TRAINING_LOG, 'a', encoding='utf-8') as f:
        f.write(json.dumps(entry, default=str) + '\n')


def get_latest_training_stats():
    """Read the training history and return the latest entry per model."""
    if not os.path.exists(TRAINING_LOG):
        return {}
    
    latest = {}
    with open(TRAINING_LOG, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                entry = json.loads(line.strip())
                key = entry["model"]
                latest[key] = entry
            except (json.JSONDecodeError, KeyError):
                continue
    return latest
