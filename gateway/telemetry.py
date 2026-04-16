"""
Telemetry Logger — Your Competitive Moat
==========================================
Logs every query, every prediction, every error.
This NEVER moves to IBM. This stays local. Always.
"""

import os
import json
import time
from datetime import datetime

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs', 'gateway')


def _write_log(filename: str, entry: dict):
    """Append a single JSON line to the specified log file."""
    os.makedirs(LOG_DIR, exist_ok=True)
    filepath = os.path.join(LOG_DIR, filename)
    with open(filepath, 'a', encoding='utf-8') as f:
        f.write(json.dumps(entry, default=str) + "\n")


def log_query(intent: str, query: str, params: dict, row_count: int, latency_ms: int, status: str):
    """Log every SQL query execution."""
    _write_log("query_telemetry.jsonl", {
        "timestamp": datetime.now().isoformat(),
        "intent": intent,
        "sql_query": query,
        "params": params,
        "row_count": row_count,
        "latency_ms": latency_ms,
        "status": status,
    })


def log_plan(user_question: str, extracted_intent: str, extracted_params: dict, confidence: float):
    """Log every LLM intent extraction attempt."""
    _write_log("plan_telemetry.jsonl", {
        "timestamp": datetime.now().isoformat(),
        "user_question": user_question,
        "extracted_intent": extracted_intent,
        "extracted_params": extracted_params,
        "confidence": confidence,
    })


def log_narration(intent: str, input_row_count: int, output_length: int, latency_ms: int):
    """Log every LLM narration call."""
    _write_log("narration_telemetry.jsonl", {
        "timestamp": datetime.now().isoformat(),
        "intent": intent,
        "input_rows": input_row_count,
        "output_chars": output_length,
        "latency_ms": latency_ms,
    })


def log_error(endpoint: str, error_type: str, message: str, context: dict = None):
    """Log every error for debugging."""
    _write_log("error_log.jsonl", {
        "timestamp": datetime.now().isoformat(),
        "endpoint": endpoint,
        "error_type": error_type,
        "message": message,
        "context": context or {},
    })
