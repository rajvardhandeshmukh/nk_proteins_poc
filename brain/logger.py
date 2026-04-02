"""
NK Protein CoPilot — AI Interaction Logger (Brain)
===================================================
Manages system audit logs and session chat history persistence.
"""

import os
import json
import datetime

def log_interaction(request_payload, response_data, error=None):
    """
    Logs every AI transaction to a standardized JSON audit file.
    Ensures that for every prompt sent, the system response or error is recorded.
    """
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = {
        "timestamp": timestamp,
        "request": request_payload,
        "response": response_data,
        "error": error
    }
    
    log_file = os.path.join(log_dir, 'chatbot_api.log')
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(json.dumps(log_entry) + "\n")

def save_chat_history(history, session_id="default"):
    """
    Saves a given chat history list to a dedicated session JSON file.
    """
    history_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs', 'sessions')
    if not os.path.exists(history_dir):
        os.makedirs(history_dir)
    
    file_path = os.path.join(history_dir, f"{session_id}.json")
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(history, f, indent=2)

def load_chat_history(session_id="default"):
    """
    Loads chat history for a specific session ID.
    Returns an empty list if No history exists.
    """
    file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs', 'sessions', f"{session_id}.json")
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []
