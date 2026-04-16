"""
LLM Client — IBM Granite via Watsonx (Floor 1)
================================================
Thin wrapper around the watsonx SDK. Used ONLY by:
  - llm_planner.py  (intent extraction fallback)
  - llm_sql.py      (dynamic SQL generation)
  - narrator.py     (executive narration — Phase 2B)

The LLM is NEVER allowed to execute queries or touch the database.
It only generates text. Execution always flows through Floor 2.
"""

import os
import json
import time
import logging
from dotenv import load_dotenv

from .telemetry import log_error

load_dotenv()
logger = logging.getLogger(__name__)

# Default model — Fast enterprise model
DEFAULT_MODEL = "ibm/granite-4-h-small"  # Upgraded from 3-8b per environment support

# SDK-level objects — lazy-initialized
_credentials = None
_project_id = None


def _get_watsonx_config():
    """Lazy-load watsonx credentials. Fail loudly if missing."""
    global _credentials, _project_id

    if _credentials is not None:
        return _credentials, _project_id

    try:
        from ibm_watsonx_ai import Credentials

        api_key = os.environ.get("WATSONX_API_KEY")
        url = os.environ.get("WATSONX_URL", "https://eu-de.ml.cloud.ibm.com")
        _project_id = os.environ.get("WATSONX_PROJECT_ID")

        if not api_key or not _project_id:
            raise EnvironmentError(
                "Missing WATSONX_API_KEY or WATSONX_PROJECT_ID in .env"
            )

        _credentials = Credentials(url=url, api_key=api_key)
        logger.info("Watsonx credentials loaded for project: %s", _project_id)
        return _credentials, _project_id

    except ImportError:
        raise ImportError(
            "ibm_watsonx_ai is not installed. Run: pip install ibm-watsonx-ai"
        )


def call_granite(
    user_prompt: str,
    system_prompt: str,
    model_id: str = None,
    max_tokens: int = 512,
    temperature: float = 0.1,
    is_json: bool = True,
) -> dict:
    """
    Call IBM Granite via Watsonx.

    Returns:
        {
            "text": "...",
            "model": "...",
            "tokens": 42,
            "latency_ms": 1234,
            "status": "success"
        }

    On error, returns {"status": "error", "text": "", "error": "..."}.
    """
    model_id = model_id or DEFAULT_MODEL
    start = time.time()

    try:
        from ibm_watsonx_ai.foundation_models import ModelInference
        from ibm_watsonx_ai.metanames import GenTextParamsMetaNames as GenParams

        credentials, project_id = _get_watsonx_config()

        params = {
            GenParams.DECODING_METHOD: "greedy",
            GenParams.MIN_NEW_TOKENS: 1,
            GenParams.MAX_NEW_TOKENS: max_tokens,
            GenParams.TEMPERATURE: temperature,
        }
        if is_json:
            params[GenParams.STOP_SEQUENCES] = ["}"]

        model = ModelInference(
            model_id=model_id,
            params=params,
            credentials=credentials,
            project_id=project_id,
        )

        sys_tag_open = chr(60) + "|system|>"
        sys_tag_close = chr(60) + "|/system|>"
        usr_tag_open = chr(60) + "|user|>"
        usr_tag_close = chr(60) + "|/user|>"
        ast_tag_open = chr(60) + "|assistant|>"

        full_prompt = (
            f"{sys_tag_open}\n{system_prompt}\n{sys_tag_close}\n"
            f"{usr_tag_open}\n{user_prompt}\n{usr_tag_close}\n"
            f"{ast_tag_open}\n"
        )

        raw_response = model.generate(prompt=full_prompt)
        duration_ms = round((time.time() - start) * 1000)

        result = raw_response.get("results", [{}])[0]
        response_text = result.get("generated_text", "").strip()
        token_count = result.get("generated_token_count", 0)

        logger.info("Granite call: model=%s, tokens=%d, latency=%dms", model_id, token_count, duration_ms)

        return {
            "text": response_text,
            "model": model_id,
            "tokens": token_count,
            "latency_ms": duration_ms,
            "status": "success"
        }

    except Exception as e:
        duration_ms = round((time.time() - start) * 1000)
        error_msg = f"Granite LLM error: {str(e)}"
        
        if "timeout" in str(e).lower() or isinstance(e, TimeoutError):
            error_type = "granite_timeout"
            error_msg = f"Granite API Timeout: {str(e)}"
        else:
            error_type = "granite_call_failed"
            
        logger.error(error_msg)
        log_error(
            endpoint="llm_client",
            error_type=error_type,
            message=error_msg,
            context={"model": model_id, "latency_ms": duration_ms},
        )
        return {
            "text": "",
            "model": model_id,
            "tokens": 0,
            "latency_ms": duration_ms,
            "status": "error",
            "error": error_msg,
        }
