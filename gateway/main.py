"""
FastAPI Gateway — NK Proteins CoPilot (IBM Gateway Branch)
============================================================
Phase 2A: Full pipeline — Planner → Executor → Narrator
Rule-based. No LLM. No ML. Just structured queries that always work.

Run: uvicorn gateway.main:app --reload --port 8000
Docs: http://localhost:8000/docs
"""

import os
import time
import logging

from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field
from typing import Optional, List
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from .executor import execute_query, execute_raw_sql
from .planner import plan_query
from .llm_planner import plan_query_llm
from .llm_sql import generate_sql_dynamic
from .narrator import narrate
from .sql_templates import SQL_TEMPLATES, VALID_INTENTS
from .mapping_v2 import get_intent as get_v2_intent
from .validators import load_entity_cache
from .telemetry import log_error, log_plan, log_narration
from .governance import get_reliability, get_governance_notes, detect_conflicts, ReliabilityLevel

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logger = logging.getLogger("gateway")

# =============================================================================
# APP
# =============================================================================

app = FastAPI(
    title="NK Proteins CoPilot Gateway",
    description="Enterprise Analytics API — Natural language → SQL templates → Narrated answers.",
    version="2.0.0",
)

# =============================================================================
# AUTHENTICATION
# =============================================================================
API_KEY = "nk-secret-key"

def verify_key(x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# =============================================================================
# REQUEST / RESPONSE MODELS
# =============================================================================

class QueryRequest(BaseModel):
    """Natural language query from the user."""
    query: str = Field(
        ...,
        description="The user's question in plain English",
        examples=["Show revenue for last 6 months in Gujarat"],
    )
    data_source_verification: str = Field(
        default="ERP_V3",
        description="MANDATORY verification tag"
    )


class PlanResponse(BaseModel):
    """Structured plan extracted from natural language."""
    intent: str
    params: dict = {}
    mode: str = "template"
    confidence: float = 0.0
    reliability_level: ReliabilityLevel = ReliabilityLevel.MEDIUM
    original_query: str = ""


class FullResponse(BaseModel):
    """Complete pipeline response: plan + data + narration."""
    plan: PlanResponse
    data: dict
    answer: str
    governance_notes: List[str] = []
    conflict_flags: List[str] = []
    pipeline_ms: int = 0


class ExecuteRequest(BaseModel):
    """Direct structured input for the SQL executor."""
    intent: str = Field(
        ...,
        description=f"The query intent. Must be one of: {VALID_INTENTS}",
        examples=["revenue_trend"],
    )
    params: dict = Field(
        default={},
        description="Parameters to fill into the SQL template",
        examples=[{"months_back": 6, "region": "Gujarat"}],
    )
    data_source_verification: str = Field(
        default="ERP_V3",
        description="MANDATORY verification tag"
    )


class ExecuteResponse(BaseModel):
    """Structured output from the SQL executor."""
    status: str
    intent: str = ""
    message: str = ""
    data: list = []
    row_count: int = 0
    query_ms: int = 0
    corrections: dict = {}


# =============================================================================
# CORE ENDPOINTS
# =============================================================================

@app.post("/query", response_model=FullResponse)
def api_query(request: QueryRequest, x_api_key: str = Header(None)):
    """
    THE MAIN ENDPOINT — Full pipeline.

    Natural language → Plan → Execute → Narrate → Answer

    Example: {"query": "Show revenue for last 6 months in Gujarat"}
    """
    verify_key(x_api_key)
    pipeline_start = time.time()

    # ENTRY LOGGING: See exactly what Orchestrate sent
    print("\n" + "=" * 30)
    print(f"[*] INCOMING REQUEST FROM ORCHESTRATE")
    print(f"[*] PAYLOAD: {request.dict()}")
    print("=" * 30 + "\n")

    try:
        # [POC V2 BYPASS] Pure Sales Math Fast Path
        v2_match = get_v2_intent(request.query)
        if v2_match and v2_match.get("intent") != "unknown":
            print(f"!!! [POC V2 BYPASS] Matched to V2 Intent: {v2_match['intent']}")
            v2_intent = v2_match["intent"]
            v2_params = v2_match["params"]
            
            # Execute V2 (Fast Track)
            v2_data = execute_query(intent=v2_intent, params=v2_params)
            
            # Narrate V2
            v2_answer = narrate({"intent": v2_intent, "params": v2_params}, v2_data)
            
            pipeline_ms = round((time.time() - pipeline_start) * 1000)
            return FullResponse(
                plan=PlanResponse(intent=v2_intent, params=v2_params, mode="v2_pure_math", confidence=1.0, reliability_level=ReliabilityLevel.HIGH, original_query=request.query),
                data=v2_data,
                answer=f"[POC V2 PURE MATH]\n\n{v2_answer}",
                pipeline_ms=pipeline_ms
            )
        else:
            # User requested to bypass advanced intelligence completely.
            # If no V2 match, we return an error rather than falling back to LLM.
            pipeline_ms = round((time.time() - pipeline_start) * 1000)
            return FullResponse(
                plan=PlanResponse(intent="unsupported", params={}, mode="v2_pure_math", confidence=0.0, reliability_level=ReliabilityLevel.LOW, original_query=request.query),
                data={"status": "error", "message": "Query not supported in pure V2 mode."},
                answer="I am currently in 'Pure Sales Mode' and can only answer predefined queries about revenue, regions, monthly trends, and top products. Please try rephrasing or ask about 'total revenue'.",
                pipeline_ms=pipeline_ms
            )

    except Exception as e:
        logger.error(f"FATAL PIPELINE CRASH: {str(e)}", exc_info=True)
        log_error(
            endpoint="/query",
            error_type="pipeline_panic",
            message=str(e),
            context={"query": request.query}
        )
        
        pipeline_ms = round((time.time() - pipeline_start) * 1000)
        
        # Determine if we even have a plan/data to return
        p = PlanResponse(intent="error", original_query=request.query)
        d = {"status": "error", "message": str(e), "data": [], "row_count": 0}
        
        # Distinguish between network errors and logic errors
        if "granite_network_error" in str(e).lower() or "connection" in str(e).lower():
            friendly_msg = "⚠️ **Service Intermittently Offline**\n\nThe AI system had trouble reaching the summarizing service. However, the data was successfully retrieved from the database and is shown below."
        else:
            friendly_msg = f"⚠️ **System Logic Error**\n\nAn internal error occurred while processing your request: `{str(e)}`. Technical details have been logged."

        return FullResponse(
            plan=p,
            data=d,
            answer=friendly_msg,
            pipeline_ms=pipeline_ms
        )


@app.post("/plan_query", response_model=PlanResponse)
def api_plan_query(request: QueryRequest, x_api_key: str = Header(None)):
    """
    Step 1 only — Extract structured intent from natural language.
    Useful for debugging what the planner thinks the user wants.
    """
    verify_key(x_api_key)
    plan = plan_query(request.query)
    log_plan(
        user_question=request.query,
        extracted_intent=plan.get("intent", "unknown"),
        extracted_params=plan.get("params", {}),
        confidence=plan.get("confidence", 0.0),
    )
    return PlanResponse(**plan)


@app.post("/execute_query", response_model=ExecuteResponse)
def api_execute_query(request: ExecuteRequest, x_api_key: str = Header(None)):
    """
    Step 2 only — Execute a pre-audited SQL template directly.
    Bypasses the planner. For structured API consumers.
    """
    verify_key(x_api_key)
    if request.intent not in VALID_INTENTS:
        log_error(
            endpoint="/execute_query",
            error_type="invalid_intent",
            message=f"Unknown intent: {request.intent}",
            context={"valid_intents": VALID_INTENTS}
        )
        raise HTTPException(
            status_code=400,
            detail=f"Unknown intent: '{request.intent}'. Valid intents: {VALID_INTENTS}"
        )

    result = execute_query(intent=request.intent, params=request.params)
    return ExecuteResponse(**result)

@app.post("/top_products", response_model=FullResponse)
def api_top_products(request: ExecuteRequest, x_api_key: str = Header(None)):
    """Dedicated endpoint for Product Ranking."""
    verify_key(x_api_key)
    pipeline_start = time.time()
    effective_intent = request.intent if request.intent else "top_products"
    plan = {"intent": effective_intent, "params": request.params, "mode": "template"}
    data = execute_query(intent=effective_intent, params=request.params)
    answer = narrate(plan, data)
    pipeline_ms = round((time.time() - pipeline_start) * 1000)
    return FullResponse(plan=plan, data=data, answer=answer, pipeline_ms=pipeline_ms)

@app.post("/region_comparison", response_model=FullResponse)
def api_region_comparison(request: ExecuteRequest, x_api_key: str = Header(None)):
    """Dedicated endpoint for Regional Sales Breakdown (Indian States)."""
    verify_key(x_api_key)
    pipeline_start = time.time()
    effective_intent = request.intent if request.intent else "region_comparison"
    plan = {"intent": effective_intent, "params": request.params, "mode": "template"}
    data = execute_query(intent=effective_intent, params=request.params)
    answer = narrate(plan, data)
    pipeline_ms = round((time.time() - pipeline_start) * 1000)
    return FullResponse(plan=plan, data=data, answer=answer, pipeline_ms=pipeline_ms)

@app.post("/revenue_trend", response_model=FullResponse)
def api_revenue_trend(request: ExecuteRequest, x_api_key: str = Header(None)):
    """Dedicated endpoint for Revenue Performance Trends."""
    verify_key(x_api_key)
    pipeline_start = time.time()
    effective_intent = request.intent if request.intent else "revenue_trend"
    plan = {"intent": effective_intent, "params": request.params, "mode": "template"}
    data = execute_query(intent=effective_intent, params=request.params)
    answer = narrate(plan, data)
    pipeline_ms = round((time.time() - pipeline_start) * 1000)
    return FullResponse(plan=plan, data=data, answer=answer, pipeline_ms=pipeline_ms)

@app.post("/inventory", response_model=FullResponse)
def api_inventory(request: ExecuteRequest, x_api_key: str = Header(None)):
    """Dedicated endpoint for Inventory Analysis (Stock Levels, Dead Stock)."""
    verify_key(x_api_key)
    pipeline_start = time.time()
    effective_intent = request.intent if request.intent else "inventory_health"
    plan = {"intent": effective_intent, "params": request.params, "mode": "template"}
    data = execute_query(intent=effective_intent, params=request.params)
    answer = narrate(plan, data)
    pipeline_ms = round((time.time() - pipeline_start) * 1000)
    return FullResponse(plan=plan, data=data, answer=answer, pipeline_ms=pipeline_ms)

@app.post("/cashflow", response_model=FullResponse)
def api_cashflow(request: ExecuteRequest, x_api_key: str = Header(None)):
    """Dedicated endpoint for Cashflow Analysis (Liquidity, Aging)."""
    verify_key(x_api_key)
    pipeline_start = time.time()
    effective_intent = request.intent if request.intent else "cashflow_projection"
    plan = {"intent": effective_intent, "params": request.params, "mode": "template"}
    data = execute_query(intent=effective_intent, params=request.params)
    answer = narrate(plan, data)
    pipeline_ms = round((time.time() - pipeline_start) * 1000)
    return FullResponse(plan=plan, data=data, answer=answer, pipeline_ms=pipeline_ms)


# =============================================================================
# UTILITY ENDPOINTS
# =============================================================================

@app.get("/health")
def health_check():
    return {"status": "healthyy", "service": "nk-proteins-gateway", "version": "2.0.0"}


@app.get("/templates")
def list_templates():
    """List all available SQL templates and their descriptions."""
    return {
        name: {
            "description": tmpl["description"],
            "table": tmpl["table"],
            "params": {k: v for k, v in tmpl["params"].items()},
            "optional_filters": list(tmpl.get("optional_filters", {}).keys()),
        }
        for name, tmpl in SQL_TEMPLATES.items()
    }


# =============================================================================
# STARTUP
# =============================================================================

@app.on_event("startup")
def startup():
    logger.info("=" * 60)
    logger.info("NK Proteins CoPilot Gateway v2.0.0")
    logger.info("Loading entity cache for fuzzy matching...")
    load_entity_cache()
    logger.info("Templates: %s", VALID_INTENTS)
    logger.info("Endpoints: /query, /plan_query, /execute_query")
    logger.info("=" * 60)
    logger.info("Gateway ONLINE — http://localhost:8000/docs")
