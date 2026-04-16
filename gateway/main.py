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
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from .executor import execute_query, execute_raw_sql
from .planner import plan_query
from .llm_planner import plan_query_llm
from .llm_sql import generate_sql_dynamic
from .narrator import narrate
from .sql_templates import SQL_TEMPLATES, VALID_INTENTS
from .validators import load_entity_cache
from .telemetry import log_error, log_plan, log_narration

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
    original_query: str = ""


class FullResponse(BaseModel):
    """Complete pipeline response: plan + data + narration."""
    plan: dict
    data: dict
    answer: str
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
    print("\n" + "🚀" * 30)
    print(f"[*] INCOMING REQUEST FROM ORCHESTRATE")
    print(f"[*] PAYLOAD: {request.dict()}")
    print("🚀" * 30 + "\n")

    # Step 1: Plan (with confidence scoring)
    plan = plan_query(request.query)
    confidence = plan.get("confidence", 0.0)
    log_plan(
        user_question=request.query,
        extracted_intent=plan.get("intent", "unknown"),
        extracted_params=plan.get("params", {}),
        confidence=confidence,
    )

    # Step 1B: LLM Planner Fallback
    if plan.get("mode") == "dynamic" or confidence < 0.70:
        logger.info(f"Low confidence ({confidence}) or dynamic mode. Falling back to LLM Planner.")
        plan = plan_query_llm(request.query)

    # TERMINAL LOGGING: Visual Audit of identified strategy
    print("\n" + "="*50)
    print(f"[*] QUERY IDENTIFIED: {plan.get('intent', 'DYNAMIC_GENERATION')}")
    print(f"[*] STRATEGY:         {plan.get('mode', 'N/A').upper()}")
    print(f"[*] PARAMETERS:       {plan.get('params', {})}")
    print(f"[*] USER QUESTION:    {request.query}")
    print("="*50 + "\n")

    # Step 2: Execute
    if plan.get("mode") == "template":
        data = execute_query(intent=plan["intent"], params=plan["params"])
    else:
        # Step 2B: LLM SQL Generation Fallback
        logger.info({
            "mode": "dynamic",
            "reason": "low_confidence_or_dynamic",
            "query": request.query
        })
        sql_info = generate_sql_dynamic(request.query)
        if sql_info["status"] == "error":
            logger.error(f"Dynamic SQL Generation Failed: {sql_info['error']}")
            raise HTTPException(
                status_code=500, 
                detail=f"I couldn't run this dynamic query safely. Error: {sql_info['error']}"
            )
        else:
            # Validate and execute
            data = execute_raw_sql(sql_info["sql"])
            if data["status"] == "error":
                logger.error(f"Query execution failed: {data['message']}")
                raise HTTPException(
                    status_code=500, 
                    detail=f"Query execution failed!: {data['message']}"
                )
            # GUARDRAIL: If dynamic SQL returned zero rows, say so explicitly.
            # Do NOT let Watsonx/narrator estimate or fabricate data.
            if data.get("row_count", 0) == 0:
                pipeline_ms = round((time.time() - pipeline_start) * 1000)
                return FullResponse(
                    plan=plan,
                    data=data,
                    answer="No data available for the specified criteria. Try broadening your filters or time range.",
                    pipeline_ms=pipeline_ms,
                )

    # Step 3: Narrate
    narration_start = time.time()
    answer = narrate(plan, data)
    narration_ms = round((time.time() - narration_start) * 1000)

    # Confidence warning for medium-confidence matches
    if plan.get("mode") == "template" and 0.70 <= confidence < 0.85:
        answer = f"⚠️ Low confidence match ({confidence:.0%}). Verify this is what you asked for.\n\n{answer}"
    elif plan.get("mode") == "dynamic":
        answer = f"🧠 Generated via AI\n\n{answer}"

    log_narration(
        intent=plan["intent"],
        input_row_count=data.get("row_count", 0),
        output_length=len(answer),
        latency_ms=narration_ms,
    )

    pipeline_ms = round((time.time() - pipeline_start) * 1000)

    return FullResponse(
        plan=plan,
        data=data,
        answer=answer,
        pipeline_ms=pipeline_ms,
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
