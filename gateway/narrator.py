import logging
import json
from .llm_client import call_granite

logger = logging.getLogger(__name__)

def narrate(plan: dict, data: dict) -> str:
    """
    Turn structured data into a human-readable answer natively utilizing the Enterprise Granite LLM.
    Phase 2B: Dynamic Executive Summaries.
    """
    intent = plan.get("intent", "unknown")
    params = plan.get("params", {})
    rows = data.get("data", [])
    row_count = data.get("row_count", 0)
    status = data.get("status", "error")

    # Handle errors
    if status == "error":
        return f"I couldn't retrieve the data. Error: {data.get('message', 'Unknown error')}. Please try rephrasing your question."

    # Handle empty results
    if row_count == 0:
        return "No data found matching your criteria. Try broadening your filters or time range."

    # Handle corrections
    corrections = data.get("corrections", {})
    correction_note = ""
    if corrections:
        fixes = [f"'{v['original']}' → '{v['corrected']}'" for v in corrections.values()]
        correction_note = f"\n*(Auto-corrected: {', '.join(fixes)})*\n\n"

    # Send the raw data directly to Granite for decoration
    sys_prompt = (
        "You are an elite Executive Assistant for NK Proteins. Summarize the provided data. "
        "CRITICAL RULE: We represent a major Indian manufacturer. Every currency value MUST be prefixed with ₹ (INR). "
        "The dollar ($) is strictly forbidden and must never be used. "
        "Avoid tables. Write 2 concise, facts-driven sentences in professional English."
    )
    
    user_prompt = (
        f"Context: Compute {intent} with parameters {params}.\n"
        f"Data (rows: {row_count}):\n"
        f"```json\n{json.dumps(rows[:5], indent=2)}\n```\n"
        "Summary:"
    )
    
    logger.info("Triggering LLM Executive Decoration (Phase 2B) for intent: %s", intent)
    try:
        response = call_granite(
            user_prompt=user_prompt,
            system_prompt=sys_prompt,
            max_tokens=300,
            temperature=0.2,
            is_json=False
        )
        if response.get("status") == "success":
            return correction_note + response.get("text", "").strip()
        else:
            logger.error("Granite generation failed: %s", response.get("error"))
            return _fallback_hardcoded(intent, params, data, correction_note)
    except Exception as e:
        logger.error("Exception during LLM narration: %s", str(e))
        return _fallback_hardcoded(intent, params, data, correction_note)

def _fallback_hardcoded(intent: str, params: dict, data: dict, correction_note: str) -> str:
    """Fallback to simple rules if LLM fails."""
    rows = data.get("data", [])
    row_count = data.get("row_count", 0)

    if intent == "region_comparison":
        top_region = rows[0].get("region", "N/A")
        total_rev = sum(r.get("total_revenue", 0) for r in rows)
        return (
            f"Regional Performance Summary (INR):\n"
            f"• Total revenue across all regions: ₹{total_rev:,.2f}\n"
            f"• Leading region: {top_region}\n"
            f"• All figures accurately reported in Indian Rupees (₹)."
        )

    if intent == "revenue_trend":
        months = params.get("months_back", 12)
        total = sum(r.get("total_revenue", 0) for r in rows)
        return (
            f"Business Performance Summary:\n"
            f"• {months}-month revenue total: ₹{total:,.2f}\n"
            f"• All monetary data processed and reported in INR (₹)."
        )

    return f"Retrieved {row_count} rows for '{intent}'. All monetary values strictly in INR (₹).{correction_note}"
