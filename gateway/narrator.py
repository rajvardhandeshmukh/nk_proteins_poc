import logging
import json
from .llm_client import call_llm

logger = logging.getLogger(__name__)

def narrate(plan: dict, data: dict) -> str:
    """
    V2 Narrator: Strictly adheres to Ground Truth Protocol.
    Reports: Revenue, Quantity, Price.
    """
    intent = plan.get("intent", "unknown")
    rows = data.get("data", [])
    row_count = data.get("row_count", 0)
    status = data.get("status", "error")

    # 0. Specialized View Bypass (Prevents Hallucination for Raw Data)
    if intent == "transaction_view":
        return f"I have retrieved the latest {row_count} transactions from the database as requested."

    # 1. Error Handling
    if status == "error":
        return f"I encountered an error while processing your request: {data.get('message', 'System busy')}. Please try again."

    if row_count == 0:
        return "The requested analysis returned no matching transactions in the current dataset."

    # 2. System Prompt Enforcement (The Laws)
    sys_prompt = (
        "You are the NK Proteins Sales Analyst. Your responses are strictly data-driven and visually premium. "
        "RULES:\n"
        "1. Report only three metrics: Revenue, Quantity, and Price.\n"
        "2. DEFINITION: Price is the 'Aggregated Price' (Revenue divided by Quantity).\n"
        "3. CURRENCY: Always use ₹ (INR). Never use $.\n"
        "4. TABLE FORMATTING: \n"
        "   - If there are multiple rows, show ONLY the Top 10 results.\n"
        "   - Use standard Markdown tables with headers.\n"
        "   - IMPORTANT: Every row MUST start on a NEW LINE. Do not put multiple rows on one line.\n"
        "   - Use clean headers: 'Rank', 'Customer/Product', 'Total Revenue (₹)', 'Total Quantity', 'Avg Price (₹)'.\n"
        "5. NO PROFITABILITY: Do not mention margins, costs, or regions.\n"
        "6. CONCISE: Max 4 sentences plus the table."
    )

    # 3. Prepare Data for LLM
    # We pass the top results and the global total
    total_rev = 0
    total_qty = 0
    for r in rows:
        # Flexible mapping for Revenue
        rev_val = r.get("Total Revenue") or r.get("Gross Value") or r.get("Revenue") or 0
        total_rev += float(rev_val)
        
        # Flexible mapping for Quantity
        qty_val = r.get("Total Quantity") or r.get("Bill Qty") or r.get("Quantity") or 0
        total_qty += float(qty_val)

    user_prompt = (
        f"Intent: {intent}\n"
        f"Calculated Totals:\n"
        f"- Total Revenue: ₹{total_rev:,.2f}\n"
        f"- Total Quantity: {total_qty:,.2f}\n"
        f"\nRaw Data (Top Results):\n"
        f"```json\n{json.dumps(rows[:10], indent=2)}\n```\n"
        "Provide a concise executive summary:"
    )

    # 4. Intelligent Narration
    try:
        response = call_llm(
            user_prompt=user_prompt,
            system_prompt=sys_prompt,
            max_tokens=300,
            temperature=0.0,
            is_json=False
        )
        
        if response.get("status") == "success":
            return response.get("text", "").strip()
        else:
            return _fallback_narration(rows, total_rev, total_qty)
    except Exception as e:
        logger.error(f"Narration failed: {e}")
        return _fallback_narration(rows, total_rev, total_qty)

def _fallback_narration(rows, total_rev, total_qty):
    """Simple text fallback if LLM fails."""
    summary = (
        f"Sales Analysis Summary:\n"
        f"• Total Revenue: ₹{total_rev:,.2f}\n"
        f"• Total Quantity: {total_qty:,.2f}\n"
        f"• Based on {len(rows)} entries in the dataset."
    )
    return summary
