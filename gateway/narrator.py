import logging
import json
from .llm_client import call_llm

logger = logging.getLogger(__name__)

def narrate(plan: dict, data: dict) -> str:
    """
    V2 Narrator: Strictly adheres to Ground Truth Protocol.
    Reports: Revenue, Quantity, Price, and Volume Metrics.
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
        "1. ONLY report metrics that are present in the provided JSON data. Do NOT calculate or infer new metrics.\n"
        "2. FORBIDDEN: Do not calculate 'Aggregated Price', 'Average Price', or any other ratio unless it is provided as a key in the JSON.\n"
        "3. DO NOT hallucinate columns like 'Customer Count' or 'Product ID' if they are missing from the JSON data.\n"
        "4. CURRENCY: Always use ₹ (INR).\n"
        "5. TABLE FORMATTING: \n"
        "   - Use standard Markdown tables.\n"
        "   - Metrics: Use the exact names from the JSON keys (standardized to Title Case for display).\n"
        "6. NO PROFITABILITY: Do not mention costs or margins unless explicitly requested.\n"
        "7. CONCISE: Max 5 sentences plus the table."
    )

    # 3. Prepare Data for LLM
    total_rev = 0
    total_qty = 0
    total_invoices = 0
    total_lines = 0
    total_customers = 0

    # Dynamically track what metrics we actually have
    has_customers = False
    has_lines = False

    for r in rows:
        rev_val = r.get("total_revenue") or r.get("Total Revenue") or r.get("Revenue") or 0
        total_rev += float(rev_val)
        
        qty_val = r.get("total_quantity") or r.get("Total Quantity") or r.get("Quantity") or 0
        total_qty += float(qty_val)

        inv_val = r.get("invoice_count") or r.get("Invoice Count") or 0
        total_invoices += int(inv_val)

        if "line_item_count" in r or "Line Item Count" in r:
            has_lines = True
            line_val = r.get("line_item_count") or r.get("Line Item Count") or 0
            total_lines += int(line_val)

        if "customer_count" in r or "Customer Count" in r:
            has_customers = True
            cust_val = r.get("customer_count") or r.get("Customer Count") or 0
            total_customers += int(cust_val)

    totals_summary = [f"- Total Revenue: ₹{total_rev:,.2f}", f"- Total Invoices: {total_invoices:,}"]
    if has_lines:
        totals_summary.append(f"- Total Line Items: {total_lines:,}")
    if has_customers:
        totals_summary.append(f"- Unique Customers: {total_customers:,}")

    user_prompt = (
        f"Intent: {intent}\n"
        f"Calculated Totals:\n"
        + "\n".join(totals_summary) +
        f"\n\nRaw Data (Top Results):\n"
        f"```json\n{json.dumps(rows[:10], indent=2)}\n```\n"
        "Provide a concise executive summary and a detailed table based ONLY on the metrics provided above:"
    )

    # 4. Intelligent Narration
    try:
        response = call_llm(
            user_prompt=user_prompt,
            system_prompt=sys_prompt,
            max_tokens=500,
            temperature=0.0,
            is_json=False
        )
        
        if response.get("status") == "success":
            return response.get("text", "").strip()
        else:
            return _fallback_narration(total_rev, total_invoices)
    except Exception as e:
        logger.error(f"Narration failed: {e}")
        return _fallback_narration(total_rev, total_invoices)

def _fallback_narration(total_rev, total_invoices):
    """Simple text fallback if LLM fails."""
    summary = (
        f"Sales Analysis Summary:\n"
        f"• Total Revenue: ₹{total_rev:,.2f}\n"
        f"• Unique Invoices: {total_invoices:,}\n"
    )
    return summary
