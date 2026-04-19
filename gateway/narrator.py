import logging
import json
from .llm_client import call_granite

logger = logging.getLogger(__name__)

def _extract_revenue(row: dict) -> float:
    """Safely extract revenue from a row using synonymous keys."""
    for key in ["total_net_revenue", "total_revenue", "revenue_30d", "total_net_sales"]:
        if key in row and row[key] is not None:
            try:
                return float(row[key])
            except (ValueError, TypeError):
                continue
    return 0.0

def narrate(plan: dict, data: dict) -> str:
    """
    Turn structured data into a human-readable answer natively utilizing the Enterprise Granite LLM.
    Phase 2B: Dynamic Executive Summaries.
    """
    intent = plan.get("intent", "unknown")
    original_query = plan.get("original_query", "").lower()
    
    # --- Pillar Matching Guardrail ---
    # Detect if user asked for "Stock/Inventory" but system served "Sales/Revenue" (or vice versa)
    inventory_keywords = ["stock", "inventory", "valuation", "on-hand", "on hand", "units available"]
    sales_keywords     = ["revenue", "sales", "sold", "performance", "top product"]
    
    asked_for_inventory = any(k in original_query for k in inventory_keywords)
    asked_for_sales     = any(k in original_query for k in sales_keywords)
    
    served_from_sales     = "sales" in intent or intent in ["region_comparison", "top_products", "revenue_trend"]
    served_from_inventory = "inventory" in intent or intent in ["inventory_health", "reorder_alerts"]

    if asked_for_inventory and served_from_sales and not asked_for_sales:
        return (
            "⚠️ **Data Pillar Mismatch Detected**\n\n"
            "You asked for **Inventory Value/Stock**, but I found a match for **Sales Revenue** instead. "
            "To prevent providing incorrect figures, I've halted this narration.\n\n"
            "**Did you mean:**\n"
            "1. 'What is our current stock valuation?' (Inventory Asset)\n"
            "2. 'What is our sales revenue for the period?' (Revenue Generated)\n\n"
            "Please clarify so I can pull from the correct data pillar."
        )

    # --- Standard Narration Path ---
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

    # --- Dynamic System Prompt (Targeted) ---
    if intent == "whole_business_snapshot":
        sys_prompt = (
            "You are the Executive AI of NK Proteins. Format this as a C-Suite Dashboard. "
            "Structure your answer exactly into these 4 sections with bold headings:\n"
            "1. **Revenue Dashboard** (Include 30d Total, MiM % and YoY % growth against prior periods)\n"
            "2. **Operational Profitability** (REPORT AS: 'Operational Gross Margin Estimate' vs Customer Receipts)\n"
            "3. **Inventory Health** (Dead Stock vs Low Stock alerts)\n"
            "4. **Market Insights** (Top Selling Products list. Formatting: 'PID [Name]')\n"
            "Be concise. Use bullet points. Every currency must be prefixed with ₹ (INR). ALWAYS include the Product ID if present."
        )
    elif intent == "inventory_valuation_summary":
        sys_prompt = (
            "You are an Elite Financial Controller for NK Proteins. "
            "Formally summarize the current INVENTORY ASSET POSITION. "
            "IMPORTANT: Distinguish clearly between 'Physical Asset Value' (positive stock) and "
            "'Valuation at Risk' (negative stock anomalies). "
            "Report the SKU count and the scale of anomalous (negative) entries as a data integrity warning. "
            "Be precise and professional. Start with the 'Physical Asset Value'."
        )
    elif intent == "sales_summary_30d":
        sys_prompt = (
            "You are an Elite Sales Performance Analyst for NK Proteins. "
            "Identify the current 30-DAY ROLLING SALES VOLUME. "
            "Clearly state the period start and end dates from the data. "
            "Summarize the total revenue, sales volume (units), and order count. "
            "Be energetic and performance-focused. Start with the 'Total 30-Day Volume'."
        )
    elif intent == "plant_footprint":
        sys_prompt = (
            "You are an Elite Operations Footprint Analyst for NK Proteins. "
            "Sumarize the company's distribution network. "
            "Report the total number of unique distribution sites/plants. "
            "Briefly list the cities or regions where we have active plants. "
            "Be concise and logistics-focused. Start with the 'Total Active Plants count'."
        )
    elif intent == "business_profitability_summary":
        sys_prompt = (
            "You are an Elite CFO and Financial Controller for NK Proteins. "
            "Report the TOTAL Business Gross Profit (Revenue, COGS, Margin). "
            "Specifically highlight the 'Aggregate Margin %'. "
            "Mention the period covered (from the data). "
            "DATA INTEGRITY AUDIT: If 'total_cogs' is suspiciously low (e.g. < 5% of revenue) for a broad period, mention that "
            "'Actual profit may be lower due to pending raw material cost allocations or missing COGS for service-based codes'. "
            "Format as a professional Financial Snapshot. Use ₹ symbol."
        )
    elif intent == "loss_making_summary":
        sys_prompt = (
            "You are an Elite Risk and Margin Audit Analyst for NK Proteins. "
            "Report the absolute COUNT of loss-making products (unique SKUs with negative net margin). "
            "Report the 'Total Loss Value' and the 'Affected Revenue' from the data. "
            "Specifically highlight the period coverage. "
            "If the count is 0, congratulate the team on healthy margins. "
            "If the count is > 0, use a professional but alert tone. "
            "Format as a Risk Exposure Summary. Use ₹ symbol."
        )
    else:
        sys_prompt = (
            "You are an elite Business Analyst for NK Proteins. Summarize the provided data. "
            "CRITICAL RULE 1: If you see the same region multiple times with different units (KG, EA, CS), you MUST sum their revenues "
            "to report the true 'Total Regional Revenue'. Do not ignore small entries."
            "CRITICAL RULE 2: Every currency value MUST be prefixed with ₹ (INR). The dollar ($) is strictly forbidden. "
            "CRITICAL RULE 3 (CONTEXT ISOLATION): Do NOT reuse product names, customers, or SKUs from earlier in the conversation. "
            "ONLY report names that are explicitly present in the 'Raw Data' JSON below. "
            "CRITICAL RULE 4 (SKU FORMATTING): When listing products, ALWAYS include the Product ID if available. "
            "Format as: 'PID [ProductName]'. For example: '20101 [TIRUPATI COTTON OIL]'. "
            "CRITICAL RULE 5 (BOM LOGIC): If multiple rows exist for the same Product ID in a Bill of Material (BOM) result, "
            "it represents the individual components required for that specific finished good. List them clearly as components. "
            "Write 2-3 concise sentences. Start with the Grand Total across all regions."
        )
    
    # Pre-calculate totals for the 'Accuracy Auditor' (Internal validation)
    regional_totals = {}
    total_for_audit = 0
    
    if intent != "whole_business_snapshot":
        for r in rows:
            reg = r.get("region", "Global")
            rev = _extract_revenue(r)
            regional_totals[reg] = regional_totals.get(reg, 0) + rev
        total_for_audit = sum(regional_totals.values())
        
        user_prompt = (
            f"Context: {intent} comparison.\n"
            f"Reference Totals (Verify your math against these sums):\n"
            f"- Grand Total: ₹{total_for_audit:,.2f}\n"
            + "\n".join([f"- {k} Total: ₹{v:,.2f}" for k, v in regional_totals.items()]) + "\n"
            f"\nRaw Data (rows: {row_count}):\n"
            f"```json\n{json.dumps(rows[:15], indent=2)}\n```\n"
            "Executive Summary:"
        )
    else:
        # Dashboard context
        d = rows[0]
        total_for_audit = d.get("revenue_annual", 0)
        
        # Pre-calculate Growth % for the LLM
        rev_30d = d.get('revenue_30d') or 0
        rev_prev_30d = d.get('revenue_prev_30d') or 0
        rev_prev_year = d.get('revenue_prev_year_30d') or 0
        
        mom_growth = ((rev_30d - rev_prev_30d) / rev_prev_30d * 100) if rev_prev_30d else 0
        yoy_growth = ((rev_30d - rev_prev_year) / rev_prev_year * 100) if rev_prev_year else 0

        user_prompt = (
            f"Context: Executive Dashboard Snapshot.\n"
            f"Raw KPI Data:\n"
            f"```json\n{json.dumps(d, indent=2)}\n```\n"
            f"Calculated Growth:\n"
            f"- MoM: {mom_growth:+.1f}%\n"
            f"- YoY: {yoy_growth:+.1f}%\n"
            "Please provide the 4-section dashboard summary:"
        )
    
    logger.info("Triggering Intelligent Narration (Phase 2C) for intent: %s", intent)
    try:
        response = call_granite(
            user_prompt=user_prompt,
            system_prompt=sys_prompt,
            max_tokens=400,
            temperature=0.0, # Forced determinant
            is_json=False
        )
        
        if response.get("status") == "success":
            llm_text = response.get("text", "").strip()
            # Accuracy Auditor: Ensure the primary total is mentioned correctly
            audit_pass = True
            if total_for_audit > 1.0:
                # Get the first 4 significant digits (e.g. 122.3 from 122315136)
                clean_target = str(int(total_for_audit))[:4]
                clean_text = llm_text.replace(",", "").replace(".", "")
                if clean_target not in clean_text:
                    logger.warning("Accuracy Auditor: Expected number segment '%s' not found in summary. Potential math error.", clean_target)
            
            return correction_note + llm_text
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
        total_rev = sum(_extract_revenue(r) for r in rows)
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
