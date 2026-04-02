from datetime import datetime
from .config import SQL_SCHEMA, ML_MODEL_CARDS

# =============================================================================
# 1. AGGREGATION SQL SYSTEM PROMPT
# =============================================================================

def build_aggregation_sql_prompt():
    today = datetime.now().strftime('%Y-%m-%d')
    return f"""You are an expert MS SQL Developer for NK Proteins.
Translate the user's natural language question into a valid T-SQL query.

TODAY'S DATE: {today}. Use this for all relative date calculations like 'last quarter', 'this year', 'last month'.

DATABASE SCHEMA:
{SQL_SCHEMA}

{ML_MODEL_CARDS}

RULES:
1. For aggregation queries (totals, comparisons, lists), use GROUP BY, SUM, AVG, COUNT as needed.
2. NEVER DELETE, UPDATE, INSERT, or DROP tables. Only SELECT.
3. The output MUST start with SELECT and be directly executable T-SQL.
4. If no WHERE clause is present, you MUST add TOP 5000 to prevent memory blowouts.
5. You may ONLY query ONE table at a time.

EXAMPLE QUERIES:
- "Top 5 customers by revenue" → SELECT TOP 5 customer_name, SUM(revenue) AS total_revenue FROM fact_sales GROUP BY customer_name ORDER BY total_revenue DESC
- "Overdue invoices above 10 lakhs" → SELECT * FROM fact_receivables WHERE outstanding_amount > 1000000 AND days_overdue > 0
- "What is our DSO?" → SELECT AVG(DATEDIFF(day, invoice_date, received_date)) AS avg_dso FROM fact_receivables WHERE received_date IS NOT NULL
- "Compare Q1 vs Q2 sales" → SELECT quarter, SUM(revenue) AS total FROM fact_sales WHERE year = 2025 GROUP BY quarter
- "Worst margin products" → SELECT TOP 10 product_name, AVG(margin_pct) AS avg_margin FROM fact_sales GROUP BY product_name ORDER BY avg_margin ASC
- "Revenue by region by quarter" → SELECT region, quarter, SUM(revenue) AS total FROM fact_sales GROUP BY region, quarter ORDER BY region, quarter
- "Inventory holding cost" → SELECT SUM(monthly_holding_cost) AS total_holding_cost FROM fact_inventory
- "Suppliers with longest lead times" → SELECT TOP 5 supplier, AVG(lead_time_days) AS avg_lead FROM fact_inventory GROUP BY supplier ORDER BY avg_lead DESC
- "Customer-wise discount analysis" → SELECT TOP 10 customer_name, AVG(discount_pct) AS avg_disc, SUM(revenue) AS total_rev FROM fact_sales GROUP BY customer_name ORDER BY avg_disc DESC

CURRENCY: "lakhs" = multiply by 100000, "crores" = multiply by 10000000.
SORTING: "top/best" = ORDER BY DESC + TOP N. "worst/lowest" = ORDER BY ASC + TOP N.
"""

# =============================================================================
# 2. LLM INTENT & PILLAR CLASSIFICATION PROMPTS
# =============================================================================

INTENT_CLASSIFIER_PROMPT = """Classify this business question into exactly ONE category.

Categories:
- forecasting: predictions about a SPECIFIC domain (e.g. sales), future projections, "what will happen next"
- anomaly_detection: risks or unusual patterns in a SPECIFIC domain (e.g., "GST mismatches", "overdue invoices", "dead stock")
- segmentation: customer grouping, clustering, profiling, categorization, tiers
- aggregation: totals, comparisons, lists, lookups, "show me", "how much", "top N", "who", "which"
- hybrid: combines a specific entity (region/product/customer/warehouse) with trend/risk/forecast analysis
- multi_pillar: asks about OVERALL business health across MULTIPLE areas, executive summary, "biggest risks", "how is the business", "full report"

IMPORTANT DISTINCTION:
- If the question mentions risks/problems in a SPECIFIC area (cashflow, GST, inventory, sales) -> anomaly_detection 
- If the question asks about risks/health ACROSS the whole business or multiple areas -> multi_pillar
- "biggest risks in cash flow" -> anomaly_detection (single domain)
- "biggest risks across my business" -> multi_pillar (all domains)
- "give me an executive summary" -> multi_pillar
- "how have our sales trended" -> forecasting (global context)
- "show me a sales report" -> aggregation (global context)

Question: "{question}"

Reply with ONLY the category name, nothing else."""

PILLAR_CLASSIFIER_PROMPT = """Which data domain does this question primarily relate to?

Domains:
- sales: revenue, products, customers, regions, margins, discounts, returns
- cashflow: invoices, overdue, receivables, payments, DSO, slow payers
- gst: tax, GST, mismatches, ITC, CGST, SGST, IGST, compliance, counterparty
- inventory: stock, warehouses, reorder, SKU, holding cost, dead stock, suppliers

Question: "{question}"

Reply with ONLY the domain name, nothing else."""

ENTITY_EXTRACTION_PROMPT = """Extract the specific business entity being filtered from this question.
If the question is general (e.g., "how are sales trended?", "how is the business?"), return "NONE".
Return ONLY the entity name or "NONE", nothing else.

Question: "{question}"

Examples:
"How is the North doing?" → North
"Is Cottonseed declining?" → Cottonseed
"How have our sales trended?" → NONE
"Market performance since 2022" → NONE
"FreshMart Vadodara performance" → FreshMart Vadodara"""
