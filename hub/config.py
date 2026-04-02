import os

# =============================================================================
# 1. SCHEMA & MODEL CARDS (Structural — does not change with data)
# =============================================================================

SQL_SCHEMA = """
TABLE: fact_sales
Columns: date (datetime), product_id (str), product_name (str), customer_id (str), customer_name (str), region (str), quantity_sold (int), unit_price (float), discount_pct (float), net_price (float), revenue (float), cogs (float), gross_margin (float), margin_pct (float), returns_qty (int), is_festive (int), quarter (str), month (int), year (int)

TABLE: fact_receivables
Columns: invoice_no (str), invoice_date (datetime), customer_id (str), customer_name (str), region (str), customer_type (str), payment_terms_days (int), invoice_amount (float), due_date (datetime), amount_received (float), received_date (datetime), outstanding_amount (float), days_overdue (int), aging_bucket (str), slow_paying_flag (int), collection_risk_score (float)

TABLE: fact_gst
Columns: return_period (str), doc_type (str), invoice_no (str), invoice_date (datetime), counterparty_id (str), counterparty_name (str), gstin (str), taxable_value (float), cgst_amount (float), sgst_amount (float), igst_amount (float), total_tax_amount (float), gstr2b_status (str), mismatch_flag (int), mismatch_reason (str)

TABLE: fact_inventory
Columns: snapshot_date (datetime), sku (str), product_name (str), category (str), warehouse (str), current_stock_kg (float), avg_daily_sales (float), days_no_movement (int), last_sale_date (datetime), lead_time_days (int), safety_stock (float), reorder_point (float), ideal_stock (float), needs_reorder (int), unit_cost_inr (float), total_value_inr (float), monthly_holding_cost (float), aging_bucket (str), is_dead_stock (int), health_score (float), supplier (str)
"""

ML_MODEL_CARDS = """
DOWNSTREAM ML MODELS (Your SQL feeds these — understand their data needs):

1. SALES FORECASTING (Prophet + XGBoost)
   - Needs columns: date, revenue, product_id, region, is_festive, discount_pct, returns_qty
   - Minimum: 200+ rows spanning 24+ months for reliable seasonal detection
   - Table: fact_sales

2. ANOMALY DETECTION (Z-Score on sales, Isolation Forest on GST)
   - Needs: Full row-level numerical data
   - Minimum: 100+ rows to detect meaningful statistical outliers
   - Tables: fact_sales, fact_gst

3. CASH FLOW RISK (Probabilistic Aging Model)
   - Needs: invoice_amount, days_overdue, aging_bucket, received_date
   - Minimum: 50+ invoices for valid collection probability
   - Table: fact_receivables

4. CUSTOMER SEGMENTATION (KMeans Clustering)
   - Needs: revenue, margin_pct, discount_pct per customer
   - Minimum: 30+ unique customers for meaningful clusters
   - Table: fact_sales

WARNING: If your SQL has narrow WHERE filters, the downstream models may receive
insufficient data and produce unreliable results. When in doubt, pull BROADER data.
The ML engine handles the filtering internally.
"""

# The base queries for full-table pulls needed by ML models.
FULL_TABLE_QUERIES = {
    'sales': """
        SELECT date, product_id, product_name, region, quantity_sold, revenue, margin_pct, discount_pct, returns_qty, is_festive 
        FROM fact_sales 
        WHERE date >= DATEADD(month, -36, GETDATE())
        ORDER BY date ASC
    """,
    'cashflow': """
        SELECT invoice_no, invoice_date, due_date, received_date, customer_id, customer_name, region, invoice_amount, days_overdue, aging_bucket 
        FROM fact_receivables 
        WHERE invoice_date >= DATEADD(month, -36, GETDATE())
    """,
    'gst': """
        SELECT invoice_no, return_period, mismatch_flag, mismatch_reason, taxable_value, cgst_amount, sgst_amount, total_tax_amount, counterparty_id, counterparty_name 
        FROM fact_gst 
        WHERE invoice_date >= DATEADD(month, -36, GETDATE())
    """,
    'inventory': """
        SELECT snapshot_date, sku, category, warehouse, total_value_inr, current_stock_kg, days_no_movement, is_dead_stock, needs_reorder, reorder_point, lead_time_days, ideal_stock 
        FROM fact_inventory 
        WHERE snapshot_date >= DATEADD(month, -36, GETDATE())
    """,
}

# Pillar → date column mapping
PILLAR_DATE_COL = {
    'sales': 'date',
    'cashflow': 'invoice_date',
    'gst': 'invoice_date',
    'inventory': 'snapshot_date',
}
