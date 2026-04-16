import os

# =============================================================================
# 1. SCHEMA & MODEL CARDS (Structural — does not change with data)
# =============================================================================

SQL_SCHEMA = """
TABLE: fact_sales
Columns: invoice_no, BillingDocumentItem, event_date, product_id, product_name, region, quantity, returns_qty, billing_quantity_unit, discount_pct, revenue, transaction_currency

TABLE: fact_inventory
Columns: product_id, Plant, StorageLocation, plant_name, storage_location_name, event_date, product_name, current_stock, base_uom, total_sales_30d, revenue, lead_time_days, unit_cost, dead_stock_flag

TABLE: fact_profitability
Columns: product_id, event_date, product_name, revenue, cogs, gross_margin, transaction_currency

TABLE: fact_cashflow
Columns: CompanyCode, AccountingDocument, FiscalYear, AccountingDocumentItem, Customer, CustomerName, CompanyCodeName, GLAccount, GLAccountLongName, PostingDate, DocumentDate, NetDueDate, ClearingDate, CompanyCodeCurrency, Amount, ReceivableStatus, CashFlowType, AgingBucket, ActualCash, ForecastCash
"""

ML_MODEL_CARDS = """
1. SALES FORECASTING (Prophet + XGBoost)
   - Needs: event_date, revenue, product_id, product_name, region
   - Table: fact_sales
"""

# Pillar → date column mapping (Unified Standard)
PILLAR_DATE_COL = {
    'sales': 'event_date',
    'cashflow': 'PostingDate',
    'profitability': 'event_date',
    'inventory': 'event_date',
}

# Queries for full-table snapshots (used by engine/validators)
FULL_TABLE_QUERIES = {
    'sales': 'SELECT * FROM fact_sales',
    'inventory': 'SELECT * FROM fact_inventory'
}

