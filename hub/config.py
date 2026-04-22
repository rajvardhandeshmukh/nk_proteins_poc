import os

# =============================================================================
# 1. SCHEMA & MODEL CARDS (Structural — does not change with data)
# =============================================================================

SQL_SCHEMA = """
TABLE: fact_sales
Columns: invoice_no, BillingDocumentItem, event_date, product_id, product_name, material_group, material_group_name, material_type_code, material_type_name, client_category, region, quantity, returns_qty, billing_quantity_unit, discount_pct, revenue, transaction_currency, SalesOrganization, DistributionChannel, Division, sales_office_code, sales_office_name, plant_code, location_name, plant_city, plant_street, plant_post_code, Customer, TaxAmount, CostAmount

TABLE: fact_inventory
Columns: product_id, product_name, Plant, StorageLocation, location_name, event_date, current_stock, base_uom, total_sales_30d, revenue, lead_time_days, unit_cost, stock_status

TABLE: fact_profitability
Columns: product_id, event_date, product_name, revenue, cogs, gross_margin, transaction_currency

TABLE: fact_cashflow
Columns: CompanyCode, AccountingDocument, FiscalYear, AccountingDocumentItem, Customer, CustomerName, CompanyCodeName, GLAccount, GLAccountLongName, PostingDate, DocumentDate, NetDueDate, ClearingDate, CompanyCodeCurrency, Amount, ReceivableStatus, CashFlowType, AgingBucket, ActualCash, ForecastCash

TABLE: fact_bom
Columns: BillOfMaterialCategory, BillOfMaterial, BillOfMaterialVariant, BillOfMaterialItemNodeNumber, HeaderMaterial, Plant, PlantName, ComponentMaterial, ComponentDescription, BillOfMaterialItemNumber, BillOfMaterialItemUnit, BillOfMaterialItemQuantity, ValidityStartDate, ValidityEndDate, BOMItemRecordCreationDate
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

