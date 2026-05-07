import os
from dotenv import load_dotenv

# Force reload of environment variables
load_dotenv(override=True)

class Config:
    def __init__(self):
        # Database
        self.MSSQL_SERVER   = os.getenv("MSSQL_SERVER", "localhost")
        self.MSSQL_DATABASE = os.getenv("MSSQL_DATABASE", "nk_proteins")
        self.MSSQL_USER     = os.getenv("MSSQL_USER", "sa")
        self.MSSQL_PASS     = os.getenv("MSSQL_PASS", "")
        self.MSSQL_PORT     = os.getenv("MSSQL_PORT", "1433")

        # Raw table -- ingestion layer only
        self.TABLE_SALES = os.getenv("TABLE_SALES", "fact_sales")

        # Clean view -- gateway / AI query layer
        self.VIEW_SALES = os.getenv("VIEW_SALES", "sales_clean")

        # Receivables -- next phase
        self.TABLE_RECEIVABLES = os.getenv("TABLE_RECEIVABLES", "fact_receivables")
        self.VIEW_RECEIVABLES  = os.getenv("VIEW_RECEIVABLES",  "receivables_clean")

        # Ground Truth Column Aliases (v2 -- from sales_clean view)
        # Keys
        self.COL_BILL_DOC      = "bill_doc"        # BillingDocument
        self.COL_BILL_DOC_ITEM = "bill_doc_item"   # BillingDocumentItem
        # Time
        self.COL_DATE          = "event_date"      # BillingDocumentDate
        # Product
        self.COL_MATERIAL      = "product_id"      # Material
        self.COL_PRODUCT_NAME  = "product_name"    # ProductName
        self.COL_MATERIAL_GRP  = "material_group"  # MaterialGroup
        # Customer
        self.COL_CUSTOMER_ID   = "customer_id"     # SoldToParty
        self.COL_CUSTOMER      = "customer_name"   # CustomerName
        self.COL_REGION        = "region"          # CustomerRegionName
        # Quantity
        self.COL_QUANTITY      = "quantity"        # BillingQuantity
        self.COL_UNIT          = "unit"            # BillingQuantityUnit
        # Financials (LOCKED -- always INR)
        self.COL_NET_AMOUNT    = "revenue"         # NetAmountINR
        self.COL_COST          = "cost"            # CostAmountINR
        self.COL_MARGIN_PCT    = "margin_pct"      # GrossMarginPercentageINR
        # Sales structure
        self.COL_SALES_ORG_CODE = "sales_org_code" # SalesOrganization
        self.COL_SALES_ORG      = "sales_org"       # SalesOrganizationText
        self.COL_CHANNEL_CODE   = "channel_code"    # DistributionChannel
        self.COL_CHANNEL        = "channel"         # DistributionChannelText
        self.COL_DIVISION_CODE  = "division_code"   # Divison
        self.COL_DIVISION       = "division"        # DivisionText
        # Sales Office (NEW v2.1)
        self.COL_SALES_OFFICE_CODE = "sales_office_code" # SalesOffice
        self.COL_SALES_OFFICE      = "sales_office"      # SalesOfficeText
        # Plant
        self.COL_PLANT          = "plant"           # PlantName
        self.COL_PLANT_CITY     = "plant_city"      # PlantCityName
        self.COL_BILL_DOC_TYPE  = "bill_doc_type_text" # BillingDocumentTypeText

    def get_column_map(self):
        """
        Logical key -> view column name.
        Used by validators and the SQL safety layer.
        """
        return {
            "BILL_DOC":     self.COL_BILL_DOC,
            "DATE":         self.COL_DATE,
            "PRODUCT_ID":   self.COL_MATERIAL,
            "PRODUCT_NAME": self.COL_PRODUCT_NAME,
            "CUSTOMER_ID":  self.COL_CUSTOMER_ID,
            "CUSTOMER":     self.COL_CUSTOMER,
            "REGION":       self.COL_REGION,
            "QUANTITY":     self.COL_QUANTITY,
            "UNIT":         self.COL_UNIT,
            "REVENUE":      self.COL_NET_AMOUNT,
            "COST":         self.COL_COST,
            "MARGIN_PCT":   self.COL_MARGIN_PCT,
            "SALES_ORG":    self.COL_SALES_ORG,
            "SALES_OFFICE": self.COL_SALES_OFFICE,
            "PLANT":        self.COL_PLANT,
            "PLANT_CITY":   self.COL_PLANT_CITY,
            "DOC_TYPE":     self.COL_BILL_DOC_TYPE,
        }


# Global config instance
config = Config()

# AI Schema Context -- LLM sees this when generating ad-hoc SQL.
# Always query sales_clean, never fact_sales directly.
SQL_SCHEMA = f"""
VIEW : {config.VIEW_SALES}
GRAIN: bill_doc + bill_doc_item (one row per billing line item)
DATA : MTD 2025-02-01 to 2025-02-15

COLUMNS (use EXACTLY these names in SQL):
  event_date     (datetime)  Billing date
  bill_doc       (string)    Billing document number
  bill_doc_item  (string)    Billing document line item
  product_id     (string)    Material/product code
  product_name   (string)    Product name
  material_group (string)    Material group/category
  customer_id    (string)    Customer SAP ID (SoldToParty)
  customer_name  (string)    Customer name
  region         (string)    Customer region name
  quantity       (decimal)   Billed quantity
  unit           (string)    Quantity unit (KG, LTR, CS, EA) -- NEVER mix units
  revenue        (decimal)   Net amount INR  [LOCKED: always use this]
  cost           (decimal)   Cost amount INR
  margin_pct     (decimal)   Gross margin % (GrossMarginPercentageINR)
  sales_org_code (string)    Sales organization code
  sales_org      (string)    Sales organization name
  sales_office_code (string) Sales office code
  sales_office      (string) Sales office name (e.g., Ahmedabad, Mumbai)
  channel_code   (string)    Distribution channel code
  channel        (string)    Distribution channel name
  division_code  (string)    Division code
  division       (string)    Division name
  plant          (string)    Plant name
  plant_city     (string)    Plant city
  bill_doc_type_text (string) Invoice or Return

RULES:
  R1: SUM(revenue) AS total_revenue -- always INR, never other amounts
  R2: GROUP BY unit when querying quantity -- never mix KG + LTR
  R3: GROUP BY product_name (always)
  R4: GROUP BY customer_id, customer_name (both, always)
  R5: ALWAYS use WHERE event_date >= :start_date AND event_date <= :end_date
"""
