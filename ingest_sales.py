"""
ingest_sales.py
--------------
Ingests the new NK Proteins Sales CSV into the SQL Server `fact_sales` table.
Schema v2: BillingDocument-based columns (Feb 2025 onward).
"""

import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

# ── Column definitions for v2 schema ───────────────────────────────────────
# Columns that should be parsed as dates
DATE_COLS = ["BillingDocumentDate"]

# Columns that should be numeric
NUMERIC_COLS = [
    "BillingQuantity",
    "NetAmount",
    "NetAmountINR",
    "NetAmountPlusTax",
    "NetAmountPlusTaxINR",
    "CostAmountINR",
    "GrossMarginPercentage",
    "GrossMarginPercentageINR",
]

# Expected columns (for validation)
EXPECTED_COLS = [
    "BillingDocument", "BillingDocumentItem", "BillingDocumentType",
    "BillingDocumentTypeText", "BillingDocumentDate", "Material",
    "MaterialGroup", "MaterialTypeText", "MaterialType", "ProductName",
    "SalesOrganization", "SalesOrganizationText", "DistributionChannel",
    "DistributionChannelText", "Divison", "DivisionText", "SalesOffice",
    "SalesOfficeText", "Plant", "PlantName", "PlantCityName",
    "PlantStreetName", "PlantPostalCode", "SoldToParty", "CustomerName",
    "CustomerRegion", "CustomerRegionCountryCode", "CustomerRegionName",
    "BillingQuantity", "BillingQuantityUnit", "TransactionCurrency",
    "NetAmount", "NetAmountINR", "NetAmountPlusTax", "NetAmountPlusTaxINR",
    "CostAmountINR", "GrossMarginPercentage", "GrossMarginPercentageINR",
]


def get_engine():
    server   = os.getenv("MSSQL_SERVER", "localhost")
    database = os.getenv("MSSQL_DATABASE", "nk_proteins")
    user     = os.getenv("MSSQL_USER", "sa")
    password = os.getenv("MSSQL_PASS")
    port     = os.getenv("MSSQL_PORT", "1433")

    if not password:
        raise ValueError("❌ MSSQL_PASS not set in .env")

    encoded = quote_plus(password)
    conn_str = (
        f"mssql+pyodbc://{user}:{encoded}@{server}:{port}/{database}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
    )
    return create_engine(conn_str, fast_executemany=True)


def ingest_sales(file_path: str):
    table = os.getenv("TABLE_SALES", "fact_sales")
    print(f"\n{'='*55}")
    print(f"  NK Proteins -- Sales Ingestion (v2 Schema)")
    print(f"{'='*55}")
    print(f"  File  : {file_path}")
    print(f"  Table : {table}")
    print(f"{'='*55}\n")

    # -- 1. Read -------------------------------------------------------
    print("[1/5] Reading CSV...")
    df = pd.read_csv(file_path, low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    print(f"   {len(df):,} rows | {len(df.columns)} columns found")

    # -- 2. Validate columns -----------------------------------------------
    missing = [c for c in EXPECTED_COLS if c not in df.columns]
    if missing:
        print(f"[WARN] Missing columns (will be skipped): {missing}")
    extra = [c for c in df.columns if c not in EXPECTED_COLS]
    if extra:
        print(f"[INFO] Extra columns (will be kept as-is): {extra}")

    # -- 3. Type coercions ------------------------------------------------
    print("[2/5] Cleaning data types...")
    for col in DATE_COLS:
        if col in df.columns:
            # SAP exports dates as YYYYMMDD integers (e.g. 20250204)
            df[col] = pd.to_datetime(df[col].astype(str), format="%Y%m%d", errors="coerce")
            print(f"   [OK] {col} -> datetime (YYYYMMDD)")

    for col in NUMERIC_COLS:
        if col in df.columns:
            # Strip commas (e.g. "1,23,456.78" -> "123456.78")
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.replace(",", "", regex=False)
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            print(f"   [OK] {col} -> numeric")

    # -- 4. Push to DB ---------------------------------------------------
    print(f"\n[3/5] Connecting to SQL Server...")
    engine = get_engine()

    print(f"[4/5] Uploading {len(df):,} rows -> [{table}] (REPLACE mode)...")
    t0 = time.time()
    df.to_sql(table, engine, if_exists="replace", index=False, chunksize=500)
    elapsed = round(time.time() - t0, 2)
    print(f"[OK] Upload complete in {elapsed}s")

    # -- 5. Verify -------------------------------------------------------
    print("[5/5] Verifying...")
    with engine.connect() as conn:
        count = conn.execute(text(f"SELECT COUNT(*) FROM [{table}]")).scalar()
        sample_date = conn.execute(
            text(f"SELECT MIN(BillingDocumentDate), MAX(BillingDocumentDate) FROM [{table}]")
        ).fetchone()

    print(f"\n{'='*55}")
    print(f"  DONE! Rows in DB : {count:,}")
    print(f"  Date range       : {sample_date[0]} -> {sample_date[1]}")
    print(f"{'='*55}\n")
    engine.dispose()


# ── View DDL ──────────────────────────────────────────────────────────────
# Uses CREATE OR ALTER VIEW — safe to re-run on every ingestion.
# FINAL DATA CONTRACT (v2, Feb 2025)
# Ignored: NetAmountPlusTax, GrossMarginPercentage (non-INR), TransactionCurrency
SALES_VIEW_DDL = """
CREATE OR ALTER VIEW sales_clean AS
SELECT
    -- Keys (grain = bill_doc + bill_doc_item)
    BillingDocument          AS bill_doc,
    BillingDocumentItem      AS bill_doc_item,
    -- Time
    BillingDocumentDate      AS event_date,
    -- Product
    Material                 AS product_id,
    ProductName              AS product_name,
    MaterialGroup            AS material_group,
    -- Customer
    SoldToParty              AS customer_id,
    CustomerName             AS customer_name,
    CustomerRegionName       AS region,
    -- Quantity (NEVER mix units -- always GROUP BY unit)
    BillingQuantity          AS quantity,
    BillingQuantityUnit      AS unit,
    -- Financials (LOCKED -- always INR, no negatives)
    NetAmountINR             AS revenue,
    CostAmountINR            AS cost,
    GrossMarginPercentageINR AS margin_pct,
    -- Sales structure
    SalesOrganization        AS sales_org_code,
    SalesOrganizationText    AS sales_org,
    DistributionChannel      AS channel_code,
    DistributionChannelText  AS channel,
    Divison                  AS division_code,
    DivisionText             AS division,
    -- Plant
    PlantName                AS plant,
    PlantCityName            AS plant_city
FROM fact_sales;
"""


def create_sales_view(engine):
    print("[6/6] Creating view  [sales_clean]  on top of [fact_sales]...")
    with engine.begin() as conn:
        conn.execute(text(SALES_VIEW_DDL))
    print("      [OK] VIEW sales_clean is live.")

    # Quick sanity check
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT COUNT(*), MIN(event_date), MAX(event_date) FROM sales_clean")
        ).fetchone()
    print(f"      Rows : {row[0]:,}  |  Date range : {row[1]} -> {row[2]}\n")


if __name__ == "__main__":
    sales_file = "data/NK_Proteins_Sales_Data_20250201_to_20250215_v2_modified (2).csv"
    if not os.path.exists(sales_file):
        print(f"File not found: {sales_file}")
    else:
        ingest_sales(sales_file)
        eng = get_engine()
        create_sales_view(eng)
        eng.dispose()
