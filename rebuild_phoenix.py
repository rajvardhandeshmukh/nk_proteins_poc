import os
import pandas as pd
import logging
import re
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("PhoenixRebuild")

load_dotenv()

# =============================================================================
# 1. DB CONNECTIVITY
# =============================================================================

def get_engine():
    server = os.getenv("MSSQL_SERVER", "localhost")
    database = os.getenv("MSSQL_DATABASE", "nk_proteins")
    user = os.getenv("MSSQL_USER", "sa")
    password = os.getenv("MSSQL_PASS")
    
    if not password:
        raise ValueError("MSSQL_PASS not found in .env")
        
    driver = "ODBC Driver 17 for SQL Server"
    params = quote_plus(f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={user};PWD={password}")
    conn_str = f"mssql+pyodbc:///?odbc_connect={params}"
    
    return create_engine(conn_str, fast_executemany=True)

# =============================================================================
# 2. NORMALIZATION UTILS
# =============================================================================

def normalize_product_id(pid):
    if pd.isna(pid) or pid == '':
        return None
    s = str(pid).split('.')[0]
    return s.zfill(18)

def normalize_name(name):
    if pd.isna(name) or name == '':
        return 'UNKNOWN'
    name = re.sub(r'\s+', ' ', str(name)).strip().upper()
    return name

def parse_date(date_val):
    if pd.isna(date_val) or date_val == '':
        return None
    try:
        # Handle YYYYMMDD format from SAP
        s = str(date_val).strip()
        if len(s) == 8 and s.isdigit():
            return pd.to_datetime(s, format='%Y%m%d')
        return pd.to_datetime(date_val, dayfirst=True)
    except:
        return pd.to_datetime(date_val, errors='coerce')

# =============================================================================
# 3. DATA LOADING
# =============================================================================

def load_data(engine):
    logger.info("Starting data ingestion with USN normalization...")

    # 1. SALES
    f_sales = "data/NK_Proteins_Sales_Data_20250201_to_20250215_v2_modified (2).csv"
    if os.path.exists(f_sales):
        logger.info(f"Loading Sales from {f_sales}")
        df = pd.read_csv(f_sales, low_memory=False)
        df.columns = [c.strip() for c in df.columns]
        
        # Date Parsing
        df['BillingDocumentDate'] = df['BillingDocumentDate'].apply(parse_date)
        
        # Pad Material/Customer IDs
        if 'Material' in df.columns:
            df['Material'] = df['Material'].apply(normalize_product_id)
        if 'SoldToParty' in df.columns:
            df['SoldToParty'] = df['SoldToParty'].apply(lambda x: str(x).zfill(10) if pd.notna(x) else x)
        
        # Handle Sales Office Nulls (v2.1)
        if 'SalesOffice' in df.columns:
            df['SalesOffice'] = df['SalesOffice'].fillna('N/A')
        if 'SalesOfficeText' in df.columns:
            df['SalesOfficeText'] = df['SalesOfficeText'].fillna('N/A')

        # Clean numeric columns
        numeric_cols = [
            'BillingQuantity', 'NetAmount', 'NetAmountINR', 
            'CostAmountINR', 'GrossMarginPercentageINR'
        ]
        for col in numeric_cols:
            if col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].astype(str).str.replace(",", "", regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Bulk insert (REPLACE mode to align with CSV schema automatically)
        logger.info("Dropping and Recreating table: fact_sales")
        df.to_sql("fact_sales", engine, if_exists='replace', index=False, chunksize=500)
        
        # 2. Create View (The Semantic Layer)
        logger.info("Creating view: sales_clean")
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE OR ALTER VIEW sales_clean AS
                SELECT
                    BillingDocument          AS bill_doc,
                    BillingDocumentItem      AS bill_doc_item,
                    BillingDocumentDate      AS event_date,
                    Material                 AS product_id,
                    ProductName              AS product_name,
                    MaterialGroup            AS material_group,
                    SoldToParty              AS customer_id,
                    CustomerName             AS customer_name,
                    CustomerRegionName       AS region,
                    BillingQuantity          AS quantity,
                    BillingQuantityUnit      AS unit,
                    NetAmountINR             AS revenue,
                    CostAmountINR            AS cost,
                    GrossMarginPercentageINR AS margin_pct,
                    SalesOrganization        AS sales_org_code,
                    SalesOrganizationText    AS sales_org,
                    SalesOffice              AS sales_office_code,
                    SalesOfficeText          AS sales_office,
                    DistributionChannel      AS channel_code,
                    DistributionChannelText  AS channel,
                    Divison                  AS division_code, -- Mapping CSV typo
                    DivisionText             AS division,
                    PlantName                AS plant,
                    PlantCityName            AS plant_city,
                    BillingDocumentType      AS bill_doc_type,
                    BillingDocumentTypeText  AS bill_doc_type_text
                FROM fact_sales;
            """))
        logger.info("View sales_clean created successfully.")
    else:
        logger.error(f"Sales file NOT FOUND: {f_sales}")

if __name__ == "__main__":
    try:
        engine = get_engine()
        logger.info("Project Phoenix: Initiating Clean-Slate Rebuild...")
        
        # We use if_exists='replace' in load_data, so no separate purge needed for fact_sales
        load_data(engine)
        
        logger.info("=" * 60)
        logger.info("REBUILD COMPLETE: Universal Semantic Schema Operational.")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"REBUILD FAILED: {str(e)}", exc_info=True)
