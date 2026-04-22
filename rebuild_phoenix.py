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
    
    return create_engine(conn_str)

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
        return pd.to_datetime(date_val, dayfirst=True)
    except:
        return pd.to_datetime(date_val, errors='coerce')

# =============================================================================
# 3. SCHEMA DEFINITION & PURGE
# =============================================================================

def purge_and_create_tables(engine):
    tables = [
        "fact_sales"
        # "fact_inventory",
        # "fact_profitability",
        # "fact_cashflow",
        # "fact_bom"
    ]
    
    with engine.begin() as conn:
        for table in tables:
            logger.info(f"Dropping table if exists: {table}")
            conn.execute(text(f"IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {table}"))

        # Create fact_sales (Updated 33-column SAP Schema)
        logger.info("Creating table: fact_sales")
        conn.execute(text("""
            CREATE TABLE fact_sales (
                BillingDocument NVARCHAR(50),
                BillingDocumentItem NVARCHAR(10),
                BillingDocumentDate DATE,
                Material NVARCHAR(18),
                MaterialGroup NVARCHAR(50),
                MaterialGroupName NVARCHAR(100),
                MaterialType NVARCHAR(50),
                MaterialTypeName NVARCHAR(100),
                ProductName NVARCHAR(255),
                SalesOrganization NVARCHAR(50),
                SalesOrganizationName NVARCHAR(100),
                DistributionChannel NVARCHAR(50),
                DistributionChannelName NVARCHAR(100),
                Division NVARCHAR(50),
                DivisionName NVARCHAR(100),
                SalesOffice NVARCHAR(50),
                SalesOfficeName NVARCHAR(100),
                Plant NVARCHAR(50),
                PlantName NVARCHAR(100),
                PlantCityName NVARCHAR(100),
                PlantStreetName NVARCHAR(255),
                PlantPostalCode NVARCHAR(20),
                SoldToParty NVARCHAR(50),
                CustomerName NVARCHAR(255),
                CustomerRegionName NVARCHAR(100),
                ProfitCenter NVARCHAR(50),
                ProfitCenterName NVARCHAR(100),
                BillingQuantity DECIMAL(18,3),
                BillingQuantityUnit NVARCHAR(20),
                TransactionCurrency NVARCHAR(10),
                NetAmount DECIMAL(18,2),
                CostAmount DECIMAL(18,2),
                GrossMargin DECIMAL(18,2)
            )
        """))

        # [SALES ONLY] Skipping Inventory, Profitability, Cashflow, and BOM creations.
        """
        # Create fact_inventory
        logger.info("Creating table: fact_inventory")
        conn.execute(text(\"\"\"
            CREATE TABLE fact_inventory (
                ...
            )
        \"\"\"))
        ...
        """

# =============================================================================
# 4. DATA LOADING
# =============================================================================

def load_data(engine):
    logger.info("Starting data ingestion with USN normalization...")

    # 1. SALES (New Integrated Profitability & Sales Data)
    f_sales = "data/NK_Proteins_Sales_Profitability_Data_20250201_to_20250228_v1.csv"
    if os.path.exists(f_sales):
        logger.info(f"Loading Sales (Integrated) from {f_sales}")
        df = pd.read_csv(f_sales)
        
        # Date Parsing
        df['BillingDocumentDate'] = df['BillingDocumentDate'].apply(parse_date)
        
        # Pad Material/Customer IDs
        df['Material'] = df['Material'].apply(normalize_product_id)
        df['SoldToParty'] = df['SoldToParty'].apply(lambda x: str(x).zfill(10) if pd.notna(x) else x)
        
        # Clean numeric columns (Pricing Truth: NetAmount used directly)
        for col in ['BillingQuantity', 'NetAmount', 'CostAmount', 'GrossMargin']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Currency Normalization (USD to INR @ 93)
        if 'TransactionCurrency' in df.columns:
            logger.info("Normalizing Currency: Converting USD to INR (Rate: 93)")
            mask_usd = df['TransactionCurrency'].str.upper() == 'USD'
            for col in ['NetAmount', 'CostAmount', 'GrossMargin']:
                if col in df.columns:
                    df.loc[mask_usd, col] = df.loc[mask_usd, col] * 93
            
            # Standardize label to INR for unified reporting
            df['TransactionCurrency'] = 'INR'
        
        # Bulk insert
        df.to_sql("fact_sales", engine, if_exists='append', index=False, chunksize=1000)
    
    # [SALES ONLY MODE] Skipping Inventory, Profitability, Cashflow, and BOM as requested.

if __name__ == "__main__":
    try:
        engine = get_engine()
        logger.info("Project Phoenix: Initiating Clean-Slate Rebuild...")
        
        purge_and_create_tables(engine)
        load_data(engine)
        
        logger.info("=" * 60)
        logger.info("REBUILD COMPLETE: Universal Semantic Schema Operational.")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"REBUILD FAILED: {str(e)}", exc_info=True)
