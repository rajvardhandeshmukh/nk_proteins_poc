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
        
    # Use ODBC Driver 17 for SQL Server as verified in research
    driver = "ODBC Driver 17 for SQL Server"
    
    params = quote_plus(f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={user};PWD={password}")
    conn_str = f"mssql+pyodbc:///?odbc_connect={params}"
    
    return create_engine(conn_str)

# =============================================================================
# 2. NORMALIZATION UTILS
# =============================================================================

def normalize_product_id(pid):
    """Pads numeric IDs to 18 digits (SAP Standard)."""
    if pd.isna(pid) or pid == '':
        return None
    s = str(pid).split('.')[0] # Remove decimal if any
    return s.zfill(18)

def normalize_name(name):
    """Clean and upper-case names for consistency."""
    if pd.isna(name) or name == '':
        return 'UNKNOWN'
    # Remove extra spaces and special characters, to uppercase
    name = re.sub(r'\s+', ' ', str(name)).strip().upper()
    return name

def parse_date(date_val):
    """Try to parse SAP dates (DD.MM.YYYY) safely."""
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
        "fact_sales",
        "fact_inventory",
        "fact_profitability",
        "fact_cashflow",
        "fact_bom"
    ]
    
    with engine.begin() as conn:
        for table in tables:
            logger.info(f"Dropping table if exists: {table}")
            conn.execute(text(f"IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {table}"))

        # Create fact_sales
        logger.info("Creating table: fact_sales")
        conn.execute(text("""
            CREATE TABLE fact_sales (
                invoice_no NVARCHAR(50),
                event_date DATE,
                product_id NVARCHAR(18),
                product_name NVARCHAR(255),
                location_name NVARCHAR(255),
                region NVARCHAR(100),
                quantity DECIMAL(18,3),
                returns_qty DECIMAL(18,3),
                revenue DECIMAL(18,2),
                TaxAmount DECIMAL(18,2),
                CostAmount DECIMAL(18,2),
                billing_quantity_unit NVARCHAR(20),
                material_group_name NVARCHAR(100),
                Customer NVARCHAR(50)
            )
        """))

        # Create fact_inventory
        logger.info("Creating table: fact_inventory")
        conn.execute(text("""
            CREATE TABLE fact_inventory (
                product_id NVARCHAR(18),
                product_name NVARCHAR(255),
                Plant NVARCHAR(50),
                StorageLocation NVARCHAR(50),
                location_name NVARCHAR(255),
                event_date DATE,
                current_stock DECIMAL(18,3),
                unit_cost DECIMAL(18,2),
                total_sales_30d DECIMAL(18,3),
                lead_time_days INT,
                base_uom NVARCHAR(20)
            )
        """))

        # Create fact_profitability
        logger.info("Creating table: fact_profitability")
        conn.execute(text("""
            CREATE TABLE fact_profitability (
                product_id NVARCHAR(18),
                product_name NVARCHAR(255),
                event_date DATE,
                revenue DECIMAL(18,2),
                cogs DECIMAL(18,2),
                gross_margin DECIMAL(18,2)
            )
        """))

        # Create fact_cashflow
        logger.info("Creating table: fact_cashflow")
        conn.execute(text("""
            CREATE TABLE fact_cashflow (
                AccountingDocument NVARCHAR(50),
                Customer NVARCHAR(50),
                CustomerName NVARCHAR(255),
                PostingDate DATE,
                ClearingDate DATE,
                NetDueDate DATE,
                ActualCash DECIMAL(18,2),
                ForecastCash DECIMAL(18,2),
                Amount DECIMAL(18,2),
                AgingBucket NVARCHAR(50)
            )
        """))

        # Create fact_bom
        logger.info("Creating table: fact_bom")
        conn.execute(text("""
            CREATE TABLE fact_bom (
                HeaderMaterial NVARCHAR(18),
                ComponentMaterial NVARCHAR(18),
                ComponentDescription NVARCHAR(255),
                BillOfMaterialItemQuantity DECIMAL(18,5),
                BillOfMaterialItemUnit NVARCHAR(20),
                Plant NVARCHAR(50),
                PlantName NVARCHAR(100),
                ValidityStartDate DATE,
                ValidityEndDate DATE
            )
        """))

# =============================================================================
# 4. DATA LOADING
# =============================================================================

def load_data(engine):
    logger.info("Starting data ingestion with USN normalization...")

    # 1. SALES
    f_sales = "data/NK_Proteins_Sales_Forecast_Data_20250115_to_20250315_v2 (1).csv"
    if os.path.exists(f_sales):
        logger.info(f"Loading Sales from {f_sales}")
        df = pd.read_csv(f_sales)
        df['product_id'] = df['product_id'].apply(normalize_product_id)
        df['product_name'] = df['product_name'].apply(normalize_name)
        df['location_name'] = df['plant_city'].apply(normalize_name)
        df['event_date'] = df['event_date'].apply(parse_date)
        
        # Select target columns
        cols = ['invoice_no', 'event_date', 'product_id', 'product_name', 'location_name', 
                'region', 'quantity', 'returns_qty', 'revenue', 'TaxAmount', 'CostAmount',
                'billing_quantity_unit', 'material_group_name', 'Customer']
        df[cols].to_sql("fact_sales", engine, if_exists='append', index=False, chunksize=1000)
    
    # 2. INVENTORY
    f_inv = "data/NK_Proteins_Inventory_new_Data_v1.csv"
    if os.path.exists(f_inv):
        logger.info(f"Loading Inventory from {f_inv}")
        df = pd.read_csv(f_inv)
        df['product_id'] = df['product_id'].apply(normalize_product_id)
        df['product_name'] = df['product_name'].apply(normalize_name)
        df['location_name'] = df['storage_location_name'].apply(normalize_name)
        df['event_date'] = df['event_date'].apply(parse_date)
        
        cols = ['product_id', 'product_name', 'Plant', 'StorageLocation', 'location_name', 
                'event_date', 'current_stock', 'unit_cost', 'total_sales_30d', 
                'lead_time_days', 'base_uom']
        df[cols].to_sql("fact_inventory", engine, if_exists='append', index=False, chunksize=1000)

    # 3. PROFITABILITY
    f_prof = "data/NK_Proteins_Profitability_Data_20250115_to_20250315 (1).csv"
    if os.path.exists(f_prof):
        logger.info(f"Loading Profitability from {f_prof}")
        df = pd.read_csv(f_prof)
        df['product_id'] = df['product_id'].apply(normalize_product_id)
        df['product_name'] = df['product_name'].apply(normalize_name)
        df['event_date'] = df['event_date'].apply(parse_date)
        
        cols = ['product_id', 'product_name', 'event_date', 'revenue', 'cogs', 'gross_margin']
        df[cols].to_sql("fact_profitability", engine, if_exists='append', index=False, chunksize=1000)

    # 4. CASHFLOW
    f_cash = "data/NK_Proteins_Cashflow_Data_20250115_to_20250315_v2.csv"
    if os.path.exists(f_cash):
        logger.info(f"Loading Cashflow from {f_cash}")
        df = pd.read_csv(f_cash)
        df['PostingDate'] = df['PostingDate'].apply(parse_date)
        df['ClearingDate'] = df['ClearingDate'].apply(parse_date)
        df['NetDueDate'] = df['NetDueDate'].apply(parse_date)
        df['CustomerName'] = df['CustomerName'].apply(normalize_name)
        
        cols = ['AccountingDocument', 'Customer', 'CustomerName', 'PostingDate', 
                'ClearingDate', 'NetDueDate', 'ActualCash', 'ForecastCash', 'Amount', 'AgingBucket']
        df[cols].to_sql("fact_cashflow", engine, if_exists='append', index=False, chunksize=1000)

    # 5. BOM
    f_bom = "data/NK_Proteins_Bill_Of_Material_Data_20250115_to_20250315.csv"
    if os.path.exists(f_bom):
        logger.info(f"Loading BOM from {f_bom}")
        df = pd.read_csv(f_bom)
        df['HeaderMaterial'] = df['HeaderMaterial'].apply(normalize_product_id)
        df['ComponentMaterial'] = df['ComponentMaterial'].apply(normalize_product_id)
        df['ComponentDescription'] = df['ComponentDescription'].apply(normalize_name)
        df['ValidityStartDate'] = df['ValidityStartDate'].apply(parse_date)
        df['ValidityEndDate'] = df['ValidityEndDate'].apply(parse_date)
        
        cols = ['HeaderMaterial', 'ComponentMaterial', 'ComponentDescription', 
                'BillOfMaterialItemQuantity', 'BillOfMaterialItemUnit', 'Plant', 
                'PlantName', 'ValidityStartDate', 'ValidityEndDate']
        df[cols].to_sql("fact_bom", engine, if_exists='append', index=False, chunksize=1000)

# =============================================================================
# MAIN
# =============================================================================

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
