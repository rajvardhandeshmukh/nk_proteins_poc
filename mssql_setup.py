import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load credentials from .env
load_dotenv()

# --- CONFIGURATION -------------------------------------------------------
SERVER   = os.getenv("MSSQL_SERVER", "localhost")
DATABASE = os.getenv("MSSQL_DATABASE", "nk_proteins")
USER     = os.getenv("MSSQL_USER")
PASS     = os.getenv("MSSQL_PASS")
PORT     = os.getenv("MSSQL_PORT", "1433")

DATA_DIR = "data"

# Mapping: Table Name -> CSV Filename
PILLARS = {
    "fact_sales":       "nk_sales_data_2022_2026_feb.csv",
    "fact_receivables": "nk_receivables_2022_2026_feb.csv",
    "fact_gst":         "nk_gst_data_2022_2026_feb.csv",
    "fact_inventory":   "nk_inventory_2022_2026_feb.csv"
}

# Date columns per table for proper SQL typing
DATE_COLS = {
    "fact_sales":       ['date'],
    "fact_receivables": ['invoice_date', 'due_date', 'received_date'],
    "fact_gst":         [],
    "fact_inventory":   []
}

def migrate_to_mssql():
    """Migrates CSV data to MS SQL Server using pymssql + sqlalchemy."""
    print(f"--- STARTING MIGRATION TO MS SQL: {DATABASE} ---")
    
    if not USER or not PASS:
        print(" [!] ERROR: MSSQL_USER or MSSQL_PASS not found in .env")
        print("     Please fill in your credentials in the .env file first.")
        return

    from urllib.parse import quote_plus
    encoded_pass = quote_plus(PASS)
    # Construct SQLAlchemy Connection String for pymssql
    # Format: mssql+pymssql://<user>:<pass>@<server>:<port>/<db>
    connection_url = f"mssql+pymssql://{USER}:{encoded_pass}@{SERVER}:{PORT}/{DATABASE}"
    
    try:
        engine = create_engine(connection_url)
        # Test connection
        with engine.connect() as conn:
            print(f" [OK] Successfully connected to {SERVER}")
            
        for table_name, csv_file in PILLARS.items():
            csv_path = os.path.join(DATA_DIR, csv_file)
            
            if not os.path.exists(csv_path):
                print(f" [!] Skipping {table_name}: CSV not found at {csv_path}")
                continue
                
            print(f" [>] Migrating {csv_file} to MS SQL table '{table_name}'...")
            
            # Load CSV
            df = pd.read_csv(csv_path, parse_dates=DATE_COLS.get(table_name, []))
            
            # Write to MS SQL
            # if_exists='replace' will drop and recreate the table
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            
            print(f"     Success: {len(df)} rows imported.")
            
        print(f"\n--- MIGRATION COMPLETE: {DATABASE} IS READY ---")
        
    except Exception as e:
        print(f"\n [!] CRITICAL ERROR: {str(e)}")
        print("     Check your .env credentials or if the SQL Server is running.")

if __name__ == "__main__":
    migrate_to_mssql()
