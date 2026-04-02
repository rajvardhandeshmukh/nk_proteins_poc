import pandas as pd
import sqlite3
import os

# --- CONFIGURATION -------------------------------------------------------
DB_NAME = "nk_protein.db"
DATA_DIR = "data"

# Mapping: Table Name -> CSV Filename
PILLARS = {
    "fact_sales":       "nk_sales_data_2022_2026_feb.csv",
    "fact_receivables": "nk_receivables_2022_2026_feb.csv",
    "fact_gst":         "nk_gst_data_2022_2026_feb.csv",
    "fact_inventory":   "nk_inventory_2022_2026_feb.csv"
}

# Date columns per table for proper SQLite typing
DATE_COLS = {
    "fact_sales":       ['date'],
    "fact_receivables": ['invoice_date', 'due_date', 'received_date'],
    "fact_gst":         [], # GST periods are strings like '2024-01'
    "fact_inventory":   []
}

def setup_database():
    """Converts all CSVs into indexed SQLite tables."""
    print(f"--- INITIALIZING DATABASE: {DB_NAME} ---")
    
    # Establish connection
    conn = sqlite3.connect(DB_NAME)
    
    for table_name, csv_file in PILLARS.items():
        csv_path = os.path.join(DATA_DIR, csv_file)
        
        if not os.path.exists(csv_path):
            print(f" [!] Skipping {table_name}: CSV not found at {csv_path}")
            continue
            
        print(f" [>] Migrating {csv_file} to table '{table_name}'...")
        
        # Load CSV with date parsing
        df = pd.read_csv(csv_path, parse_dates=DATE_COLS.get(table_name, []))
        
        # Write to SQLite
        # If it exists, we replace it (clean sweep for POC)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        print(f"     Success: {len(df)} rows imported.")
        
    # --- ADDING INDEXES FOR SPEED (i3 Optimization) ---
    print(" [>] Building performance indexes...")
    cursor = conn.cursor()
    
    # Sales index
    try:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sales_date ON fact_sales(date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sales_region ON fact_sales(region)")
        
        # Receivables index
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_receivables_cust ON fact_receivables(customer_id)")
        
        # Inventory index
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inventory_sku ON fact_inventory(sku)")
    except Exception as e:
        print(f" [!] Indexing error: {e}")
    
    conn.commit()
    conn.close()
    print(f"\n--- DATABASE READY: {DB_NAME} ---")
    print(" You can now run SQL queries against this file securely.\n")

if __name__ == "__main__":
    setup_database()
