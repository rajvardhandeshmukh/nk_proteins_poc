import pandas as pd
import os
import time
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def ingest_sales_data(file_path):
    print(f"--- Starting Ingestion: {file_path} ---")
    
    # 1. Connection setup
    server = os.getenv("MSSQL_SERVER")
    database = os.getenv("MSSQL_DATABASE")
    username = os.getenv("MSSQL_USER")
    password = os.getenv("MSSQL_PASS")
    port = os.getenv("MSSQL_PORT", "1433")
    
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}"
    params = quote_plus(conn_str)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    
    # 2. Read CSV
    print("Reading CSV...")
    df = pd.read_csv(file_path)
    
    # Clean headers (strip spaces/quotes)
    df.columns = [c.strip().replace('"', '') for c in df.columns]
    print(f"Headers found: {list(df.columns)}")
    
    # 3. Data Cleaning
    print("Cleaning data types...")
    # Convert Billing Date to datetime
    if 'Billing Date' in df.columns:
        df['Billing Date'] = pd.to_datetime(df['Billing Date'], dayfirst=True, errors='coerce')
    
    # Convert numeric columns (Gross Value, Bill Qty, etc.)
    numeric_cols = ['Gross Value', 'Bill Qty', 'Price Per Unit', 'CostAmount', 'GrossMargin']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    # 4. Push to Database
    table_name = os.getenv("TABLE_SALES", "fact_sales")
    print(f"Pushing {len(df)} rows to table '{table_name}' (Replace Mode)...")
    
    start_time = time.time()
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=1000)
        print(f"SUCCESS! Data ingested in {round(time.time() - start_time, 2)}s")
        
        # Verify with a quick count
        with engine.connect() as conn:
            res = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            print(f"Verification: Total rows in DB: {res}")
            
    except Exception as e:
        print(f"FAILED: {e}")

if __name__ == "__main__":
    csv_file = "data/Sales_01022025_To_02022025.csv"
    if os.path.exists(csv_file):
        ingest_sales_data(csv_file)
    else:
        print(f"Error: File not found at {csv_file}")
