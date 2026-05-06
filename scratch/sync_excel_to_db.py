import os
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv

# 1. Load Environment Config
load_dotenv()

def sync_data(file_path, table_name):
    print(f"🚀 Starting Sync: {file_path} -> {table_name}")
    
    # DB Credentials from .env
    server = os.getenv("MSSQL_SERVER", "localhost")
    database = os.getenv("MSSQL_DATABASE", "nk_proteins")
    user = os.getenv("MSSQL_USER", "sa")
    password = os.getenv("MSSQL_PASS")
    port = os.getenv("MSSQL_PORT", "1433")

    if not password:
        print("❌ Error: MSSQL_PASS not found in .env")
        return

    # 2. Read Excel/CSV
    print("📖 Reading file...")
    if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
        df = pd.read_excel(file_path)
    else:
        df = pd.read_csv(file_path)

    # 3. Clean Data (Handle common ERP export issues)
    print("🧹 Cleaning data...")
    # Strip spaces from column names
    df.columns = [c.strip() for c in df.columns]
    
    # Try to convert date columns automatically
    for col in df.columns:
        if 'date' in col.lower() or 'billing' in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Convert numeric columns (handles strings like '1,000.00')
        if df[col].dtype == 'object':
            try:
                # Remove commas and try to convert to numeric
                temp = df[col].str.replace(',', '', regex=False)
                df[col] = pd.to_numeric(temp)
                print(f"   - Converted {col} to numeric")
            except:
                pass

    # 4. Connect and Upload
    print("🔗 Connecting to MS SQL Server...")
    encoded_pass = quote_plus(password)
    conn_str = f"mssql+pyodbc://{user}:{encoded_pass}@{server}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(conn_str)

    try:
        # 'replace' drops the table and recreates it with the new Excel columns
        print(f"📤 Uploading {len(df)} rows to [{table_name}]...")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print("✅ Sync Complete!")
        
        # Verification check
        with engine.connect() as conn:
            count = conn.execute(f"SELECT COUNT(*) FROM [{table_name}]").scalar()
            print(f"📊 Verified: {count} rows now in database.")
            
    except Exception as e:
        print(f"❌ Error during upload: {e}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    # CHANGE THESE TWO LINES
    FILE_TO_UPLOAD = "data/new_sales_data.xlsx" 
    TARGET_TABLE = os.getenv("TABLE_SALES", "fact_sales")
    
    if os.path.exists(FILE_TO_UPLOAD):
        sync_data(FILE_TO_UPLOAD, TARGET_TABLE)
    else:
        print(f"❌ File not found: {FILE_TO_UPLOAD}")
        print("Please place your file in the 'data' folder or update the FILE_TO_UPLOAD path.")
