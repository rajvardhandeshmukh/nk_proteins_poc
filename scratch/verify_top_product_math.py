import pandas as pd
import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()

def check_math():
    try:
        conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={os.getenv("MSSQL_SERVER")};DATABASE={os.getenv("MSSQL_DATABASE")};UID={os.getenv("MSSQL_USER")};PWD={os.getenv("MSSQL_PASS")}')
        
        # 1. Get Top 1 from DB
        db_query = """
        SELECT TOP 1 product_id, product_name, SUM(revenue) as total_revenue
        FROM fact_sales
        GROUP BY product_id, product_name
        ORDER BY total_revenue DESC
        """
        db_top = pd.read_sql(db_query, conn)
        print("DB Top Product:")
        print(db_top)
        
        if db_top.empty:
            print("No data in fact_sales")
            return

        target_id = db_top.iloc[0]['product_id']
        target_name = db_top.iloc[0]['product_name']
        db_sum = db_top.iloc[0]['total_revenue']

        # 2. Check source data
        # Correct filename from research
        csv_path = 'data/NK_Proteins_Sales_Forecast_Data_20250115_to_20250315_v2 (1).csv'
        if os.path.exists(csv_path):
            df_csv = pd.read_csv(csv_path, dtype={'product_id': str, 'revenue': float})
            # Check how many rows match
            # Note: We need to account for normalization in our comparison
            # In rebuild_phoenix.py: df['product_id'] = df['product_id'].str.zfill(18)
            df_csv['product_id_norm'] = df_csv['product_id'].str.zfill(18)
            
            csv_match = df_csv[df_csv['product_id_norm'] == target_id]
            csv_sum = csv_match['revenue'].sum()
            
            print(f"\nComparing for Product {target_id} ({target_name}):")
            print(f"DB Sum:  {db_sum:,.2f}")
            print(f"CSV Sum: {csv_sum:,.2f}")
            print(f"Diff:    {db_sum - csv_sum:,.2f}")
            
            if abs(db_sum - csv_sum) > 0.01:
                print("\nMISMATCH DETECTED!")
                print("Checking for duplicate rows in DB...")
                dup_check = pd.read_sql(f"SELECT COUNT(*) as cnt FROM fact_sales WHERE product_id = '{target_id}'", conn)
                print(f"DB Row Count: {dup_check.iloc[0]['cnt']}")
                print(f"CSV Row Count: {len(csv_match)}")
            else:
                print("\nSums match perfectly!")
        else:
            print(f"CSV not found at {csv_path}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_math()
