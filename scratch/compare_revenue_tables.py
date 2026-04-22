import pandas as pd
import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()

def compare_tables():
    try:
        conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={os.getenv("MSSQL_SERVER")};DATABASE={os.getenv("MSSQL_DATABASE")};UID={os.getenv("MSSQL_USER")};PWD={os.getenv("MSSQL_PASS")}')
        
        target_id = '000000000000020101'
        
        # 1. Revenue from fact_sales
        sales_rev = pd.read_sql(f"SELECT SUM(revenue) as rev FROM fact_sales WHERE product_id = '{target_id}'", conn).iloc[0]['rev']
        
        # 2. Revenue from fact_profitability
        prof_rev = pd.read_sql(f"SELECT SUM(revenue) as rev FROM fact_profitability WHERE product_id = '{target_id}'", conn).iloc[0]['rev']
        
        print(f"Product: {target_id}")
        print(f"Sales Table Revenue:         {sales_rev:,.2f}")
        print(f"Profitability Table Revenue: {prof_rev:,.2f}")
        print(f"Diff:                        {prof_rev - sales_rev:,.2f}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    compare_tables()
