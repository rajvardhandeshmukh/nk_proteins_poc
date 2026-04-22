import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    server = os.getenv("MSSQL_SERVER", "localhost")
    database = os.getenv("MSSQL_DATABASE", "nk_proteins")
    user = os.getenv("MSSQL_USER", "sa")
    password = os.getenv("MSSQL_PASS")
    driver = "ODBC Driver 17 for SQL Server"
    params = quote_plus(f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={user};PWD={password}")
    conn_str = f"mssql+pyodbc:///?odbc_connect={params}"
    return create_engine(conn_str)

engine = get_engine()

with engine.connect() as conn:
    print("--- Fact Sales Summary ---")
    sales_info = conn.execute(text("SELECT COUNT(*) as cnt, COUNT(DISTINCT invoice_no) as unique_invoices, COUNT(DISTINCT product_id) as unique_products FROM fact_sales")).fetchone()
    print(f"Total Rows: {sales_info[0]}")
    print(f"Unique Invoices: {sales_info[1]}")
    print(f"Unique Products: {sales_info[2]}")

    print("\n--- Fact Profitability Summary ---")
    prof_info = conn.execute(text("SELECT COUNT(*) as cnt, COUNT(DISTINCT product_id) as unique_products FROM fact_profitability")).fetchone()
    print(f"Total Rows: {prof_info[0]}")
    print(f"Unique Products: {prof_info[1]}")

    print("\n--- Sample Sales Row ---")
    sales_sample = conn.execute(text("SELECT TOP 1 * FROM fact_sales")).fetchone()
    print(sales_sample)

    print("\n--- Sample Profitability Row ---")
    prof_sample = conn.execute(text("SELECT TOP 1 * FROM fact_profitability")).fetchone()
    print(prof_sample)
