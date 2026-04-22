import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()

server = os.getenv("MSSQL_SERVER")
database = os.getenv("MSSQL_DATABASE")
user = os.getenv("MSSQL_USER")
password = os.getenv("MSSQL_PASS")
driver = "ODBC Driver 17 for SQL Server"

conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={user};PWD={password}"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

cursor.execute("SELECT TOP 5 CostAmount, revenue, product_name FROM fact_sales")
rows = cursor.fetchall()

print("TOP 5 ROWS IN fact_sales:")
for row in rows:
    print(f"Cost: {row[0]}, Rev: {row[1]}, Name: {row[2]}")

cursor.execute("SELECT COUNT(*) FROM fact_sales WHERE CostAmount > 0")
print(f"Count with CostAmount > 0: {cursor.fetchone()[0]}")

cursor.execute("SELECT COUNT(*) FROM fact_sales")
print(f"Total count: {cursor.fetchone()[0]}")
