import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Database connection details
server = os.getenv("MSSQL_SERVER", "localhost")
database = os.getenv("MSSQL_DATABASE", "nk_proteins")
user = os.getenv("MSSQL_USER")
password = os.getenv("MSSQL_PASS")
port = os.getenv("MSSQL_PORT", "1433")

connection_string = f"mssql+pyodbc://{user}:{password}@{server}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(connection_string)

def check_schema():
    try:
        with engine.connect() as conn:
            # Check if fact_sales exists
            result = conn.execute(text("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'fact_sales'"))
            columns = result.fetchall()
            if columns:
                print("Columns in fact_sales:")
                for col in columns:
                    print(f" - {col[0]} ({col[1]})")
            else:
                print("Table fact_sales does not exist.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_schema()
