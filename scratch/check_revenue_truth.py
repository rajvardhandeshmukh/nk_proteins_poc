import os
from dotenv import load_dotenv
from gateway.executor import execute_raw_sql

load_dotenv()

def check_truth():
    print("Checking Database Truth...")
    res = execute_raw_sql("SELECT SUM(NetAmount) AS [Total Revenue] FROM fact_sales")
    print(f"Result: {res}")

if __name__ == "__main__":
    check_truth()
