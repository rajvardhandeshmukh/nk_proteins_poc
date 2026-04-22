"""
V2 SQL Templates for NK Protein POC.
This file contains pure query-to-data extraction logic, 
strictly separated from previous iterations to avoid interference.
"""

SQL_TEMPLATES = {
    "total_revenue_overall": {
        "description": "Total Revenue (Overall) - Pure Extraction",
        "query": "SELECT SUM(revenue) AS Total_Revenue FROM fact_sales;",
        "params": {}
    },
    "revenue_by_region": {
        "description": "Revenue by Region - Pure Extraction",
        "query": "SELECT Customer AS Region, SUM(revenue) AS Revenue FROM fact_sales GROUP BY Customer ORDER BY Revenue DESC;",
        "params": {}
    }
}

VALID_INTENTS = list(SQL_TEMPLATES.keys())
