# SQL Templates for Pure Sales Mode (V2 - Ground Truth Protocol V3)
from .config import config

# Rules implemented:
# 1. Revenue = SUM(Gross Value)
# 2. Quantity = SUM(Bill Qty) 
# 3. Derived Price = Revenue / Row Count
# 4. Transaction Price = Price Per Unit (Direct from row)
# 5. Product Grouping = Material Code + Material Desc

SQL_TEMPLATES = {
    # 1. Total Revenue
    "total_revenue": f"""
        SELECT SUM(CAST({config.COL_NET_AMOUNT} AS DECIMAL(20,2))) AS [Total Revenue]
        FROM {config.TABLE_SALES};
    """,

    # 2. Total Quantity
    "total_quantity": f"""
        SELECT SUM(CAST({config.COL_QUANTITY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {config.TABLE_SALES};
    """,

    # 3. Revenue by Customer
    "revenue_by_customer": f"""
        SELECT 
            {config.COL_CUSTOMER} AS [Customer],
            SUM(CAST({config.COL_NET_AMOUNT} AS DECIMAL(20,2))) AS [Revenue],
            SUM(CAST({config.COL_QUANTITY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {config.TABLE_SALES}
        GROUP BY {config.COL_CUSTOMER}
        ORDER BY [Revenue] DESC;
    """,

    # 4. Revenue by Product
    "revenue_by_product": f"""
        SELECT 
            {config.COL_MATERIAL} AS [Product ID],
            {config.COL_PRODUCT_NAME} AS [Product Name],
            SUM(CAST({config.COL_NET_AMOUNT} AS DECIMAL(20,2))) AS [Revenue],
            SUM(CAST({config.COL_QUANTITY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {config.TABLE_SALES}
        GROUP BY {config.COL_MATERIAL}, {config.COL_PRODUCT_NAME}
        ORDER BY [Revenue] DESC;
    """,

    # 5. Customer -> Product Drilldown
    "customer_product_revenue": f"""
        SELECT 
            {config.COL_CUSTOMER} AS [Customer],
            {config.COL_MATERIAL} AS [Product ID],
            {config.COL_PRODUCT_NAME} AS [Product Name],
            SUM(CAST({config.COL_NET_AMOUNT} AS DECIMAL(20,2))) AS [Revenue],
            SUM(CAST({config.COL_QUANTITY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {config.TABLE_SALES}
        GROUP BY {config.COL_CUSTOMER}, {config.COL_MATERIAL}, {config.COL_PRODUCT_NAME}
        ORDER BY [Customer], [Revenue] DESC;
    """,

    # 6. Product Price Analysis (Dual Price Logic)
    "product_price_analysis": f"""
        SELECT 
            {config.COL_MATERIAL} AS [Product ID],
            {config.COL_PRODUCT_NAME} AS [Product Name],
            AVG(CAST({config.COL_PRICE_UNIT} AS DECIMAL(20,2))) AS [Avg Price Per Unit],
            SUM(CAST({config.COL_NET_AMOUNT} AS DECIMAL(20,2))) * 1.0 / NULLIF(COUNT(*), 0) AS [Derived Price],
            SUM(CAST({config.COL_NET_AMOUNT} AS DECIMAL(20,2))) AS [Revenue],
            SUM(CAST({config.COL_QUANTITY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {config.TABLE_SALES}
        GROUP BY {config.COL_MATERIAL}, {config.COL_PRODUCT_NAME}
        ORDER BY [Revenue] DESC;
    """,

    # 7. Customer Price Behavior
    "customer_price_analysis": f"""
        SELECT 
            {config.COL_CUSTOMER} AS [Customer],
            AVG(CAST({config.COL_PRICE_UNIT} AS DECIMAL(20,2))) AS [Avg Price Per Unit],
            SUM(CAST({config.COL_NET_AMOUNT} AS DECIMAL(20,2))) * 1.0 / NULLIF(COUNT(*), 0) AS [Derived Price],
            SUM(CAST({config.COL_NET_AMOUNT} AS DECIMAL(20,2))) AS [Revenue],
            SUM(CAST({config.COL_QUANTITY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {config.TABLE_SALES}
        GROUP BY {config.COL_CUSTOMER}
        ORDER BY [Revenue] DESC;
    """,

    # 8. Top Products
    "top_products": f"""
        SELECT TOP 10
            {config.COL_MATERIAL} AS [Product ID],
            {config.COL_PRODUCT_NAME} AS [Product Name],
            SUM(CAST({config.COL_NET_AMOUNT} AS DECIMAL(20,2))) AS [Revenue],
            SUM(CAST({config.COL_QUANTITY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {config.TABLE_SALES}
        GROUP BY {config.COL_MATERIAL}, {config.COL_PRODUCT_NAME}
        ORDER BY [Revenue] DESC;
    """,

    # 9. Top Customers
    "top_customers": f"""
        SELECT TOP 10
            {config.COL_CUSTOMER} AS [Customer],
            SUM(CAST({config.COL_NET_AMOUNT} AS DECIMAL(20,2))) AS [Revenue],
            SUM(CAST({config.COL_QUANTITY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {config.TABLE_SALES}
        GROUP BY {config.COL_CUSTOMER}
        ORDER BY [Revenue] DESC;
    """,

    # 10. Raw Transaction View (Audit)
    "transaction_view": f"""
        SELECT TOP 50
            {config.COL_DATE} AS [Billing Date],
            {config.COL_CUSTOMER} AS [Customer Name],
            {config.COL_MATERIAL} AS [Material Code],
            {config.COL_PRODUCT_NAME} AS [Material Desc],
            {config.COL_QUANTITY} AS [Bill Qty],
            {config.COL_PRICE_UNIT} AS [Price Per Unit],
            {config.COL_NET_AMOUNT} AS [Gross Value]
        FROM {config.TABLE_SALES}
        ORDER BY {config.COL_DATE};
    """
}
