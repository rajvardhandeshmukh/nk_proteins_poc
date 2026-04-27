# SQL Templates for Pure Sales Mode (V2 - Ground Truth Protocol V3)
from .config import config

# Rules implemented:
# 1. Revenue = SUM(Gross Value)
# 2. Quantity = SUM(Bill Qty) 
# 3. Derived Price = Revenue / Row Count
# 4. Transaction Price = Price Per Unit (Direct from row)
# 5. Product Grouping = Material Code + Material Desc

# Column wrapping helper for spaces
C_REV = f"[{config.COL_NET_AMOUNT}]"
C_QTY = f"[{config.COL_QUANTITY}]"
C_MAT = f"[{config.COL_MATERIAL}]"
C_NAM = f"[{config.COL_PRODUCT_NAME}]"
C_CUS = f"[{config.COL_CUSTOMER}]"
C_DAT = f"[{config.COL_DATE}]"
C_PRC = f"[{config.COL_PRICE_UNIT}]"
T_SAL = f"[{config.TABLE_SALES}]"

SQL_TEMPLATES = {
    # 1. Total Revenue
    "total_revenue": f"""
        SELECT SUM(CAST({C_REV} AS DECIMAL(20,2))) AS [Total Revenue]
        FROM {T_SAL};
    """,

    # 2. Total Quantity
    "total_quantity": f"""
        SELECT SUM(CAST({C_QTY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {T_SAL};
    """,

    # 3. Revenue by Customer
    "revenue_by_customer": f"""
        SELECT 
            {C_CUS} AS [Customer],
            SUM(CAST({C_REV} AS DECIMAL(20,2))) AS [Revenue],
            SUM(CAST({C_QTY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {T_SAL}
        GROUP BY {C_CUS}
        ORDER BY [Revenue] DESC;
    """,

    # 4. Revenue by Product
    "revenue_by_product": f"""
        SELECT 
            {C_MAT} AS [Product ID],
            {C_NAM} AS [Product Name],
            SUM(CAST({C_REV} AS DECIMAL(20,2))) AS [Revenue],
            SUM(CAST({C_QTY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {T_SAL}
        GROUP BY {C_MAT}, {C_NAM}
        ORDER BY [Revenue] DESC;
    """,

    # 5. Customer -> Product Drilldown
    "customer_product_revenue": f"""
        SELECT 
            {C_CUS} AS [Customer],
            {C_MAT} AS [Product ID],
            {C_NAM} AS [Product Name],
            SUM(CAST({C_REV} AS DECIMAL(20,2))) AS [Revenue],
            SUM(CAST({C_QTY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {T_SAL}
        GROUP BY {C_CUS}, {C_MAT}, {C_NAM}
        ORDER BY [Customer], [Revenue] DESC;
    """,

    # 6. Product Price Analysis (Dual Price Logic)
    "product_price_analysis": f"""
        SELECT 
            {C_MAT} AS [Product ID],
            {C_NAM} AS [Product Name],
            AVG(CAST({C_PRC} AS DECIMAL(20,2))) AS [Avg Price Per Unit],
            SUM(CAST({C_REV} AS DECIMAL(20,2))) * 1.0 / NULLIF(COUNT(*), 0) AS [Derived Price],
            SUM(CAST({C_REV} AS DECIMAL(20,2))) AS [Revenue],
            SUM(CAST({C_QTY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {T_SAL}
        GROUP BY {C_MAT}, {C_NAM}
        ORDER BY [Revenue] DESC;
    """,

    # 7. Customer Price Behavior
    "customer_price_analysis": f"""
        SELECT 
            {C_CUS} AS [Customer],
            AVG(CAST({C_PRC} AS DECIMAL(20,2))) AS [Avg Price Per Unit],
            SUM(CAST({C_REV} AS DECIMAL(20,2))) * 1.0 / NULLIF(COUNT(*), 0) AS [Derived Price],
            SUM(CAST({C_REV} AS DECIMAL(20,2))) AS [Revenue],
            SUM(CAST({C_QTY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {T_SAL}
        GROUP BY {C_CUS}
        ORDER BY [Revenue] DESC;
    """,

    # 8. Top Products
    "top_products": f"""
        SELECT TOP 10
            {C_MAT} AS [Product ID],
            {C_NAM} AS [Product Name],
            SUM(CAST({C_REV} AS DECIMAL(20,2))) AS [Revenue],
            SUM(CAST({C_QTY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {T_SAL}
        GROUP BY {C_MAT}, {C_NAM}
        ORDER BY [Revenue] DESC;
    """,

    # 9. Top Customers
    "top_customers": f"""
        SELECT TOP 10
            {C_CUS} AS [Customer],
            SUM(CAST({C_REV} AS DECIMAL(20,2))) AS [Revenue],
            SUM(CAST({C_QTY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {T_SAL}
        GROUP BY {C_CUS}
        ORDER BY [Revenue] DESC;
    """,

    # 10. Raw Transaction View (Audit)
    "transaction_view": f"""
        SELECT TOP 50
            {C_DAT} AS [Billing Date],
            {C_CUS} AS [Customer Name],
            {C_MAT} AS [Material Code],
            {C_NAM} AS [Material Desc],
            {C_QTY} AS [Bill Qty],
            {C_PRC} AS [Price Per Unit],
            {C_REV} AS [Gross Value]
        FROM {T_SAL}
        ORDER BY {C_DAT};
    """
}
