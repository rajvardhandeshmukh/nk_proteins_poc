# SQL Templates for Sales Domain
from gateway.config import config

# Column wrapping helper for spaces
C_REV = f"{config.COL_NET_AMOUNT}" if config.COL_NET_AMOUNT.startswith("[") else f"[{config.COL_NET_AMOUNT}]"
C_QTY = f"{config.COL_QUANTITY}" if config.COL_QUANTITY.startswith("[") else f"[{config.COL_QUANTITY}]"
C_MAT = f"{config.COL_MATERIAL}" if config.COL_MATERIAL.startswith("[") else f"[{config.COL_MATERIAL}]"
C_NAM = f"{config.COL_PRODUCT_NAME}" if config.COL_PRODUCT_NAME.startswith("[") else f"[{config.COL_PRODUCT_NAME}]"
C_CUS = f"{config.COL_CUSTOMER}" if config.COL_CUSTOMER.startswith("[") else f"[{config.COL_CUSTOMER}]"
C_DAT = f"{config.COL_DATE}" if config.COL_DATE.startswith("[") else f"[{config.COL_DATE}]"
C_PRC = f"{config.COL_PRICE_UNIT}" if config.COL_PRICE_UNIT.startswith("[") else f"[{config.COL_PRICE_UNIT}]"
T_SAL = f"{config.TABLE_SALES}" if config.TABLE_SALES.startswith("[") else f"[{config.TABLE_SALES}]"

SQL_TEMPLATES = {
    "total_revenue": f"""
        SELECT SUM(CAST({C_REV} AS DECIMAL(20,2))) AS [Total Revenue]
        FROM {T_SAL};
    """,
    "total_quantity": f"""
        SELECT SUM(CAST({C_QTY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {T_SAL};
    """,
    "revenue_by_customer": f"""
        SELECT 
            {C_CUS} AS [Customer],
            SUM(CAST({C_REV} AS DECIMAL(20,2))) AS [Revenue],
            SUM(CAST({C_QTY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {T_SAL}
        GROUP BY {C_CUS}
        ORDER BY [Revenue] DESC;
    """,
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
    "top_customers": f"""
        SELECT TOP 10
            {C_CUS} AS [Customer],
            SUM(CAST({C_REV} AS DECIMAL(20,2))) AS [Revenue],
            SUM(CAST({C_QTY} AS DECIMAL(20,2))) AS [Total Quantity]
        FROM {T_SAL}
        GROUP BY {C_CUS}
        ORDER BY [Revenue] DESC;
    """,
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
