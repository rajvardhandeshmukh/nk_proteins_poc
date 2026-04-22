# SQL Templates for Pure Sales Mode (V2 - Strict Logic)

SQL_TEMPLATES = {
    "total_revenue": """
        SELECT SUM(NetAmount) AS [Total Revenue (INR)]
        FROM fact_sales;
    """,
    
    "revenue_by_region": """
        -- Demand View
        SELECT CustomerRegionName AS [Region], 
               SUM(NetAmount) AS [Revenue (INR)]
        FROM fact_sales
        GROUP BY CustomerRegionName
        ORDER BY [Revenue (INR)] DESC;
    """,
    
    "revenue_by_plant": """
        -- Supply View
        SELECT PlantCityName AS [Supply Point], 
               SUM(NetAmount) AS [Revenue (INR)]
        FROM fact_sales
        GROUP BY PlantCityName
        ORDER BY [Revenue (INR)] DESC;
    """,
    
    "top_products_overall": """
        -- Global Revenue Ranking (Ignores units as revenue is additive)
        SELECT TOP 10 
               Material,
               ProductName,
               SUM(NetAmount) AS [Revenue (INR)]
        FROM fact_sales
        GROUP BY Material, ProductName
        ORDER BY [Revenue (INR)] DESC;
    """,
    
    "top_products_revenue_unit_safe": """
        -- Demand View (Strictly separated by unit)
        SELECT TOP 10 Material, 
               ProductName, 
               BillingQuantityUnit AS [Unit],
               SUM(NetAmount) AS [Revenue (INR)]
        FROM fact_sales
        GROUP BY Material, ProductName, BillingQuantityUnit
        ORDER BY [Revenue (INR)] DESC;
    """,
    
    "product_performance_detailed": """
        -- Strict Unit Isolation Rule
        SELECT Material, 
               ProductName, 
               BillingQuantityUnit AS [Unit],
               SUM(NetAmount) AS [Revenue (INR)],
               SUM(BillingQuantity) AS [Total Quantity]
        FROM fact_sales
        GROUP BY Material, ProductName, BillingQuantityUnit
        ORDER BY [Revenue (INR)] DESC, [Total Quantity] DESC;
    """,
    
    "profitability_all": """
        -- GrossMargin_All (Includes zero-cost rows)
        SELECT SUM(NetAmount - CostAmount) AS [Gross Margin (All)]
        FROM fact_sales;
    """,
    
    "profitability_valid": """
        -- Analyzes only rows where CostAmount is available
        SELECT 
            SUM(NetAmount - CostAmount) AS [Gross Margin (Valid)],
            COUNT(*) AS [Rows Used],
            SUM(NetAmount) AS [Revenue Covered]
        FROM fact_sales
        WHERE CostAmount > 0;
    """,
    
    "monthly_revenue_trend": """
        SELECT 
            CONCAT(YEAR(BillingDocumentDate), '-', RIGHT('0' + CAST(MONTH(BillingDocumentDate) AS VARCHAR), 2)) AS [Year-Month],
            SUM(NetAmount) AS [Revenue (INR)]
        FROM fact_sales
        GROUP BY YEAR(BillingDocumentDate), MONTH(BillingDocumentDate)
        ORDER BY [Year-Month];
    """,
    
    "daily_revenue_trend": """
        SELECT CAST(BillingDocumentDate AS DATE) AS [Date],
               SUM(NetAmount) AS [Revenue (INR)]
        FROM fact_sales
        GROUP BY CAST(BillingDocumentDate AS DATE)
        ORDER BY [Date] ASC;

    """,
    "revenue_by_region_product": """
    -- Demand View (Unit-safe)
    SELECT CustomerRegionName AS [Region],
           Material,
           ProductName,
           BillingQuantityUnit AS [Unit],
           SUM(NetAmount) AS [Revenue (INR)]
    FROM fact_sales
    GROUP BY CustomerRegionName, Material, ProductName, BillingQuantityUnit
    ORDER BY [Revenue (INR)] DESC;
""",
}

