"""
SQL Template Library — The Backbone (Floor 2)
==============================================
Every query here is:
  - Pre-audited (no SELECT *, no dynamic SQL)
  - Parameterized (no string concatenation)
  - Performance-bound (TOP limits, date filters, indexed columns)

The LLM NEVER writes SQL. It only picks a template + fills params.
"""

SQL_TEMPLATES = {

    # =========================================================================
    # 1. REVENUE TREND
    # =========================================================================
    "revenue_trend": {
        "description": "Monthly revenue trend using Net Sales (before returns).",
        "table": "fact_sales",
        "query": """
            SELECT
                YEAR(event_date) AS yr,
                MONTH(event_date) AS mo,
                FORMAT(event_date, 'MMM yyyy') AS month_label,
                SUM(revenue) AS total_revenue
            FROM fact_sales
            WHERE event_date BETWEEN '2024-10-01' AND '2026-04-30'
            {region_filter}
            {product_filter}
            GROUP BY YEAR(event_date), MONTH(event_date), FORMAT(event_date, 'MMM yyyy')
            ORDER BY yr, mo
        """,
        "params": {},
        "optional_filters": {
            "region_filter": "AND region = :region",
            "product_filter": "AND product_name LIKE :product",
        },
    },

    # =========================================================================
    # 2. TOP PRODUCTS
    # =========================================================================
    "top_products": {
        "description": "Top N products ranked by final Net Sales revenue.",
        "table": "fact_sales",
        "query": """
            SELECT TOP (:limit)
                product_name,
                SUM(revenue) AS total_revenue,
                SUM(quantity) AS total_qty
            FROM fact_sales
            WHERE event_date BETWEEN '2024-10-01' AND '2026-04-30'
            {region_filter}
            {month_filter}
            GROUP BY product_name
            ORDER BY total_revenue DESC
        """,
        "params": {
            "limit": {"type": "int", "default": 10, "max": 50},
        },
        "optional_filters": {
            "region_filter": "AND region = :region",
            "month_filter": "AND LEFT(CONVERT(VARCHAR, event_date, 120), 7) = :month",
        },
    },

    # =========================================================================
    # 3. DEAD STOCK
    # =========================================================================
    "dead_stock": {
        "table": "fact_inventory",
        "query": """
            SELECT TOP (:limit)
                product_id,
                product_name,
                current_stock,
                total_sales_30d,
                (current_stock / NULLIF((total_sales_30d / 30.0), 0)) AS inventory_days,
                CASE
                    WHEN total_sales_30d = 0 THEN 'DEAD'
                    WHEN (current_stock / NULLIF((total_sales_30d / 30.0), 0)) > 90 THEN 'SLOW'
                    ELSE 'HEALTHY'
                END AS stock_status
            FROM fact_inventory
            WHERE current_stock > 0 
              AND (total_sales_30d = 0 OR (current_stock / NULLIF((total_sales_30d / 30.0), 0)) > 90)
            ORDER BY 
                CASE WHEN total_sales_30d = 0 THEN 0 ELSE 1 END ASC,
                (current_stock / NULLIF((total_sales_30d / 30.0), 0)) DESC,
                current_stock DESC
        """,
        "params": {
            "limit": {"type": "int", "default": 20, "max": 100},
        },
        "optional_filters": {},
    },

    # =========================================================================
    # 4. INVENTORY HEALTH
    # =========================================================================
    "inventory_health": {
        "description": "Inventory summary snapshot showing stock bifurcated by unit.",
        "table": "fact_inventory",
        "query": """
            SELECT
                base_uom AS unit,
                COUNT(DISTINCT product_id) AS total_skus,
                SUM(current_stock) AS total_current_stock,
                SUM(current_stock * unit_cost) AS total_inventory_value
            FROM fact_inventory
            WHERE event_date = (SELECT MAX(event_date) FROM fact_inventory)
            {product_filter}
            {region_filter}
            GROUP BY base_uom
        """,
        "params": {},
        "optional_filters": {
            "product_filter": "AND product_name LIKE :product",
            "region_filter": "AND plant_name = :region",
        },
    },

    # =========================================================================
    # 5. REGION COMPARISON
    # =========================================================================
    "region_comparison": {
        "description": "Compare net revenue across regions.",
        "table": "fact_sales",
        "query": """
            SELECT
                region,
                SUM(revenue) AS total_revenue,
                SUM(quantity) AS total_qty,
                billing_quantity_unit as unit,
                COUNT(DISTINCT product_id) AS product_count
            FROM fact_sales
            WHERE event_date BETWEEN '2024-10-01' AND '2026-04-30'
            {month_filter}
            GROUP BY region, billing_quantity_unit
            ORDER BY total_revenue DESC
        """,
        "params": {},
        "optional_filters": {
            "month_filter": "AND LEFT(CONVERT(VARCHAR, event_date, 120), 7) = :month",
        },
    },

    # =========================================================================
    # 6. WORST MARGIN PRODUCTS
    # =========================================================================
    "worst_margins": {
        "description": "Products with the lowest profit margins determined dynamically.",
        "table": "fact_sales",
        "query": """
            SELECT TOP (:limit)
                s.product_name,
                SUM(s.revenue - (s.quantity * i.unit_cost)) AS total_margin,
                SUM(s.revenue) AS total_revenue
            FROM fact_sales s
            LEFT JOIN fact_inventory i ON s.product_name = i.product_name
            WHERE s.event_date BETWEEN '2024-10-01' AND '2026-04-30'
            GROUP BY s.product_name
            ORDER BY total_margin ASC
        """,
        "params": {
            "limit": {"type": "int", "default": 10, "max": 50},
        },
        "optional_filters": {},
    },

    # =========================================================================
    # 7. TOP MARGIN PRODUCTS (BEST SELLERS)
    # =========================================================================
    "top_margins": {
        "description": "Products with the highest profit margins determined dynamically.",
        "table": "fact_sales",
        "query": """
            SELECT TOP (:limit)
                s.product_name,
                SUM(s.revenue - (s.quantity * i.unit_cost)) AS total_margin,
                SUM(s.revenue) AS total_revenue
            FROM fact_sales s
            LEFT JOIN fact_inventory i ON s.product_name = i.product_name
            WHERE s.event_date BETWEEN '2024-10-01' AND '2026-04-30'
            GROUP BY s.product_name
            ORDER BY total_margin DESC
        """,
        "params": {
            "limit": {"type": "int", "default": 10, "max": 50},
        },
        "optional_filters": {},
    },

    # =========================================================================
    # 8. CASHFLOW PROJECTION
    # =========================================================================
    "cashflow_projection": {
        "description": "True realized cashflow aggregated strictly by ClearingDate.",
        "table": "fact_cashflow",
        "query": """
            SELECT
                YEAR(ClearingDate) AS yr,
                MONTH(ClearingDate) AS mo,
                FORMAT(ClearingDate, 'MMM yyyy') AS month_label,
                SUM(Amount) AS total_realized_cash
            FROM fact_cashflow
            WHERE transaction_type = 'customer_receipt'
              AND ClearingDate BETWEEN '2024-10-01' AND '2026-04-30'
            GROUP BY YEAR(ClearingDate), MONTH(ClearingDate), FORMAT(ClearingDate, 'MMM yyyy')
            ORDER BY yr, mo
        """,
        "params": {},
        "optional_filters": {},
    },

}

# Quick lookup: all valid intent names
VALID_INTENTS = list(SQL_TEMPLATES.keys())

