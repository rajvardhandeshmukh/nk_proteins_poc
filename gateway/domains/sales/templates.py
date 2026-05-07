# SQL Templates for Sales Domain
# All queries target sales_clean (view) -- never fact_sales directly.
# FINAL DATA CONTRACT (v2, Feb 2025)
from gateway.config import config

V = config.VIEW_SALES  # "sales_clean"

SQL_TEMPLATES = {

    # 1. Revenue by Region
    "revenue_by_region": f"""
        SELECT
            region,
            SUM(revenue)              AS total_revenue,
            COUNT(DISTINCT bill_doc)  AS invoice_count,
            COUNT(*)                  AS line_item_count
        FROM sales_clean
        WHERE event_date >= :start_date AND event_date <= :end_date
        GROUP BY region
        ORDER BY total_revenue DESC;
    """,

    # 2. Revenue by Customer
    "revenue_by_customer": f"""
        SELECT
            customer_id,
            customer_name,
            region,
            SUM(revenue)              AS total_revenue,
            COUNT(DISTINCT bill_doc)  AS invoice_count,
            COUNT(*)                  AS line_item_count
        FROM sales_clean
        WHERE event_date >= :start_date AND event_date <= :end_date
        GROUP BY customer_id, customer_name, region
        ORDER BY total_revenue DESC;
    """,

    # 3. Revenue by Product (Unit Safe -- grouped by unit to prevent mixing KG+LTR)
    "revenue_by_product": f"""
        SELECT
            product_name,
            unit,
            SUM(revenue)              AS total_revenue,
            SUM(quantity)             AS total_quantity,
            COUNT(DISTINCT bill_doc)  AS invoice_count
        FROM sales_clean
        WHERE event_date >= :start_date AND event_date <= :end_date
        GROUP BY product_name, unit
        ORDER BY total_revenue DESC;
    """,

    # 4. Product by Plant
    "product_by_plant": f"""
        SELECT
            product_name,
            plant,
            unit,
            SUM(revenue)  AS total_revenue,
            SUM(quantity) AS total_quantity
        FROM sales_clean
        WHERE event_date >= :start_date AND event_date <= :end_date
        GROUP BY product_name, plant, unit
        ORDER BY total_revenue DESC;
    """,

    # 5. Revenue by Sales Org
    "revenue_by_sales_org": f"""
        SELECT
            sales_org_code,
            sales_org,
            SUM(revenue)  AS total_revenue,
            COUNT(*)      AS line_item_count
        FROM sales_clean
        WHERE event_date >= :start_date AND event_date <= :end_date
        GROUP BY sales_org_code, sales_org
        ORDER BY total_revenue DESC;
    """,

    # 6. Customer x Product (cross analysis)
    "customer_product_revenue": f"""
        SELECT TOP 20
            customer_id,
            customer_name,
            product_id,
            product_name,
            unit,
            SUM(revenue)  AS total_revenue,
            COUNT(*)      AS line_item_count
        FROM sales_clean
        WHERE event_date >= :start_date AND event_date <= :end_date
        GROUP BY customer_id, customer_name, product_id, product_name, unit
        ORDER BY total_revenue DESC;
    """,

    # 7. Daily Revenue Trend (MTD)
    "daily_revenue_trend": f"""
        SELECT
            event_date,
            SUM(revenue)             AS total_revenue,
            COUNT(DISTINCT bill_doc) AS invoice_count,
            COUNT(*)                 AS line_item_count
        FROM sales_clean
        WHERE event_date >= :start_date AND event_date <= :end_date
        GROUP BY event_date
        ORDER BY event_date;
    """,

    # 8. Total Revenue Summary
    "total_revenue": f"""
        SELECT
            SUM(revenue)              AS total_revenue,
            SUM(cost)                 AS total_cost,
            SUM(revenue - cost) * 100.0 / NULLIF(SUM(revenue), 0) AS weighted_margin_pct,
            COUNT(DISTINCT bill_doc)  AS invoice_count,
            COUNT(*)                  AS line_item_count,
            COUNT(DISTINCT customer_id) AS customer_count
        FROM sales_clean
        WHERE event_date >= :start_date AND event_date <= :end_date;
    """,

    # 9. Top 10 Products by Revenue
    "top_products": f"""
        SELECT TOP 10
            product_id,
            product_name,
            unit,
            SUM(revenue)  AS total_revenue,
            SUM(quantity) AS total_quantity,
            COUNT(*)      AS line_item_count
        FROM sales_clean
        WHERE event_date >= :start_date AND event_date <= :end_date
        GROUP BY product_id, product_name, unit
        ORDER BY total_revenue DESC;
    """,

    # 10. Top 10 Customers by Revenue
    "top_customers": f"""
        SELECT TOP 10
            customer_id,
            customer_name,
            region,
            SUM(revenue)             AS total_revenue,
            COUNT(DISTINCT bill_doc) AS invoice_count,
            COUNT(*)                 AS line_item_count
        FROM sales_clean
        WHERE event_date >= :start_date AND event_date <= :end_date
        GROUP BY customer_id, customer_name, region
        ORDER BY total_revenue DESC;
    """,

    # 11. Revenue by Sales Office
    "revenue_by_sales_office": f"""
        SELECT
            sales_office_code,
            sales_office,
            SUM(revenue)              AS total_revenue,
            COUNT(DISTINCT bill_doc)  AS invoice_count,
            COUNT(*)                  AS line_item_count
        FROM sales_clean
        WHERE event_date >= :start_date AND event_date <= :end_date
        GROUP BY sales_office_code, sales_office
        ORDER BY total_revenue DESC;
    """,
}
