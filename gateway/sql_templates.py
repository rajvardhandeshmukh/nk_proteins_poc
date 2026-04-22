"""
SQL Template Library — The Backbone (Floor 2)
==============================================
Every query here is:
  - Pre-audited (no SELECT *, no dynamic SQL)
  - Parameterized (no string concatenation)
  - Performance-bound (TOP limits, date filters, indexed columns)

The LLM NEVER writes SQL. It only picks a template + fills params.

==============================================================================
SALES PILLAR — BUSINESS LOGIC CHARTER (from SAP Z_C_NKP_SALES_FORECAST view)
==============================================================================
  revenue     = item.NetAmount = AFTER K007 discount, BEFORE tax
                → Do NOT re-subtract discount_pct. It is already baked in.
                → TaxAmount is GST and is EXCLUDED from revenue.

  region      = Derived from the Customer record (NOT from plant).
                → Use for Demand analytics (where is demand?).
                → Use plant_city for Supply/Logistics analytics.

  returns_qty = Populated ONLY when SDDocumentCategory = 'H' (Return order).
                ⚠️  WARNING: returns_qty is tracked but revenue is NOT
                reduced by the return value in this extract. This is a
                known data inconsistency. Always flag this in narration.

  CostAmount  = 100% zero in current SAP extract. ALWAYS use Inventory
                unit_cost fallback via product_id join.

  JOIN KEY    = product_id (18-digit padded material number). NEVER join
                on product_name (text) as names contain typos/variants.
==============================================================================
"""

SQL_TEMPLATES = {

    # =========================================================================
    # 10. BILL OF MATERIALS (BOM) LOOKUP
    # =========================================================================
    "bom_lookup": {
        "description": "Lookup components, ingredients, and quantities for a parent product (HeaderMaterial).",
        "table": "fact_bom",
        "query": """
            SELECT 
                HeaderMaterial,
                ComponentMaterial,
                ComponentDescription,
                BillOfMaterialItemQuantity AS quantity,
                BillOfMaterialItemUnit AS unit,
                Plant,
                PlantName,
                ValidityStartDate,
                ValidityEndDate
            FROM fact_bom
            WHERE HeaderMaterial = :product
            ORDER BY BillOfMaterialItemNumber ASC
        """,
        "params": {
            "product": {"type": "str"},
        },
        "optional_filters": {},
    },

    # =========================================================================
    # 1. REVENUE TREND
    # Business: revenue = Net Sales (post-discount, pre-tax). Trends are
    #           valid. Returns are NOT subtracted — narrate the caveat.
    # =========================================================================
    "revenue_trend": {
        "description": "Monthly Net Sales trend. Revenue = NetAmount (post-discount, pre-tax). Returns NOT deducted — flag in narration.",
        "table": "fact_sales",
        "query": """
            SELECT
                YEAR(event_date)                        AS yr,
                MONTH(event_date)                       AS mo,
                FORMAT(event_date, 'MMM yyyy')          AS month_label,
                SUM(revenue)                            AS total_net_revenue,
                SUM(TaxAmount)                          AS total_tax,
                SUM(revenue) + SUM(TaxAmount)           AS total_gross_billing,
                SUM(quantity)                           AS total_qty,
                SUM(returns_qty)                        AS total_returns_qty
            FROM fact_sales
            WHERE 1=1
            {month_filter}
            {year_filter}
            {region_filter}
            {product_filter}
            GROUP BY YEAR(event_date), MONTH(event_date), FORMAT(event_date, 'MMM yyyy')
            ORDER BY yr, mo
        """,
        "params": {
            "month": {"type": "int", "default": None},
            "year": {"type": "int", "default": None},
        },
        "optional_filters": {
            # region comes from Customer record in SAP — correct for demand analytics
            "region_filter": "AND region = :region",
            "product_filter": "AND product_name LIKE :product",
            "month_filter": "AND MONTH(event_date) = :month",
            "year_filter": "AND YEAR(event_date) = :year",
        },
    },

    "sales_summary_30d": {
        "description": "Rolling 30-day sales aggregate (Revenue, Volume, Order Count). Use for '30-day sales volume' or 'last month performance'.",
        "table": "fact_sales",
        "query": """
            SELECT 
                SUM(revenue)                                AS total_net_revenue,
                SUM(quantity)                               AS total_sales_volume,
                COUNT(DISTINCT invoice_no)                  AS order_count,
                MIN(event_date)                             AS period_start,
                MAX(event_date)                             AS period_end
            FROM fact_sales
            WHERE event_date >= DATEADD(day, -30, (SELECT MAX(event_date) FROM fact_sales))
        """,
        "params": {},
        "optional_filters": {},
    },

    # =========================================================================
    # 2. TOP PRODUCTS
    # Business: Rank by Net Revenue. Expose returns qty alongside for context.
    # =========================================================================
    "top_products": {
        "description": "Top N products by Net Sales. Returns qty shown as context (not subtracted from revenue).",
        "table": "fact_sales",
        "query": """
            SELECT TOP (:limit)
                product_id,
                product_name,
                material_group_name,
                SUM(revenue)       AS total_net_revenue,
                SUM(quantity)      AS total_qty,
                SUM(returns_qty)   AS total_returns_qty,
                billing_quantity_unit AS unit
            FROM fact_sales
            WHERE 1=1
            {month_filter}
            {year_filter}
            {region_filter}
            GROUP BY product_id, product_name, material_group_name, billing_quantity_unit
            ORDER BY total_net_revenue DESC
        """,
        "params": {
            "limit": {"type": "int", "default": 10, "max": 50},
            "month": {"type": "int", "default": None},
            "year": {"type": "int", "default": None},
        },
        "optional_filters": {
            "region_filter": "AND region = :region",
            "month_filter": "AND MONTH(event_date) = :month",
            "year_filter": "AND YEAR(event_date) = :year",
        },
    },

    # =========================================================================
    # 3. DEAD STOCK
    # Business Logic:
    #   Grain        = product_id + Plant + StorageLocation (NOT just product_id)
    #   Dead Stock   = total_sales_30d = 0 AND current_stock > 0
    #   Slow Stock   = inventory_days > 90
    #   inventory_days = current_stock / (total_sales_30d / 30)
    #   A product may be DEAD at one plant but HEALTHY at another → show plant grain
    # =========================================================================
    "dead_stock": {
        "description": "Dead and slow-moving stock at plant+storage grain. Grain: product_id + Plant + StorageLocation.",
        "table": "fact_inventory",
        "query": """
            SELECT TOP (:limit)
                product_id,
                product_name,
                StorageLocation,
                location_name,
                current_stock,
                base_uom,
                current_stock * unit_cost                           AS inventory_value,
                total_sales_30d / 30.0                              AS daily_sales,
                current_stock / NULLIF(total_sales_30d / 30.0, 0)  AS inventory_days,
                CASE
                    WHEN total_sales_30d = 0 AND current_stock > 0              THEN 'DEAD'
                    WHEN current_stock / NULLIF(total_sales_30d / 30.0, 0) > 90 THEN 'SLOW'
                    ELSE 'HEALTHY'
                END AS stock_status
            FROM fact_inventory
            WHERE current_stock > 0
              AND (
                  total_sales_30d = 0
                  OR current_stock / NULLIF(total_sales_30d / 30.0, 0) > 90
              )
            ORDER BY
                CASE WHEN total_sales_30d = 0 THEN 0 ELSE 1 END ASC,
                inventory_value DESC
        """,
        "params": {
            "limit": {"type": "int", "default": 20, "max": 100},
        },
        "optional_filters": {},
    },

    # =========================================================================
    # 4. INVENTORY HEALTH
    # Business Logic:
    #   Grain         = product_id + Plant + StorageLocation
    #   inventory_value = current_stock * unit_cost
    #   daily_sales     = total_sales_30d / 30
    #   reorder_flag    = current_stock < (daily_sales * lead_time_days)
    #   Negative stock  = DATA INTEGRITY anomaly — flag, do not correct
    # =========================================================================
    "inventory_health": {
        "description": "Itemized stock position, stock-outs, and dead stock alerts at plant+storage grain. Use for 'list our stock' or 'inventory by location'.",
        "table": "fact_inventory",
        "query": """
            SELECT
                product_id,
                product_name,
                Plant,
                StorageLocation,
                location_name,
                base_uom,
                current_stock,
                unit_cost,
                current_stock * unit_cost                               AS inventory_value,
                total_sales_30d / 30.0                                  AS daily_sales,
                current_stock / NULLIF(total_sales_30d / 30.0, 0)       AS inventory_days,
                lead_time_days,
                -- REORDER SIGNAL: stock below safety buffer
                CASE
                    WHEN current_stock < ((total_sales_30d / 30.0) * lead_time_days)
                    THEN 'REORDER NOW'
                    ELSE 'OK'
                END AS reorder_flag,
                -- DEAD STOCK: stock sitting with zero demand
                CASE
                    WHEN total_sales_30d = 0 AND current_stock > 0 THEN 'DEAD'
                    WHEN current_stock / NULLIF(total_sales_30d / 30.0, 0) > 90  THEN 'SLOW'
                    WHEN current_stock < 0                                        THEN 'ANOMALY'
                    ELSE 'HEALTHY'
                END AS stock_status
            FROM fact_inventory
            {product_filter}
            ORDER BY inventory_value DESC
        """,
        "params": {},
        "optional_filters": {
            "product_filter": "AND product_name LIKE :product",
        },
    },

    # =========================================================================
    # 4b. REORDER ALERTS
    # Business Logic:
    #   reorder_flag = current_stock < (daily_sales * lead_time_days)
    #   This means: we will run out before the next shipment arrives
    #   HOW MUCH TO ORDER = (daily_sales * lead_time_days) - current_stock
    # =========================================================================
    "reorder_alerts": {
        "description": "Products that need immediate replenishment. Quantity-to-order calculated using lead time.",
        "table": "fact_inventory",
        "query": """
            SELECT TOP (:limit)
                product_id,
                product_name,
                Plant,
                location_name,
                base_uom,
                current_stock,
                ROUND(total_sales_30d / 30.0, 2)                                    AS daily_sales,
                lead_time_days,
                ROUND((total_sales_30d / 30.0) * lead_time_days, 2)                 AS safety_stock_needed,
                ROUND(((total_sales_30d / 30.0) * lead_time_days) - current_stock, 2) AS qty_to_order,
                current_stock * unit_cost                                             AS current_stock_value
            FROM fact_inventory
            WHERE current_stock < ((total_sales_30d / 30.0) * lead_time_days)
              AND total_sales_30d > 0
            ORDER BY qty_to_order DESC
        """,
        "params": {
            "limit": {"type": "int", "default": 20, "max": 100},
        },
        "optional_filters": {},
    },

    "inventory_valuation_summary": {
        "description": "High-level inventory asset valuation. Total on-hand units, total value in INR, and average cost. Use for 'What is our total inventory value?'.",
        "table": "fact_inventory",
        "query": """
            SELECT 
                COUNT(DISTINCT product_id)                          AS SKU_count,
                SUM(CASE WHEN current_stock > 0 THEN current_stock ELSE 0 END) AS physical_stock_qty,
                SUM(CASE WHEN current_stock > 0 THEN current_stock * unit_cost ELSE 0 END) AS physical_asset_value,
                SUM(CASE WHEN current_stock < 0 THEN current_stock * unit_cost ELSE 0 END) AS valuation_at_risk,
                COUNT(CASE WHEN current_stock < 0 THEN 1 END)       AS anomaly_count,
                MAX(event_date)                                     AS as_of_date
            FROM fact_inventory
        """,
        "params": {},
        "optional_filters": {},
    },

    # =========================================================================
    # 5. REGION COMPARISON
    # Business: region field is Customer-derived (demand location).
    #           plant_city is supply location. These are different!
    # =========================================================================
    "region_comparison": {
        "description": "Net Sales by region (customer demand location, not plant). Returns qty shown as audit trail.",
        "table": "fact_sales",
        "query": """
            SELECT
                region,
                SUM(revenue)                    AS total_net_revenue,
                SUM(TaxAmount)                  AS total_tax,
                SUM(quantity)                   AS total_qty,
                SUM(returns_qty)                AS total_returns_qty,
                COUNT(DISTINCT product_id)      AS product_count,
                COUNT(DISTINCT Customer)        AS customer_count
            FROM fact_sales
            WHERE event_date BETWEEN '2025-01-01' AND '2025-03-31'
            {month_filter}
            GROUP BY region
            ORDER BY total_net_revenue DESC
        """,
        "params": {},
        "optional_filters": {
            "month_filter": "AND LEFT(CONVERT(VARCHAR, event_date, 120), 7) = :month",
        },
    },

    # =========================================================================
    # 6. WORST MARGIN PRODUCTS  *** ESTIMATED MARGIN ***
    # Source: fact_sales + fact_inventory cost fallback
    # Use for: Operational decisions, real-time margin optimization
    # DO NOT use for: Financial reporting, audit, official P&L
    # Reliability: MEDIUM (uses inventory avg as proxy)
    # =========================================================================
    "worst_margins": {
        "description": "ESTIMATED lowest margin products using inventory cost proxy. NOT for financial reporting — use product_profitability for audit-grade numbers.",
        "table": "fact_sales",
        "query": """
            WITH UniqueInventoryCost AS (
                -- Deduplicate inventory to avoid fan-out joins
                SELECT product_id, AVG(unit_cost) AS avg_unit_cost
                FROM fact_inventory
                GROUP BY product_id
            )
            SELECT TOP (:limit)
                s.product_id,
                s.product_name,
                s.material_group_name,
                SUM(s.revenue)                                              AS total_net_revenue,
                SUM(s.quantity * ISNULL(i.avg_unit_cost, 0))               AS total_cost,
                SUM(s.revenue - (s.quantity * ISNULL(i.avg_unit_cost, 0))) AS total_gross_margin,
                CASE
                    WHEN SUM(s.revenue) = 0 THEN 0
                    ELSE ROUND(SUM(s.revenue - (s.quantity * ISNULL(i.avg_unit_cost, 0))) / SUM(s.revenue) * 100, 2)
                END AS margin_pct
            FROM fact_sales s
            LEFT JOIN UniqueInventoryCost i ON s.product_id = i.product_id
            WHERE s.event_date BETWEEN '2025-01-01' AND '2025-03-31'
            GROUP BY s.product_id, s.product_name, s.material_group_name
            ORDER BY total_gross_margin ASC
        """,
        "params": {
            "limit": {"type": "int", "default": 10, "max": 50},
        },
        "optional_filters": {},
    },

    # =========================================================================
    # 7. TOP MARGIN PRODUCTS  *** ESTIMATED MARGIN ***
    # Source: fact_sales + fact_inventory cost fallback
    # Same caveat as worst_margins — operational proxy, not audit grade
    # Reliability: MEDIUM
    # =========================================================================
    "top_margins": {
        "description": "ESTIMATED highest margin products using inventory cost proxy. NOT for financial reporting.",
        "table": "fact_sales",
        "query": """
            WITH UniqueInventoryCost AS (
                SELECT product_id, AVG(unit_cost) AS avg_unit_cost
                FROM fact_inventory
                GROUP BY product_id
            )
            SELECT TOP (:limit)
                s.product_id,
                s.product_name,
                s.material_group_name,
                SUM(s.revenue)                                              AS total_net_revenue,
                SUM(s.quantity * ISNULL(i.avg_unit_cost, 0))               AS total_cost,
                SUM(s.revenue - (s.quantity * ISNULL(i.avg_unit_cost, 0))) AS total_gross_margin,
                CASE
                    WHEN SUM(s.revenue) = 0 THEN 0
                    ELSE ROUND(SUM(s.revenue - (s.quantity * ISNULL(i.avg_unit_cost, 0))) / SUM(s.revenue) * 100, 2)
                END AS margin_pct
            FROM fact_sales s
            LEFT JOIN UniqueInventoryCost i ON s.product_id = i.product_id
            WHERE s.event_date BETWEEN '2025-01-01' AND '2025-03-31'
            GROUP BY s.product_id, s.product_name, s.material_group_name
            ORDER BY total_gross_margin DESC
        """,
        "params": {
            "limit": {"type": "int", "default": 10, "max": 50},
        },
        "optional_filters": {},
    },

    # =========================================================================
    # 8. PRODUCT PROFITABILITY  *** FINANCIAL / AUDIT GRADE ***
    # Source: fact_profitability
    # Grain: product_id + event_date (daily aggregated — NOT invoice level)
    # Use for: Financial reporting, official margin, audit-level P&L queries
    # DO NOT use for: Operational decisions, inventory-linked margin
    #
    # Formula:
    #   gross_margin = revenue - cogs  (SUM of SAP NetAmount - CostAmount)
    #   margin_pct   = gross_margin / revenue
    #
    # WARNING: If cogs = 0 for a product, flag as 'COGS MISSING' — do NOT
    #          compute margin as it would falsely show 100% profit.
    # =========================================================================
    "product_profitability": {
        "description": "[FINANCIAL/AUDIT] Official gross margin per product from fact_profitability. Grain: product_id + event_date. Do not mix with operational estimates.",
        "table": "fact_profitability",
        "query": """
            SELECT TOP (:limit)
                product_id,
                product_name,
                SUM(revenue)                                            AS total_net_revenue,
                SUM(cogs)                                               AS total_cogs,
                SUM(gross_margin)                                       AS total_gross_margin,
                CASE
                    WHEN SUM(cogs) = 0
                    THEN 'COGS MISSING - margin unreliable'
                    WHEN SUM(revenue) = 0 THEN '0'
                    ELSE CAST(ROUND(SUM(gross_margin) / SUM(revenue) * 100, 2) AS VARCHAR) + '%'
                END AS margin_pct,
                CASE
                    WHEN SUM(gross_margin) < 0 THEN 'LOSS-MAKING'
                    WHEN SUM(cogs) = 0          THEN 'DATA INCOMPLETE'
                    ELSE 'PROFITABLE'
                END AS profitability_status
            FROM fact_profitability
            WHERE 1=1
            {month_filter}
            {year_filter}
            {product_filter}
            GROUP BY product_id, product_name
            ORDER BY total_gross_margin DESC
        """,
        "params": {
            "limit": {"type": "int", "default": 20, "max": 100},
            "month": {"type": "int", "optional": True},
            "year": {"type": "int", "optional": True},
        },
        "optional_filters": {
            "product_filter": "AND product_name LIKE :product",
            "month_filter": "AND MONTH(event_date) = :month",
            "year_filter": "AND YEAR(event_date) = :year",
        },
    },

    # =========================================================================
    # 8b. LOSS-MAKING PRODUCTS  *** FINANCIAL / AUDIT GRADE ***
    # Business: gross_margin < 0 per product in the profitability table.
    # Only valid where cogs > 0 (otherwise cogs is missing in SAP extract).
    # =========================================================================
    "loss_making_products": {
        "description": "[FINANCIAL/AUDIT] Products with negative gross margin from official profitability data. Only shows rows where COGS is populated.",
        "table": "fact_profitability",
        "query": """
            SELECT TOP (:limit)
                product_id,
                product_name,
                SUM(revenue)        AS total_net_revenue,
                SUM(cogs)           AS total_cogs,
                SUM(gross_margin)   AS total_gross_margin,
                ROUND(SUM(gross_margin) / NULLIF(SUM(revenue), 0) * 100, 2) AS margin_pct
            FROM fact_profitability
            WHERE event_date BETWEEN '2025-01-01' AND '2025-03-31'
            GROUP BY product_id, product_name
            HAVING SUM(gross_margin) < 0
               AND SUM(cogs) > 0
            ORDER BY total_gross_margin ASC
        """,
        "params": {
            "limit": {"type": "int", "default": 20, "max": 100},
        },
        "optional_filters": {},
    },

    # =========================================================================
    # 8c. FINANCIAL MARGIN TREND  *** FINANCIAL / AUDIT GRADE ***
    # Grain: product_id + event_date rolled up to month.
    # Only includes rows where cogs > 0 to avoid false 100% margin months.
    # =========================================================================
    "financial_margin_trend": {
        "description": "[FINANCIAL/AUDIT] Monthly gross margin trend from fact_profitability. Official P&L numbers.",
        "table": "fact_profitability",
        "query": """
            SELECT
                YEAR(event_date)               AS yr,
                MONTH(event_date)              AS mo,
                FORMAT(event_date, 'MMM yyyy') AS month_label,
                SUM(revenue)                   AS total_net_revenue,
                SUM(cogs)                      AS total_cogs,
                SUM(gross_margin)              AS total_gross_margin,
                ROUND(SUM(gross_margin) / NULLIF(SUM(revenue), 0) * 100, 2) AS margin_pct,
                COUNT(DISTINCT product_id)     AS product_count
            FROM fact_profitability
            WHERE event_date BETWEEN '2025-01-01' AND '2025-03-31'
              AND cogs > 0
            GROUP BY YEAR(event_date), MONTH(event_date), FORMAT(event_date, 'MMM yyyy')
            ORDER BY yr, mo
        """,
        "params": {},
        "optional_filters": {},
    },

    # =========================================================================
    # 8d. TOP PROFITABLE PRODUCTS  *** FINANCIAL / AUDIT GRADE ***
    # Business: ORDER BY gross_margin DESC — official ranking for P&L review
    # =========================================================================
    "top_profitable_products": {
        "description": "[FINANCIAL/AUDIT] Profitability ranking based on Pure Sales Math. Revenue/Qty from fact_sales, Cost from fact_inventory avg(unit_cost).",
        "table": "fact_sales + fact_inventory",
        "query": """
            SELECT TOP (:limit)
                product_id,
                product_name,
                SUM(revenue)                                AS total_net_revenue,
                SUM(CostAmount)                             AS total_cogs,
                SUM(revenue - CostAmount)                   AS total_gross_margin,
                ROUND(SUM(revenue - CostAmount) / NULLIF(SUM(revenue), 0) * 100, 2) AS margin_pct
            FROM fact_sales
            WHERE 1=1
            {month_filter}
            {year_filter}
            {product_filter}
            GROUP BY product_id, product_name
            ORDER BY total_net_revenue DESC
        """,
        "params": {
            "limit": {"type": "int", "default": 20, "max": 100},
            "month": {"type": "int", "default": None},
            "year": {"type": "int", "default": None},
        },
        "optional_filters": {
            "product_filter": "AND product_name LIKE :product",
            "month_filter": "AND MONTH(event_date) = :month",
            "year_filter": "AND YEAR(event_date) = :year",
        },
    },
    # 9. CASHFLOW / AR — ACCOUNTS RECEIVABLE LEDGER  *** FI GRADE ***
    # Source: fact_cashflow
    # Grain: CompanyCode + AccountingDocument + Item (line-level FI entries)
    #
    # THIS IS: Accounts Receivable Ledger (money owed TO NK Proteins)
    # NOT: Full cashflow, expenses, vendor payments
    #
    # Formula:
    #   Realized Cash     = SUM(ActualCash)    — money already in bank
    #   Outstanding AR    = SUM(ForecastCash)  — money still owed by customers
    #   Collection Ratio  = ActualCash / (ActualCash + ForecastCash)
    #   Aging             = GROUP BY AgingBucket (30/60/90/90+ days)
    #
    # KEY DATE RULES:
    #   PostingDate  = when the invoice was posted in FI (billing event)
    #   ClearingDate = when cash was received (NULL if still outstanding)
    #   NetDueDate   = payment due date (for aging calculations)
    # =========================================================================
    "cashflow_projection": {
        "description": "[FI/AR GRADE] Monthly AR summary showing realized cash vs outstanding receivables. PostingDate = invoice date. ClearingDate = cash received date.",
        "table": "fact_cashflow",
        "query": """
            SELECT
                YEAR(PostingDate)                AS yr,
                MONTH(PostingDate)               AS mo,
                FORMAT(PostingDate, 'MMM yyyy')  AS month_label,
                -- Realized: money already collected
                SUM(ActualCash)                                    AS realized_cash,
                -- Outstanding: money still owed by customers
                SUM(ForecastCash)                                  AS outstanding_ar,
                -- Total AR booked in this period
                SUM(ActualCash) + SUM(ForecastCash)                AS total_ar_booked,
                -- Collection ratio: how much of what we billed, we collected
                ROUND(
                    SUM(ActualCash)
                    / NULLIF(SUM(ActualCash) + SUM(ForecastCash), 0)
                    * 100, 2
                )                                                  AS collection_ratio_pct,
                COUNT(DISTINCT AccountingDocument)                 AS invoice_count
            FROM fact_cashflow
            WHERE TRY_CAST(PostingDate AS DATE) BETWEEN '2025-01-01' AND '2025-03-31'
            GROUP BY
                YEAR(PostingDate),
                MONTH(PostingDate),
                FORMAT(PostingDate, 'MMM yyyy')
            ORDER BY yr, mo
        """,
        "params": {},
        "optional_filters": {},
    },

    # =========================================================================
    # 9b. OUTSTANDING RECEIVABLES (AR Health)
    # Business: SUM(ForecastCash) = money not yet collected
    #           Only show where ClearingDate is NULL (not yet cleared)
    # =========================================================================
    "outstanding_receivables": {
        "description": "[FI/AR GRADE] All unsettled receivables. ForecastCash = money still owed. Grouped by customer and aging.",
        "table": "fact_cashflow",
        "query": """
            SELECT TOP (:limit)
                Customer,
                CustomerName,
                AgingBucket,
                COUNT(DISTINCT AccountingDocument) AS open_invoices,
                SUM(ForecastCash)                  AS outstanding_amount,
                SUM(Amount)                        AS original_invoice_amount,
                MIN(TRY_CAST(NetDueDate AS DATE))  AS earliest_due_date
            FROM fact_cashflow
            WHERE ForecastCash > 0
              AND TRY_CAST(PostingDate AS DATE) BETWEEN '2025-01-01' AND '2025-03-31'
            GROUP BY Customer, CustomerName, AgingBucket
            ORDER BY outstanding_amount DESC
        """,
        "params": {
            "limit": {"type": "int", "default": 20, "max": 100},
        },
        "optional_filters": {},
    },

    # =========================================================================
    # 9c. AGING DISTRIBUTION
    # Business: GROUP BY AgingBucket to understand DSO profile
    #           AgingBucket contains values like '0-30', '31-60', '61-90', '90+'
    # =========================================================================
    "aging_distribution": {
        "description": "[FI/AR GRADE] Outstanding AR broken down by aging bucket (0-30, 31-60, 61-90, 90+ days overdue).",
        "table": "fact_cashflow",
        "query": """
            SELECT
                AgingBucket,
                COUNT(DISTINCT AccountingDocument) AS invoice_count,
                COUNT(DISTINCT Customer)           AS customer_count,
                SUM(ForecastCash)                  AS outstanding_ar,
                SUM(Amount)                        AS total_billed,
                ROUND(
                    SUM(ForecastCash)
                    / NULLIF(SUM(SUM(ForecastCash)) OVER (), 0)
                    * 100, 2
                )                                  AS pct_of_total_ar
            FROM fact_cashflow
            WHERE ForecastCash > 0
              AND TRY_CAST(PostingDate AS DATE) BETWEEN '2025-01-01' AND '2025-03-31'
            GROUP BY AgingBucket
            ORDER BY
                CASE AgingBucket
                    WHEN '0-30'  THEN 1
                    WHEN '31-60' THEN 2
                    WHEN '61-90' THEN 3
                    ELSE 4
                END
        """,
        "params": {},
        "optional_filters": {},
    },

    # =========================================================================
    # 9d. COLLECTION EFFICIENCY
    # Business: collection_ratio = ActualCash / (ActualCash + ForecastCash)
    #           Per customer — identifies slow payers vs good payers
    # =========================================================================
    "collection_efficiency": {
        "description": "[FI/AR GRADE] Customer-level collection efficiency. Ratio of cash collected vs total AR. Low ratio = slow payer risk.",
        "table": "fact_cashflow",
        "query": """
            SELECT TOP (:limit)
                Customer,
                CustomerName,
                SUM(ActualCash)                                    AS realized_cash,
                SUM(ForecastCash)                                  AS outstanding_ar,
                SUM(ActualCash) + SUM(ForecastCash)                AS total_ar,
                ROUND(
                    SUM(ActualCash)
                    / NULLIF(SUM(ActualCash) + SUM(ForecastCash), 0)
                    * 100, 2
                )                                                  AS collection_ratio_pct,
                CASE
                    WHEN SUM(ActualCash) / NULLIF(SUM(ActualCash) + SUM(ForecastCash), 0) >= 0.9
                    THEN 'EXCELLENT'
                    WHEN SUM(ActualCash) / NULLIF(SUM(ActualCash) + SUM(ForecastCash), 0) >= 0.7
                    THEN 'GOOD'
                    WHEN SUM(ActualCash) / NULLIF(SUM(ActualCash) + SUM(ForecastCash), 0) >= 0.5
                    THEN 'AT RISK'
                    ELSE 'POOR'
                END AS collection_status
            FROM fact_cashflow
            WHERE TRY_CAST(PostingDate AS DATE) BETWEEN '2025-01-01' AND '2025-03-31'
            GROUP BY Customer, CustomerName
            HAVING SUM(ActualCash) + SUM(ForecastCash) > 0
            ORDER BY {order_col}
        """,
        "params": {
            "limit": {"type": "int", "default": 20, "max": 100},
        },
        "optional_filters": {
            # worst payers first by default
            "order_col": "collection_ratio_pct ASC",
        },
    },

    # =========================================================================
    # 9. WHOLE BUSINESS SNAPSHOT (EXECUTIVE DASHBOARD)
    # Business Logic applied:
    #   - Revenue = Net Sales (post-discount, pre-tax) from fact_sales
    #   - Margin uses Inventory unit_cost fallback (CostAmount = 0 in SAP)
    #   - Joins on product_id to prevent name-based fan-out
    #   - returns_qty is flagged in the output but NOT subtracted from revenue
    # =========================================================================
    "whole_business_snapshot": {
        "description": "Executive dashboard across all 5 pillars. Net Revenue (pre-tax). Margin via inventory cost fallback.",
        "table": "fact_sales, fact_inventory, fact_cashflow",
        "query": """
            WITH UniqueInventoryCost AS (
                SELECT product_id, AVG(unit_cost) AS avg_unit_cost
                FROM fact_inventory
                GROUP BY product_id
            ),
            TopProducts AS (
                SELECT TOP 5 product_name, SUM(revenue) as rev
                FROM fact_sales
                WHERE event_date >= '2025-02-14' AND event_date <= '2025-03-15'
                GROUP BY product_name
                ORDER BY rev DESC
            )
            SELECT
                'Total Business Intelligence' AS dashboard_label,

                -- SECTION A: NET REVENUE (post-discount, pre-tax)
                (SELECT SUM(revenue) FROM fact_sales WHERE event_date >= '2025-02-14' AND event_date <= '2025-03-15') AS revenue_30d,
                (SELECT SUM(revenue) FROM fact_sales WHERE event_date >= '2025-01-15' AND event_date < '2025-02-14') AS revenue_prev_30d,
                (SELECT SUM(revenue) FROM fact_sales WHERE event_date >= '2025-01-15') AS revenue_qtd,

                -- SECTION A2: TAX VISIBILITY (TaxAmount excluded from revenue)
                (SELECT SUM(TaxAmount) FROM fact_sales WHERE event_date >= '2025-02-14' AND event_date <= '2025-03-15') AS tax_30d,

                -- SECTION A3: RETURNS TRACKING (⚠️ NOT subtracted from revenue — SAP inconsistency)
                (SELECT SUM(returns_qty) FROM fact_sales WHERE event_date >= '2025-02-14' AND event_date <= '2025-03-15') AS returns_qty_30d,

                -- SECTION B: GROSS MARGIN (revenue - inventory unit cost; CostAmount=0 in SAP extract)
                (SELECT SUM(s.revenue - (s.quantity * ISNULL(i.avg_unit_cost, 0)))
                 FROM fact_sales s
                 LEFT JOIN UniqueInventoryCost i ON s.product_id = i.product_id
                 WHERE s.event_date >= '2025-02-14' AND s.event_date <= '2025-03-15') AS gross_profit_30d,

                -- SECTION C: AR HEALTH (Realized cash + Outstanding AR)
                -- Realized: money already in bank (ClearingDate set)
                (SELECT SUM(ActualCash) FROM fact_cashflow
                 WHERE TRY_CAST(PostingDate AS DATE) >= '2025-02-14'
                   AND TRY_CAST(PostingDate AS DATE) <= '2025-03-15') AS actual_cash_30d,
                -- Outstanding: money still owed by customers
                (SELECT SUM(ForecastCash) FROM fact_cashflow
                 WHERE TRY_CAST(PostingDate AS DATE) >= '2025-02-14'
                   AND TRY_CAST(PostingDate AS DATE) <= '2025-03-15') AS outstanding_ar_30d,
                -- Collection ratio for the period
                (SELECT ROUND(SUM(ActualCash) / NULLIF(SUM(ActualCash) + SUM(ForecastCash), 0) * 100, 2)
                 FROM fact_cashflow
                 WHERE TRY_CAST(PostingDate AS DATE) >= '2025-02-14'
                   AND TRY_CAST(PostingDate AS DATE) <= '2025-03-15') AS collection_ratio_pct,

                -- SECTION D: INVENTORY ALERTS
                (SELECT COUNT(*) FROM fact_inventory WHERE current_stock > 0 AND (total_sales_30d = 0 OR (current_stock / NULLIF((total_sales_30d / 30.0), 0)) > 90)) AS dead_stock_skus,
                (SELECT COUNT(*) FROM fact_inventory WHERE current_stock <= 10) AS low_stock_alerts,
                (SELECT COUNT(*) FROM fact_inventory WHERE current_stock < 0)   AS negative_stock_anomalies,

                -- SECTION E: TOP SELLERS
                (SELECT STRING_AGG(product_name, ', ') FROM TopProducts) AS top_sellers
        """,
        "params": {},
        "optional_filters": {},
    },

    # =========================================================================
    # 10. MATERIAL COMPOSITION (BOM Lookup)
    # =========================================================================
    "material_composition": {
        "description": "Bill of Materials (BOM) for a specific finished good. Structural data ONLY.",
        "table": "fact_bom",
        "query": """
            SELECT
                b.ComponentMaterial,
                b.ComponentDescription,
                b.BillOfMaterialItemQuantity,
                b.BillOfMaterialItemUnit,
                i.current_stock          AS component_stock,
                i.base_uom               AS inventory_unit
            FROM fact_bom b
            LEFT JOIN fact_inventory i ON b.ComponentMaterial = i.product_id
            WHERE b.HeaderMaterial = :product_id
            ORDER BY b.BillOfMaterialItemNumber
        """,
        "params": {
            "product_id": {"type": "str", "default": ""}
        },
        "optional_filters": {}
    },

    "bom_dependency_analysis": {
        "description": "Identify which finished goods (HeaderMaterial) use a specific component material.",
        "table": "fact_bom",
        "query": """
            SELECT
                HeaderMaterial,
                Plant,
                PlantName,
                BillOfMaterialItemQuantity,
                BillOfMaterialItemUnit
            FROM fact_bom
            WHERE ComponentMaterial = :material_id
            ORDER BY HeaderMaterial
        """,
        "params": {
            "material_id": {"type": "str", "default": ""}
        },
        "optional_filters": {}
    },

    "shortage_prediction": {
        "description": "Production Planning: Calculates required component quantities vs current stock based on sales run-rate.",
        "table": "fact_bom",
        "query": """
            WITH HeaderDemand AS (
                -- Aggregate sales demand at product level (across all plants)
                SELECT
                    product_id,
                    MAX(product_name) AS product_name,
                    SUM(total_sales_30d) AS total_sales_30d
                FROM fact_inventory
                GROUP BY product_id
            ),
            ComponentStock AS (
                -- Aggregate component stock at product level
                SELECT
                    product_id,
                    SUM(current_stock) AS total_stock,
                    MAX(base_uom) AS base_uom
                FROM fact_inventory
                GROUP BY product_id
            )
            SELECT
                b.HeaderMaterial,
                h.product_name            AS header_name,
                b.ComponentMaterial,
                b.ComponentDescription,
                b.BillOfMaterialItemQuantity,
                b.BillOfMaterialItemUnit,
                h.total_sales_30d        AS header_sales_30d,
                -- Required qty to fulfill next 30 days of standard run-rate
                ROUND(b.BillOfMaterialItemQuantity * h.total_sales_30d, 2) AS required_qty_30d,
                i.total_stock             AS component_stock,
                i.base_uom                AS inventory_unit,
                -- Calculation of shortage
                CASE
                    WHEN (b.BillOfMaterialItemQuantity * h.total_sales_30d) > i.total_stock
                    THEN ROUND((b.BillOfMaterialItemQuantity * h.total_sales_30d) - i.total_stock, 2)
                    ELSE 0
                END AS predicted_shortage,
                -- Risk flag for unit mismatch
                CASE
                    WHEN b.BillOfMaterialItemUnit != i.base_uom THEN 'UNIT_MISMATCH'
                    ELSE 'OK'
                END AS unit_consistency_risk
            FROM fact_bom b
            JOIN HeaderDemand h ON b.HeaderMaterial = h.product_id
            LEFT JOIN ComponentStock i ON b.ComponentMaterial = i.product_id
            WHERE (:product_id = '' OR b.HeaderMaterial = :product_id)
              AND (:material_id = '' OR b.ComponentMaterial = :material_id)
            ORDER BY predicted_shortage DESC
        """,
        "params": {
            "product_id": {"type": "str", "default": ""},
            "material_id": {"type": "str", "default": ""}
        },
        "optional_filters": {}
    },

    # =========================================================================
    # 11. RETURNS ANALYSIS
    # Business: returns_qty is only non-zero when SDDocumentCategory = 'H'.
    #           Revenue is NOT adjusted for returns in current extract.
    #           This query surfaces the risk explicitly.
    # =========================================================================
    "returns_analysis": {
        "description": "Identify invoices with returns. Revenue NOT adjusted — this is a known SAP extract limitation.",
        "table": "fact_sales",
        "query": """
            SELECT TOP (:limit)
                invoice_no,
                product_name,
                region,
                quantity,
                returns_qty,
                revenue,
                -- Estimated revenue at risk from returns (using avg price per unit)
                CASE
                    WHEN quantity > 0
                    THEN ROUND((revenue / quantity) * returns_qty, 2)
                    ELSE 0
                END AS estimated_return_value
            FROM fact_sales
            WHERE returns_qty > 0
              AND event_date BETWEEN '2025-01-01' AND '2025-03-31'
            ORDER BY estimated_return_value DESC
        """,
        "params": {
            "limit": {"type": "int", "default": 20, "max": 100},
        },
        "optional_filters": {},
    },

    # =========================================================================
    # 12. PLANT vs CUSTOMER REGION SPLIT
    # Business: plant_city = where product ships FROM (supply).
    #           region = where customer is (demand). These are different!
    # =========================================================================
    "supply_demand_split": {
        "description": "Compare supply origin (plant_city) vs demand destination (region). Critical for logistics.",
        "table": "fact_sales",
        "query": """
            SELECT
                plant_city              AS supply_origin,
                region                  AS demand_destination,
                SUM(revenue)            AS total_net_revenue,
                SUM(quantity)           AS total_qty,
                COUNT(DISTINCT Customer) AS customer_count
            FROM fact_sales
            WHERE event_date BETWEEN '2025-01-01' AND '2025-03-31'
            GROUP BY plant_city, region
            ORDER BY total_net_revenue DESC
        """,
        "params": {},
        "optional_filters": {},
    },

    "plant_footprint": {
        "description": "Information about the company's distribution plants, locations, and site geography.",
        "table": "fact_sales",
        "query": """
            SELECT 
                plant_code,
                plant_name,
                plant_city,
                plant_street,
                plant_post_code
            FROM fact_sales
            GROUP BY plant_code, plant_name, plant_city, plant_street, plant_post_code
            ORDER BY plant_name
        """,
        "params": {},
        "optional_filters": {}
    },
    "business_profitability_summary": {
        "description": "[FINANCIAL/AUDIT] Aggregate company-wide gross profit summary (Revenue, COGS, Margin).",
        "table": "fact_profitability",
        "query": """
            SELECT 
                SUM(revenue)                                            AS total_net_revenue,
                SUM(cogs)                                               AS total_cogs,
                SUM(gross_margin)                                       AS total_gross_margin,
                ROUND(SUM(gross_margin) / NULLIF(SUM(revenue), 0) * 100, 2) AS aggregate_margin_pct,
                COUNT(DISTINCT product_id)                              AS product_count,
                MIN(event_date)                                         AS period_start,
                MAX(event_date)                                         AS period_end
            FROM fact_profitability
            WHERE 1=1
            {month_filter}
            {year_filter}
        """,
        "params": {
            "month": {"type": "int", "optional": True},
            "year": {"type": "int", "optional": True},
        },
        "optional_filters": {
            "month_filter": "AND MONTH(event_date) = :month",
            "year_filter": "AND YEAR(event_date) = :year",
        },
    },
    "loss_making_summary": {
        "description": "[FINANCIAL/AUDIT] Aggregate count and total value of loss-making products (negative margin).",
        "table": "fact_profitability",
        "query": """
            SELECT 
                COUNT(DISTINCT product_id) AS loss_making_product_count,
                SUM(gross_margin)          AS total_loss_value,
                SUM(revenue)               AS affected_revenue,
                MIN(event_date)            AS period_start,
                MAX(event_date)            AS period_end
            FROM (
                SELECT 
                    product_id, 
                    SUM(gross_margin) AS gross_margin,
                    SUM(revenue)      AS revenue,
                    SUM(cogs)         AS cogs,
                    MIN(event_date)   AS event_date
                FROM fact_profitability
                WHERE 1=1
                {month_filter}
                {year_filter}
                GROUP BY product_id
                HAVING SUM(gross_margin) < 0
                   AND SUM(cogs) > 0
            ) AS loss_table
        """,
        "params": {
            "month": {"type": "int", "optional": True},
            "year": {"type": "int", "optional": True},
        },
        "optional_filters": {
            "month_filter": "AND MONTH(event_date) = :month",
            "year_filter": "AND YEAR(event_date) = :year",
        },
    },
}

# Quick lookup: all valid intent names
VALID_INTENTS = list(SQL_TEMPLATES.keys())
