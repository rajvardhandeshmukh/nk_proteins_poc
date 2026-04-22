# Intent Mapping for Pure Sales Mode (V2)

INTENT_MAP = {
    "total_revenue": [
        "total revenue", "overall revenue", "how much revenue", 
        "total sales", "sum of sales", "net amount total"
    ],
    "revenue_by_region": [
        "total revenue broken down by region", "revenue broken down by region",
        "revenue by region", "sales by region", "regional sales",
        "which region has most revenue", "demand by region", "customer region",
        "region", "by region", "region-wise"
    ],
    "revenue_by_plant": [
        "revenue by plant", "sales by plant", "supply point sales",
        "plant city revenue", "fulfillment revenue",
        "plant", "by plant", "plant-wise"
    ],
    "top_products_overall": [
        "top products overall", "global top products", "best sellers total",
        "top revenue products", "all units top products"
    ],
    "top_products_revenue_unit_safe": [
        "top products", "best selling products", "most revenue product",
        "product ranking", "top selling materials", "top sellers", "top materials"
    ],
    "product_performance_detailed": [
        "product performance", "detailed product sales", "product quantity",
        "sales by material", "product analysis"
    ],
    "profitability_all": [
        "total profit", "overall margin", "gross margin all",
        "how much profit", "total gross margin"
    ],
    "profitability_valid": [
        "valid profit", "true margin", "actual profitability",
        "gross margin valid", "profit where cost exists"
    ],
    "monthly_revenue_trend": [
        "monthly revenue trend", "monthly sales revenue", "monthly trend",
        "revenue over time", "sales by month", "monthly growth", "trend analysis"
    ],
    "daily_revenue_trend": [
        "daily trend", "day wise sales", "daily revenue",
        "sales today", "trend by day"
    ],
    "revenue_by_region_product": [
        "revenue by region and product", "region wise product sales",
        "product sales by region", "material by region"
    ]
}

def map_intent(query):
    query = query.lower()
    
    matches = []
    for intent, keywords in INTENT_MAP.items():
        for keyword in keywords:
            if keyword in query:
                # Store match with its length for prioritization
                matches.append((len(keyword), intent))
    
    if not matches:
        return None
        
    # Sort matches by keyword length (descending) to get the most specific one
    matches.sort(key=lambda x: x[0], reverse=True)
    return matches[0][1]

def get_intent(query):
    """
    Main entry point for V2 bypass.
    Returns: {"intent": str, "params": dict} or None
    """
    intent = map_intent(query)
    if intent:
        return {
            "intent": intent,
            "params": {} # V2 Pure Math uses fixed templates, so params are usually empty
        }
    return None
