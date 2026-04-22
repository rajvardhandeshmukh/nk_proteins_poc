# Intent Mapping for Pure Sales Mode (V2)

# Priority 3: Combined / Complex Intents
# Priority 2: Detail / Breakdown Intents
# Priority 1: High-level / Summary Intents
INTENT_PRIORITY = {
    "revenue_by_region_product": 10,
    "top_products_overall": 5,
    "top_products_revenue_unit_safe": 5,
    "revenue_by_region": 4,
    "revenue_by_plant": 4,
    "monthly_revenue_trend": 2,
    "daily_revenue_trend": 2,
    "total_revenue": 1,
    "profitability_valid": 1,
    "profitability_all": 0
}

INTENT_MAP = {
    "total_revenue": [
        "total revenue", "overall revenue", "how much revenue", 
        "total sales", "sum of sales", "net amount total"
    ],
    "revenue_by_region": [
        "total revenue broken down by region", "revenue broken down by region",
        "revenue by region", "sales by region", "regional sales",
        "which region has most revenue", "demand by region", "customer region"
    ],
    "revenue_by_plant": [
        "revenue by plant", "sales by plant", "supply point sales",
        "plant city revenue", "fulfillment revenue"
    ],
    "top_products_overall": [
        "top products overall", "global top products", "best sellers total",
        "top revenue products", "all units top products"
    ],
    "top_products_revenue_unit_safe": [
        "top products", "best selling products", "most revenue product",
        "product ranking", "top selling materials", "top sellers", "top materials"
    ],
    "revenue_by_region_product": [
        "top products by region", "best products in each region", 
        "region wise top products", "products per region", "regional top products"
    ],
    "product_performance_detailed": [
        "product performance", "detailed product sales", "product quantity",
        "sales by material", "product analysis"
    ],
    "profitability_all": [
        "overall margin", "estimated profit", "theoretical margin"
    ],
    "profitability_valid": [
        "total profit", "real profit", "profit where cost exists", 
        "actual margin", "gross margin valid"
    ],
    "monthly_revenue_trend": [
        "monthly revenue trend", "monthly sales revenue", "monthly trend",
        "revenue over time", "sales by month", "monthly growth"
    ],
    "daily_revenue_trend": [
        "daily trend", "day wise sales", "daily revenue", "revenue per day"
    ]
}

def map_intent(query):
    query = query.lower()
    
    matches = []
    for intent, keywords in INTENT_MAP.items():
        priority = INTENT_PRIORITY.get(intent, 0)
        for keyword in keywords:
            if keyword in query:
                # Store (Priority, Length, Intent)
                matches.append((priority, len(keyword), intent))
    
    if not matches:
        return "unknown"
        
    # Sort by Priority (Primary) and Length (Secondary)
    matches.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return matches[0][2]

def get_intent(query):
    """
    Main entry point for V2 bypass.
    """
    intent = map_intent(query)
    
    if intent == "unknown":
        return {
            "intent": "unknown",
            "params": {},
            "message": "Query not supported in V2 templates. Please ask about total revenue, regions, or top products."
        }
        
    return {
        "intent": intent,
        "params": {}
    }
