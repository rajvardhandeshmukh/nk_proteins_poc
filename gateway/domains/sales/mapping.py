# Intent Mapping for Sales Domain
# Maps natural language triggers -> SQL template keys
import logging
logger = logging.getLogger(__name__)

# Higher number = matched first when multiple intents trigger
INTENT_PRIORITY = {
    "customer_product_revenue": 10,
    "product_by_plant":          9,
    "revenue_by_sales_office":   8,
    "revenue_by_region":         8,
    "revenue_by_sales_org":      8,
    "daily_revenue_trend":       7,
    "top_products":              6,
    "top_customers":             6,
    "revenue_by_customer":       4,
    "revenue_by_product":        4,
    "total_revenue":             1,
}

INTENT_MAP = {
    "total_revenue": [
        "total revenue", "total sales", "overall revenue", "how much revenue",
        "total amount", "overall sales",
    ],
    "revenue_by_region": [
        "revenue by region", "sales by region", "region breakdown",
        "which region", "region wise", "regionwise", "region performance",
    ],
    "revenue_by_customer": [
        "revenue by customer", "sales by customer", "customer revenue",
        "customer breakdown", "customer wise", "customerwise",
    ],
    "revenue_by_product": [
        "revenue by product", "sales by product", "product revenue",
        "product breakdown", "product wise", "productwise",
    ],
    "product_by_plant": [
        "product by plant", "plant performance", "sales by plant",
        "which plant", "plant wise", "plantwise", "plant breakdown",
    ],
    "revenue_by_sales_org": [
        "sales org", "sales organization", "by sales org",
        "which sales org", "sales org breakdown",
    ],
    "revenue_by_sales_office": [
        "sales office", "by sales office", "office wise", "officewise",
        "sales office breakdown", "which sales office", "office performance",
    ],
    "customer_product_revenue": [
        "customer product", "product by customer", "customer and product",
        "which customer buys", "what does customer buy",
    ],
    "daily_revenue_trend": [
        "daily revenue", "revenue trend", "daily trend", "day by day",
        "revenue by day", "daily sales", "trend over time",
    ],
    "top_products": [
        "top products", "best products", "top selling products",
        "highest revenue products", "most sold",
    ],
    "top_customers": [
        "top customers", "best customers", "highest revenue customers",
        "biggest customers", "top buyers",
    ],
}


def map_intent(query: str) -> str:
    query = query.lower()
    matches = []
    for intent, keywords in INTENT_MAP.items():
        priority = INTENT_PRIORITY.get(intent, 0)
        for keyword in keywords:
            if keyword in query:
                matches.append((priority, len(keyword), intent))

    if not matches:
        return "unknown"

    matches.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return matches[0][2]


def get_intent(query: str) -> dict:
    intent = map_intent(query)
    if intent == "unknown":
        return {
            "intent": "unknown",
            "params": {},
            "message": (
                "Query not matched. Try: 'revenue by region', 'top products', "
                "'daily revenue trend', 'customer product', 'product by plant'."
            ),
        }
    return {"intent": intent, "params": {}}
