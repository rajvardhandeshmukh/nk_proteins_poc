# Intent Mapping for Pure Sales Mode (V2 - Ground Truth Protocol V3)
import logging
logger = logging.getLogger(__name__)

# Priority: Drilldowns and Analysis first
INTENT_PRIORITY = {
    "customer_product_revenue": 10,
    "product_price_analysis": 8,
    "customer_price_analysis": 8,
    "transaction_view": 7,
    "top_products": 5,
    "top_customers": 5,
    "revenue_by_customer": 4,
    "revenue_by_product": 4,
    "total_quantity": 2,
    "total_revenue": 1,
}

# EXACT USER MAPPING V3
INTENT_MAP = {
    "total_revenue": [
        "total revenue", "total sales", "overall revenue"
    ],
    "total_quantity": [
        "total quantity", "total qty"
    ],
    "revenue_by_customer": [
        "sales by customer", "revenue by customer"
    ],
    "revenue_by_product": [
        "sales by product", "revenue by product"
    ],
    "customer_product_revenue": [
        "customer product sales", "product by customer"
    ],
    "product_price_analysis": [
        "product price", "price of product", "product pricing"
    ],
    "customer_price_analysis": [
        "customer price", "price per customer"
    ],
    "top_products": [
        "top products", "best products"
    ],
    "top_customers": [
        "top customers", "best customers"
    ],
    "transaction_view": [
        "show transactions", "raw data"
    ]
}

def map_intent(query):
    query = query.lower()
    
    matches = []
    for intent, keywords in INTENT_MAP.items():
        priority = INTENT_PRIORITY.get(intent, 0)
        for keyword in keywords:
            if keyword in query:
                matches.append((priority, len(keyword), intent))
    
    if not matches:
        return "unknown"
        
    # Sort by priority, then by keyword length to get most specific match
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
            "message": "Query not supported. Try 'total sales', 'product price', or 'raw data'."
        }
        
    return {
        "intent": intent,
        "params": {}
    }
