# Intent Mapping for Pure Sales Mode (V2 - Ground Truth Protocol V3)
import logging
logger = logging.getLogger(__name__)

# Priority: Higher number = Checked first
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

INTENT_MAP = {
    "total_revenue": [
        "total revenue", "overall sales", "how much did we sell", "total sales",
        "company revenue", "grand total revenue", "revenue", "sales", "show revenue", "total"
    ],
    "total_quantity": [
        "total quantity", "how many units", "total volume", "bill qty total", "quantity total"
    ],
    "revenue_by_customer": [
        "revenue by customer", "customer wise sales", "customer sales", "which customer spent most"
    ],
    "revenue_by_product": [
        "revenue by product", "product wise sales", "product sales", "sales by material"
    ],
    "customer_product_revenue": [
        "what did customer buy", "customer product breakdown", "drilldown", 
        "product breakdown for", "sales for customer"
    ],
    "product_price_analysis": [
        "product price analysis", "price per unit", "average price", "derived price", 
        "price behavior of product", "product value"
    ],
    "customer_price_analysis": [
        "customer price analysis", "how much customer pays", "customer price behavior"
    ],
    "top_products": [
        "top products", "top selling products", "best sellers", "product ranking", "top 10 products"
    ],
    "top_customers": [
        "top customers", "best customers", "customer ranking", "top 10 customers"
    ],
    "transaction_view": [
        "transaction view", "raw sales", "audit view", "show me data", "list transactions", "show transactions"
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
            "message": "Query not supported. Ask for 'total revenue', 'top products', 'price analysis', or 'transaction view'."
        }
        
    return {
        "intent": intent,
        "params": {}
    }
