# Intent Mapping for Pure Sales Mode (V2)

# Priority 3: Combined / Complex Intents
# Priority 2: Detail / Breakdown Intents
# Priority 1: High-level / Summary Intents
INTENT_PRIORITY = {
    "revenue_by_region": 15,
    "profitability_all": 9,
    "profitability_valid": 8,
    "revenue_by_region_product": 10,
    "sales_office_customer_revenue": 6,
    "top_products_overall": 5,
    "top_products_revenue_unit_safe": 5,
    "revenue_by_plant": 4,
    "monthly_revenue_trend": 2,
    "daily_revenue_trend": 2,
    "list_sales_offices": 2,
    "total_revenue": 1,
}

# Recognized Sales Offices for parameter extraction
SALES_OFFICES = [
    "Ahmedabad", "Ahmedabad City", "Akola", "Amreli", "Amritsar", "Anand", "Bananskantha", 
    "Bareilly", "Baroda", "Baroda City", "Bharuch", "Bharuch City", "Bhavnagar", "Centre - 5", 
    "Chittorgarh", "Dahod", "Delhi", "FMCG Sales office", "Gandhinagar", "Haryana", 
    "Himachal Pradesh", "Indore", "Jaipur", "Jamnagar", "Jamnagar City", "Junagadh", 
    "Junagadh City", "Kheda", "Kota", "Kutch-Bhuj", "Madhya Pradesh", "Maharastra", 
    "Mehsana", "Modern Trade", "Mumbai", "Narmada", "Navsari", "NKPL Head Office", 
    "NKRL Head Office", "Out-State", "Pnachmahal", "Patan", "Porbandar", "Rajasthan", 
    "Rajkot", "Rajkot City", "Rajsamand", "Retail", "Sabarkantha", "Surat", "Surat City", 
    "Surendra Nagar", "Udaipur", "Valsad"
]

INTENT_MAP = {
    "total_revenue": [
        "total revenue", "overall sales", "how much did we sell", "total sales",
        "company revenue", "net amount total", "grand total revenue"
    ],
    "revenue_by_region": [
        "revenue by region", "sales by region", "regional sales", "region wise",
        "which region has most revenue", "demand by region", "customer region",
        "nk proteins regional revenue", "sap region sales", "ground truth region sales",
        "actual region revenue", "real sales by region", "only region revenue",
        "strictly regional sales"
    ],
    "revenue_by_plant": [
        "total revenue broken down by plant", "revenue broken down by plant",
        "revenue by plant", "sales by plant", "supply point sales",
        "plant city revenue", "fulfillment revenue"
    ],
    "top_products_overall": [
        "top products overall", "global top products", "best sellers overall",
        "most revenue products global"
    ],
    "top_products_revenue_unit_safe": [
        "top products", "best selling products", "most revenue product",
        "product ranking", "top selling materials", "top sellers", "top materials",
        "top 10 products", "top 10 products by revenue", "highest revenue items"
    ],
    "revenue_by_region_product": [
        "top products by region", "best products in each region",
        "region wise top products"
    ],
    "product_performance_detailed": [
        "product performance", "detailed product sales", "product quantity",
        "sales by material", "product analysis", "quantity", "qty"
    ],
    "profitability_all": [
        "profitability margin", "profitability", "margin", 
        "overall margin", "estimated profit", "theoretical margin",
        "how much profit", "profit summary", "company margin",
        "gross profit", "net profit", "profit amount", "profit"
    ],
    "profitability_valid": [
        "total profit", "real profit", "profit where cost exists", 
        "actual margin", "gross margin valid", "verified margin", "audited profit"
    ],
    "monthly_revenue_trend": [
        "monthly revenue trend", "monthly sales revenue", "monthly trend",
        "revenue over time", "sales by month", "monthly growth"
    ],
    "daily_revenue_trend": [
        "daily trend", "day wise sales", "daily revenue", "revenue per day"
    ],
    
    "sales_office_customer_revenue": [
        "sales office sold to which customers",
        "customers for sales office",
        "who did sales office sell to",
        "sales office customer revenue",
        "customers and revenue for sales office",
        "sales office", "office revenue"
    ],

    "list_sales_offices": [
        "list sales offices",
        "all sales offices",
        "show sales office names",
        "available sales offices",
        "which offices", "list offices"
    ],
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
    Main entry point for V2 bypass. Extracts intent and parameters.
    """
    intent = map_intent(query)
    
    if intent == "unknown":
        return {
            "intent": "unknown",
            "params": {},
            "message": "Query not supported in V2 templates. Please ask about total revenue, regions, top products, or sales offices."
        }
        
    params = {}
    
    # PARAMETER EXTRACTION: Sales Office
    if intent == "sales_office_customer_revenue":
        # Sort offices by length descending to match most specific first (e.g. 'Ahmedabad City' vs 'Ahmedabad')
        sorted_offices = sorted(SALES_OFFICES, key=len, reverse=True)
        # Look for recognized office names in the query
        for office in sorted_offices:
            if office.lower() in query.lower():
                params["sales_office"] = office
                break
        
        # Fallback: if no office mentioned, switch to list_sales_offices to help user
        if "sales_office" not in params:
            return {
                "intent": "list_sales_offices",
                "params": {},
                "message": "You didn't specify a sales office. Here are the available offices to choose from:"
            }

    return {
        "intent": intent,
        "params": params
    }
