"""
Simplified V2 Mapping - Keyword & Fuzzy logic for the POC.
Maps user questions to sql_templates_v2.py intents.
"""

# Simple Keyword to Intent Mapping
# We will expand this as we add new queries.
KEYWORDS_V2 = {
    "total revenue": "total_revenue_overall",
    "overall revenue": "total_revenue_overall",
    "revenue by region": "revenue_by_region",
    "regional revenue": "revenue_by_region",
    "region wise revenue": "revenue_by_region"
}

def get_intent(user_query: str) -> dict:
    """
    Very simple keyword matching for the POC.
    Returns a dict with intent and empty params if matched.
    """
    query_clean = user_query.lower().strip()
    
    # Direct keyword search
    for keyword, intent in KEYWORDS_V2.items():
        if keyword in query_clean:
            return {"intent": intent, "params": {}}
            
    return None
