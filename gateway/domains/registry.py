"""
Domain Registry — The Master Router
===================================
Orchestrates multiple datasets (Sales, Inventory, Cashflow, etc.)
"""
import logging
from . import sales

logger = logging.getLogger(__name__)

# REGISTER NEW DOMAINS HERE
DOMAINS = {
    "sales": sales,
    # "inventory": inventory,
    # "cashflow": cashflow,
}

def route_query(query: str):
    """
    Search all registered domains for a matching intent.
    Returns: { "domain": "sales", "intent": "total_revenue", "params": {} }
    """
    for domain_name, domain_module in DOMAINS.items():
        result = domain_module.get_intent(query)
        if result["intent"] != "unknown":
            result["domain"] = domain_name
            return result
            
    return {"intent": "unknown", "domain": None, "params": {}}

def get_template(domain: str, intent: str):
    """Fetch the SQL template for a specific domain/intent pair."""
    if domain in DOMAINS:
        return DOMAINS[domain].SQL_TEMPLATES.get(intent)
    return None

def get_all_templates():
    """Aggregates all templates from all domains."""
    all_tmpl = {}
    for domain_name, domain_module in DOMAINS.items():
        all_tmpl.update(domain_module.SQL_TEMPLATES)
    return all_tmpl
