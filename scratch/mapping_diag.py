from gateway.mapping_v2 import get_intent
from gateway.sql_templates_v2 import SQL_TEMPLATES

queries = [
    "What is our profitability margin?",
    "Show profit summary",
    "Actual margin where cost exists"
]

print("--- DIAGNOSTIC START ---")
for q in queries:
    res = get_intent(q)
    intent = res["intent"]
    print(f"Query: '{q}' -> Intent: '{intent}'")
    if intent in SQL_TEMPLATES:
        print(f"  [OK] Found in SQL_TEMPLATES")
    else:
        print(f"  [ERROR] NOT FOUND in SQL_TEMPLATES. Available: {list(SQL_TEMPLATES.keys())}")
print("--- DIAGNOSTIC END ---")
