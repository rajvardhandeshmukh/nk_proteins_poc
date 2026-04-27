from gateway.mapping_v2 import map_intent, get_intent

queries = [
    "What is the current profitability margin for the company overall?",
    "profitability margin",
    "Top products by revenue and quantity",
    "What is our profitability margin?"
]

for q in queries:
    intent = map_intent(q)
    print(f"Query: '{q}' -> Intent: {intent}")
    full = get_intent(q)
    print(f"Full Result: {full}")
    print("-" * 20)
