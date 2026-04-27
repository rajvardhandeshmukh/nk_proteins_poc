from gateway.mapping_v2 import map_intent, get_intent

queries = [
    "What is the company's profitability margin for the latest fiscal year, including gross margin, operating margin, and net profit margin?"
]

for q in queries:
    intent = map_intent(q)
    print(f"Query: '{q}'\nIntent: {intent}")
    full = get_intent(q)
    print(f"Full Result: {full}")
