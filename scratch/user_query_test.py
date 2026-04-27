import requests
import json

url = "http://127.0.0.1:8004/query"
headers = {"x-api-key": "nk-secret-key"}

test_queries = [
    "profitability margin",
    "Actual margin where cost exists",
    "total cost of goods sold"
]

for query in test_queries:
    print(f"\nTesting Query: '{query}'")
    payload = {
        "query": query,
        "data_source_verification": "ERP_V3"
    }
    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            data = response.json()
            intent = data.get("plan", {}).get("intent", "unknown")
            status = data.get("data", {}).get("status", "error")
            print(f"  -> Intent: {intent}")
            print(f"  -> Status: {status}")
            if status == "error":
                print(f"  -> ERROR: {data.get('data', {}).get('message')}")
        else:
            print(f"  -> HTTP ERROR: {response.status_code}")
    except Exception as e:
        print(f"  -> Connection Error: {e}")
