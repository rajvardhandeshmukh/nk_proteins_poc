import requests
import json

url = "http://127.0.0.1:8003/query"
headers = {"x-api-key": "nk-secret-key"}
payload = {
    "query": "What is our profitability margin?",
    "data_source_verification": "ERP_V3"
}

try:
    response = requests.post(url, json=payload, headers=headers)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
except Exception as e:
    print(f"Error: {e}")
