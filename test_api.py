import requests
import json

base_url = "http://127.0.0.1:8000"
headers = {
    "Content-Type": "application/json",
    "x-api-key": "nk-secret-key"
}

def test_query(intent, params={}):
    print(f"\n--- Testing Intent: {intent} ---")
    payload = {
        "intent": intent,
        "params": params
    }
    response = requests.post(f"{base_url}/execute_query", headers=headers, json=payload)
    if response.status_code == 200:
        data = response.json()
        print(f"Status: {data.get('status', 'N/A')}")
        print(f"Message: {data.get('message', 'N/A')}")
        print(f"Row count: {data.get('row_count', 0)}")
        if data.get('data'):
            print("First row:", data['data'][0])
    else:
        print(f"Error {response.status_code}: {response.text}")

if __name__ == "__main__":
    test_query("top_profitable_products", {"limit": 100})
    test_query("inventory_health", {"limit": 1})
    test_query("revenue_trend", {"limit": 1})
