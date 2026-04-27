import sys
import os
import json
from fastapi.testclient import TestClient

# Add current directory to path
sys.path.append(os.getcwd())

from gateway.main import app

client = TestClient(app)

queries = [
    "What is our profitability margin?",
    "Show revenue by region",
    "Top products by revenue and quantity"
]

for q in queries:
    print(f"\nTESTING QUERY: {q}")
    response = client.post(
        "/query",
        json={"query": q, "data_source_verification": "ERP_V3"},
        headers={"x-api-key": "nk-secret-key"}
    )
    print(f"Status Code: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    print("-" * 40)
