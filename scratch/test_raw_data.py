import sys
import os
import json
from fastapi.testclient import TestClient

sys.path.append(os.getcwd())
from gateway.main import app

client = TestClient(app)

print("\nTESTING RAW TRANSACTION DATA QUERY")
response = client.post(
    "/query",
    json={"query": "show transactions", "data_source_verification": "ERP_V3"},
    headers={"x-api-key": "nk-secret-key", "ngrok-skip-browser-warning": "true"}
)
print(f"Status Code: {response.status_code}")
print(f"Response: {json.dumps(response.json(), indent=2)}")
