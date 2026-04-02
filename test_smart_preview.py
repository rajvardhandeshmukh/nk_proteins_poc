import pandas as pd
from agentic_hub import get_smart_preview

# Mock data
df = pd.DataFrame({
    'invoice_no': ['INV1', 'INV2', 'INV3'],
    'invoice_date': ['2023-01-01', '2023-01-02', '2023-01-03'],
    'customer_name': ['A', 'B', 'C'],
    'invoice_amount': [100, 200, 300],
    'days_overdue': [0, 50, 10], # INV2 is most overdue
    'aging_bucket': ['Closed', '60-90', '1-30'],
    'internal_ml_feature': [0.1, 0.2, 0.3] # Should be filtered out
})

print("--- Testing Cashflow Anomaly Sorting ---")
preview = get_smart_preview(df, 'cashflow', 'anomaly_detection')
print(preview)

assert preview.iloc[0]['invoice_no'] == 'INV2', "Should sort by days_overdue DESC"
assert 'internal_ml_feature' not in preview.columns, "Should filter out non-UI columns"
print("\nSUCCESS: Smart Preview working as expected.")
