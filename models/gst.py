import pandas as pd
from sklearn.ensemble import IsolationForest
import warnings
warnings.filterwarnings('ignore')

GST_FILE = 'data/nk_gst_data_2022_2026_feb.csv'

def run_gst():
    df = pd.read_csv(GST_FILE)

    mismatches = df[df['mismatch_flag'] == 1]

    type_breakdown = df[df['mismatch_flag']==1]['mismatch_reason'].value_counts().to_dict()

    iso_feats = df[['taxable_value', 'cgst_amount', 'sgst_amount', 'total_tax_amount']]
    iso = IsolationForest(contamination=0.12, random_state=42)
    df['iso_flag'] = iso.fit_predict(iso_feats)
    iso_mismatches = int((df['iso_flag'] == -1).sum())

    supplier_risk = (mismatches.groupby(['counterparty_id','counterparty_name'])
                               .agg(invoice_count=('invoice_no','count'),
                                    total_itc_risk=('total_tax_amount','sum'))
                               .sort_values('total_itc_risk', ascending=False)
                               .head(5)
                               .reset_index()
                               .round(2)
                               .to_dict('records'))

    monthly = (df.groupby('return_period')['mismatch_flag']
                 .sum()
                 .tail(6)
                 .to_dict())

    return {
        "module":             "gst",
        "total_invoices":     int(len(df)),
        "total_mismatches":   int(len(mismatches)),
        "mismatch_pct":       round(len(mismatches)/len(df)*100, 1),
        "total_itc_at_risk":  int(mismatches['total_tax_amount'].sum()),
        "isolation_forest_flags": iso_mismatches,
        "mismatch_type_breakdown": type_breakdown,
        "top_supplier_risks": supplier_risk,
        "monthly_mismatch_trend": monthly,
    }
