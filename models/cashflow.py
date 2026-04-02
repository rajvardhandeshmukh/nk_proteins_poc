import pandas as pd
import warnings
warnings.filterwarnings('ignore')

AR_FILE = 'data/nk_receivables_2022_2026_feb.csv'

def run_cashflow(df=None):
    if df is None:
        df = pd.read_csv(AR_FILE, parse_dates=['invoice_date',
                                               'due_date','received_date'])

    prob = {0: 0.98, 1: 0.92, 2: 0.70, 3: 0.45, 4: 0.20}
    df['bucket']   = pd.cut(df['days_overdue'],
                            bins=[-999,0,30,60,90,9999],
                            labels=[0,1,2,3,4]).astype(float)
    df['cprob']    = df['bucket'].map(prob)
    df['exp_inflow']= df['invoice_amount'] * df['cprob']

    overdue = df[df['days_overdue'] > 0]

    slow_payers = (overdue.groupby(['customer_id','customer_name'])
                          .agg(total_overdue=('invoice_amount','sum'),
                               invoice_count=('invoice_no','count'),
                               avg_days_late=('days_overdue','mean'))
                          .sort_values('total_overdue', ascending=False)
                          .head(5)
                          .reset_index()
                          .round(2)
                          .to_dict('records'))

    status_summary = df['aging_bucket'].value_counts().to_dict()

    region_overdue = (overdue.groupby('region')['invoice_amount']
                             .sum()
                             .sort_values(ascending=False)
                             .round(2)
                             .to_dict())

    df['payment_days'] = (df['received_date'] - df['invoice_date']).dt.days
    dso = round(df['payment_days'].dropna().mean(), 1)

    bad_debt = df[df['aging_bucket'] == 'Bad Debt']['invoice_amount'].sum()

    return {
        "module":             "cash_flow",
        "total_overdue":      int(overdue['invoice_amount'].sum()),
        "expected_30d_inflow":int(df['exp_inflow'].sum()),
        "overdue_invoice_count": int(len(overdue)),
        "slow_payer_count":   int(overdue['customer_id'].nunique()),
        "top_slow_payers":    slow_payers,
        "dso_days":           dso,
        "bad_debt_amount":    int(bad_debt),
        "status_breakdown":   status_summary,
        "region_overdue":     region_overdue,
    }
