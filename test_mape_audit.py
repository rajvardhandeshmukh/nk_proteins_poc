"""
MAPE Audit: Measure actual forecast accuracy on held-out data.
This uses the REAL dataset, not mocked data.
"""
import sys, io
sys.stdout = open('d:/projects/nk_protein_poc/mape_audit_results.txt', 'w', encoding='utf-8')

import pandas as pd
import numpy as np
import xgboost as xgb
from prophet import Prophet
from sklearn.metrics import mean_absolute_percentage_error as mape_score
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# ── Load real data from DB ──
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv
load_dotenv()

server = os.getenv("MSSQL_SERVER", "localhost")
database = os.getenv("MSSQL_DATABASE", "nk_proteins")
user = os.getenv("MSSQL_USER", "sa")
password = os.getenv("MSSQL_PASS", "Admin@12345")
port = os.getenv("MSSQL_PORT", "1433")
encoded_pass = quote_plus(password)
engine = create_engine(f"mssql+pymssql://{user}:{encoded_pass}@{server}:{port}/{database}")

print("=" * 70)
print("MAPE AUDIT — NK Proteins Sales Forecasting Model")
print("=" * 70)

with engine.connect() as conn:
    df = pd.read_sql("""
        SELECT date, product_id, product_name, region, quantity_sold, revenue, 
               margin_pct, discount_pct, returns_qty, is_festive 
        FROM fact_sales 
        WHERE date >= DATEADD(month, -36, GETDATE())
        ORDER BY date ASC
    """, conn)

print(f"\nDataset: {len(df)} rows, {df['date'].nunique()} unique dates")
print(f"Date range: {df['date'].min()} to {df['date'].max()}")
print(f"Unique products: {df['product_id'].nunique()}")
print(f"Unique regions: {df['region'].nunique()}")

# ══════════════════════════════════════════════════════════════════
# 1. XGBoost MAPE (Same methodology as models/sales.py)
# ══════════════════════════════════════════════════════════════════
df = df.sort_values('date')

df_m = (df.groupby(['date','product_id','product_name','region'])
          .agg(revenue=('revenue','sum'),
               quantity=('quantity_sold','sum'),
               avg_margin=('margin_pct','mean'),
               avg_discount=('discount_pct','mean'),
               returns=('returns_qty','sum'),
               is_festive=('is_festive','max'))
          .reset_index())

df_m = df_m.sort_values(['product_id','region','date'])
grp  = df_m.groupby(['product_id','region'])

df_m['lag_1']        = grp['revenue'].shift(1)
df_m['lag_3']        = grp['revenue'].shift(3)
df_m['lag_12']       = grp['revenue'].shift(12)   
df_m['rolling_3']    = grp['revenue'].transform(lambda x: x.rolling(3).mean())
df_m['rolling_std3'] = grp['revenue'].transform(lambda x: x.rolling(3).std())
df_m['month']        = df_m['date'].dt.month
df_m['quarter']      = df_m['date'].dt.quarter
df_m['year']         = df_m['date'].dt.year
df_m = df_m.dropna()

region_map = {r: i for i, r in enumerate(df_m['region'].unique())}
df_m['region_enc'] = df_m['region'].map(region_map)
prod_map = {p: i for i, p in enumerate(df_m['product_id'].unique())}
df_m['product_enc'] = df_m['product_id'].map(prod_map)

feats = ['lag_1','lag_3','lag_12','rolling_3','rolling_std3',
         'month','quarter','year','is_festive',
         'avg_discount','returns','region_enc','product_enc']

# ── Multiple split ratios ──
print("\n" + "─" * 70)
print("XGBoost MAPE at different train/test splits:")
print("─" * 70)

for split_pct in [0.7, 0.75, 0.8, 0.85, 0.9]:
    split = int(len(df_m) * split_pct)
    train, test = df_m.iloc[:split], df_m.iloc[split:]
    
    xm = xgb.XGBRegressor(n_estimators=300, learning_rate=0.05,
                           max_depth=5, subsample=0.8,
                           colsample_bytree=0.8, verbosity=0)
    xm.fit(train[feats], train['revenue'])
    xpred = xm.predict(test[feats])
    xmape = round(mape_score(test['revenue'], xpred) * 100, 2)
    
    print(f"  Split {int(split_pct*100)}/{int((1-split_pct)*100)}: "
          f"Train={len(train)}, Test={len(test)}, MAPE={xmape}%")

# ── The actual split used in production (80/20) ──
split = int(len(df_m) * 0.8)
train, test = df_m.iloc[:split], df_m.iloc[split:]
xm = xgb.XGBRegressor(n_estimators=300, learning_rate=0.05,
                       max_depth=5, subsample=0.8,
                       colsample_bytree=0.8, verbosity=0)
xm.fit(train[feats], train['revenue'])
xpred = xm.predict(test[feats])
production_mape = round(mape_score(test['revenue'], xpred) * 100, 2)

# ── Per-region MAPE ──
print("\n" + "─" * 70)
print("Per-Region MAPE (80/20 split):")
print("─" * 70)

test_with_pred = test.copy()
test_with_pred['predicted'] = xpred

for region in test_with_pred['region'].unique():
    r_df = test_with_pred[test_with_pred['region'] == region]
    r_mape = round(mape_score(r_df['revenue'], r_df['predicted']) * 100, 2)
    print(f"  {region}: MAPE={r_mape}% ({len(r_df)} test rows)")

# ── Per-product MAPE (top 5 by volume) ──
print("\n" + "─" * 70)
print("Per-Product MAPE (top 5 products by test volume):")
print("─" * 70)

prod_counts = test_with_pred['product_name'].value_counts().head(5)
for prod_name in prod_counts.index:
    p_df = test_with_pred[test_with_pred['product_name'] == prod_name]
    p_mape = round(mape_score(p_df['revenue'], p_df['predicted']) * 100, 2)
    print(f"  {prod_name}: MAPE={p_mape}% ({len(p_df)} rows)")

# ══════════════════════════════════════════════════════════════════
# 2. Prophet MAPE (aggregate-level forecasting)
# ══════════════════════════════════════════════════════════════════
print("\n" + "─" * 70)
print("Prophet MAPE (aggregate monthly revenue):")
print("─" * 70)

df_agg = (df.groupby('date')['revenue'].sum()
            .reset_index()
            .rename(columns={'date':'ds','revenue':'y'}))

# Hold out last 3 months for testing
prophet_train = df_agg.iloc[:-3]
prophet_test = df_agg.iloc[-3:]

pm = Prophet(yearly_seasonality=True,
             seasonality_mode='multiplicative',
             changepoint_prior_scale=0.05)
pm.add_country_holidays(country_name='IN')
pm.fit(prophet_train)

# Predict the held-out 3 months
fut = pm.make_future_dataframe(periods=3, freq='MS')
fc = pm.predict(fut)

# Extract predictions for test period
predicted_test = fc.tail(3)[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].reset_index(drop=True)
actual_test = prophet_test.reset_index(drop=True)

prophet_mape = round(mape_score(actual_test['y'], predicted_test['yhat']) * 100, 2)

print(f"  Train months: {len(prophet_train)}")
print(f"  Test months: {len(prophet_test)} (held out)")
print(f"  Prophet MAPE: {prophet_mape}%")
print()
for i in range(len(actual_test)):
    actual_val = actual_test['y'].iloc[i]
    pred_val = predicted_test['yhat'].iloc[i]
    pct_err = abs(actual_val - pred_val) / actual_val * 100
    print(f"  {actual_test['ds'].iloc[i].strftime('%Y-%m')}: "
          f"Actual=Rs{actual_val/100000:.1f}L, "
          f"Predicted=Rs{pred_val/100000:.1f}L, "
          f"Error={pct_err:.1f}%")

# ══════════════════════════════════════════════════════════════════
# 3. SUMMARY
# ══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("FINAL AUDIT SUMMARY")
print("=" * 70)
print(f"  XGBoost MAPE (80/20 split, product-level): {production_mape}%")
print(f"  Prophet MAPE (held-out 3 months, aggregate): {prophet_mape}%")
print(f"  Current Confidence Gate Threshold: >25% MAPE")
print()

if production_mape <= 10:
    print(f"  ✅ XGBoost MEETS the ≤10% MAPE target")
elif production_mape <= 15:
    print(f"  ⚠️  XGBoost is CLOSE (10-15%) — acceptable with disclaimer")
elif production_mape <= 25:
    print(f"  ❌ XGBoost FAILS target — needs model improvement or tighter gate")
else:
    print(f"  🚨 XGBoost is UNRELIABLE — presentations should be blocked")

if prophet_mape <= 10:
    print(f"  ✅ Prophet MEETS the ≤10% MAPE target")
elif prophet_mape <= 15:
    print(f"  ⚠️  Prophet is CLOSE (10-15%) — acceptable with disclaimer")
elif prophet_mape <= 25:
    print(f"  ❌ Prophet FAILS target — needs model improvement or tighter gate")
else:
    print(f"  🚨 Prophet is UNRELIABLE — presentations should be blocked")

print()
print("  RECOMMENDED CONFIDENCE GATE THRESHOLDS:")
print(f"    - High Confidence: ≤10% MAPE (present confidently)")
print(f"    - Medium Confidence: 10-15% MAPE (present with 'approximate' disclaimer)")  
print(f"    - Low Confidence: 15-25% MAPE (present with strong warning)")
print(f"    - BLOCK: >25% MAPE (do not present forecast)")
print("=" * 70)
