import pandas as pd
import numpy as np
import xgboost as xgb
from prophet import Prophet
from sklearn.metrics import mean_absolute_percentage_error as mape_score
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

SALES_FILE  = 'data/nk_sales_data_2022_2026_feb.csv'

def run_sales():
    df = pd.read_csv(SALES_FILE, parse_dates=['date'])
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

    split = int(len(df_m) * 0.8)
    train, test = df_m.iloc[:split], df_m.iloc[split:]

    feats = ['lag_1','lag_3','lag_12','rolling_3','rolling_std3',
             'month','quarter','year','is_festive',
             'avg_discount','returns','region_enc','product_enc']

    xm = xgb.XGBRegressor(n_estimators=300, learning_rate=0.05,
                           max_depth=5, subsample=0.8,
                           colsample_bytree=0.8, verbosity=0)
    xm.fit(train[feats], train['revenue'])
    xpred  = xm.predict(test[feats])
    xmape  = round(mape_score(test['revenue'], xpred) * 100, 2)

    df_agg = (df.groupby('date')['revenue'].sum()
                .reset_index()
                .rename(columns={'date':'ds','revenue':'y'}))
    pm = Prophet(yearly_seasonality=True,
                 seasonality_mode='multiplicative',
                 changepoint_prior_scale=0.05)
    pm.add_country_holidays(country_name='IN')
    pm.fit(df_agg) # FIT ON FULL DATA
    fut    = pm.make_future_dataframe(periods=3, freq='MS')
    fc     = pm.predict(fut)
    p_fore = fc.tail(3)[['ds','yhat','yhat_lower','yhat_upper']].round(0)

    monthly_rev = df.groupby('date')['revenue'].sum()
    zscores     = np.abs(stats.zscore(monthly_rev))
    anomaly_months = monthly_rev[zscores > 2].index.strftime('%Y-%m').tolist()

    top_products = (df.groupby(['product_id','product_name'])['revenue']
                      .sum()
                      .sort_values(ascending=False)
                      .head(5)
                      .reset_index()
                      .to_dict('records'))

    historical_summary = (df.groupby('date')['revenue'].sum()
                            .reset_index())
    historical_summary['date'] = historical_summary['date'].dt.strftime('%Y-%m')
    hist_dict = dict(zip(historical_summary['date'], historical_summary['revenue']))

    return {
        "module":          "sales_forecast",
        "winner_model":    "XGBoost" if xmape < 20 else "Prophet",
        "xgboost_mape":    xmape,
        "total_revenue_last_month": int(monthly_rev.iloc[-1]),
        "historical_monthly_revenue": hist_dict,
        "forecast_next_3_months":  p_fore.to_dict('records'),
        "trend":           "upward" if float(p_fore['yhat'].iloc[-1])
                                     > float(p_fore['yhat'].iloc[0])
                                     else "downward",
        "anomaly_months":  anomaly_months,
        "top_5_products":  top_products,
    }
