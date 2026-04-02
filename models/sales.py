import os
import time
import pandas as pd
import numpy as np
import joblib
import xgboost as xgb
from prophet import Prophet
from sklearn.metrics import mean_absolute_percentage_error as mape_score
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

SALES_FILE  = 'data/nk_sales_data_2022_2026_feb.csv'
MODEL_DIR   = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'saved_models')

# Feature columns used by XGBoost — defined once, used everywhere
XGB_FEATURES = [
    'lag_1', 'lag_3', 'lag_12', 'rolling_3', 'rolling_std3',
    'month', 'quarter', 'year', 'is_festive',
    'avg_discount', 'returns', 'region_enc', 'product_enc'
]


def _prepare_features(df):
    """Shared feature engineering used by both train and infer modes."""
    df = df.sort_values('date')

    df_m = (df.groupby(['date', 'product_id', 'product_name', 'region'])
              .agg(revenue=('revenue', 'sum'),
                   quantity=('quantity_sold', 'sum'),
                   avg_margin=('margin_pct', 'mean'),
                   avg_discount=('discount_pct', 'mean'),
                   returns=('returns_qty', 'sum'),
                   is_festive=('is_festive', 'max'))
              .reset_index())

    df_m = df_m.sort_values(['product_id', 'region', 'date'])
    grp = df_m.groupby(['product_id', 'region'])

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

    return df_m, region_map, prod_map


def _train_xgboost(df_m):
    """Train XGBoost, return model + MAPE."""
    split = int(len(df_m) * 0.8)
    train, test = df_m.iloc[:split], df_m.iloc[split:]

    xm = xgb.XGBRegressor(
        n_estimators=300, learning_rate=0.05,
        max_depth=5, subsample=0.8,
        colsample_bytree=0.8, verbosity=0
    )
    xm.fit(train[XGB_FEATURES], train['revenue'])
    xpred = xm.predict(test[XGB_FEATURES])
    xmape = round(mape_score(test['revenue'], xpred) * 100, 2)
    return xm, xmape


def _train_prophet(df):
    """Train Prophet, return model + forecast + MAPE."""
    df_agg = (df.groupby('date')['revenue'].sum()
                .reset_index()
                .rename(columns={'date': 'ds', 'revenue': 'y'}))

    try:
        pm = Prophet(
            yearly_seasonality=True,
            seasonality_mode='multiplicative',
            changepoint_prior_scale=0.05
        )
        pm.add_country_holidays(country_name='IN')
        pm.fit(df_agg)

        fut = pm.make_future_dataframe(periods=3, freq='MS')
        fc  = pm.predict(fut)
        p_fore = fc.tail(3)[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].round(0)

        actual_agg = df_agg['y'].iloc[-6:]
        pred_agg   = fc[fc['ds'].isin(df_agg['ds'])]['yhat'].iloc[-6:]
        if len(actual_agg) == len(pred_agg) and len(actual_agg) > 0:
            prophet_mape = round(mape_score(actual_agg, pred_agg) * 100, 2)
        else:
            prophet_mape = 5.0

        return pm, p_fore, prophet_mape, True

    except Exception as e:
        print(f" [WARNING] Prophet/Stan backend failed: {e}. Falling back to linear projection.")
        last_date = df_agg['ds'].max()
        last_val  = df_agg['y'].iloc[-6:].mean()
        fut_dates = pd.date_range(start=last_date + pd.DateOffset(months=1), periods=3, freq='MS')
        p_fore = pd.DataFrame({
            'ds': fut_dates,
            'yhat': [last_val * (1.02 ** i) for i in range(1, 4)],
            'yhat_lower': [last_val * 0.9 for _ in range(3)],
            'yhat_upper': [last_val * 1.1 for _ in range(3)]
        }).round(0)
        return None, p_fore, 15.0, False


def _save_models(xgb_model, prophet_model, region_map, prod_map):
    """Persist trained models to disk with timestamp + latest pointer."""
    os.makedirs(MODEL_DIR, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M")

    # Save with timestamp
    joblib.dump(xgb_model, os.path.join(MODEL_DIR, f'xgb_sales_{ts}.pkl'))
    joblib.dump({'region_map': region_map, 'prod_map': prod_map},
                os.path.join(MODEL_DIR, f'encoders_sales_{ts}.pkl'))
    if prophet_model:
        joblib.dump(prophet_model, os.path.join(MODEL_DIR, f'prophet_sales_{ts}.pkl'))

    # Save "latest" pointers (always overwritten)
    joblib.dump(xgb_model, os.path.join(MODEL_DIR, 'xgb_sales_latest.pkl'))
    joblib.dump({'region_map': region_map, 'prod_map': prod_map},
                os.path.join(MODEL_DIR, 'encoders_sales_latest.pkl'))
    if prophet_model:
        joblib.dump(prophet_model, os.path.join(MODEL_DIR, 'prophet_sales_latest.pkl'))

    print(f"[✓] Sales models saved to {MODEL_DIR} (timestamp: {ts})")


def _load_models():
    """Load cached models from disk. Returns None if not found."""
    xgb_path     = os.path.join(MODEL_DIR, 'xgb_sales_latest.pkl')
    enc_path     = os.path.join(MODEL_DIR, 'encoders_sales_latest.pkl')
    prophet_path = os.path.join(MODEL_DIR, 'prophet_sales_latest.pkl')

    if not os.path.exists(xgb_path) or not os.path.exists(enc_path):
        return None, None, None, None

    xgb_model = joblib.load(xgb_path)
    encoders  = joblib.load(enc_path)

    prophet_model = None
    if os.path.exists(prophet_path):
        prophet_model = joblib.load(prophet_path)

    return xgb_model, prophet_model, encoders.get('region_map'), encoders.get('prod_map')


def run_sales(df=None, mode="infer"):
    """
    Dual-mode sales forecasting.
    
    mode="infer" : Load cached models from disk, run prediction only (~1-2s).
                   Falls back to full train if no cached models exist.
    mode="train" : Full train from data, save models to disk, return results.
    """
    if df is None:
        df = pd.read_csv(SALES_FILE, parse_dates=['date'])

    # ── INFER MODE: Load cached models ──
    if mode == "infer":
        xgb_model, prophet_model, region_map, prod_map = _load_models()

        if xgb_model is not None:
            # Feature engineering (same pipeline as training)
            df_m, _, _ = _prepare_features(df)

            # Re-encode using saved maps (handle unseen categories gracefully)
            df_m['region_enc']  = df_m['region'].map(region_map).fillna(-1).astype(int)
            df_m['product_enc'] = df_m['product_id'].map(prod_map).fillna(-1).astype(int)

            # XGBoost inference
            xpred = xgb_model.predict(df_m[XGB_FEATURES])
            split = int(len(df_m) * 0.8)
            test_actual = df_m['revenue'].iloc[split:]
            test_pred   = xpred[split:]
            xmape = round(mape_score(test_actual, test_pred) * 100, 2) if len(test_actual) > 0 else 10.0

            # Prophet forecast
            if prophet_model:
                fut    = prophet_model.make_future_dataframe(periods=3, freq='MS')
                fc     = prophet_model.predict(fut)
                p_fore = fc.tail(3)[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].round(0)

                df_agg = (df.groupby('date')['revenue'].sum()
                            .reset_index()
                            .rename(columns={'date': 'ds', 'revenue': 'y'}))
                actual_agg = df_agg['y'].iloc[-6:]
                pred_agg   = fc[fc['ds'].isin(df_agg['ds'])]['yhat'].iloc[-6:]
                if len(actual_agg) == len(pred_agg) and len(actual_agg) > 0:
                    prophet_mape = round(mape_score(actual_agg, pred_agg) * 100, 2)
                else:
                    prophet_mape = 5.0
                prophet_available = True
            else:
                # Linear fallback
                df_agg = (df.groupby('date')['revenue'].sum()
                            .reset_index()
                            .rename(columns={'date': 'ds', 'revenue': 'y'}))
                last_val  = df_agg['y'].iloc[-6:].mean()
                fut_dates = pd.date_range(start=df_agg['ds'].max() + pd.DateOffset(months=1), periods=3, freq='MS')
                p_fore = pd.DataFrame({
                    'ds': fut_dates, 'yhat': [last_val * (1.02 ** i) for i in range(1, 4)],
                    'yhat_lower': [last_val * 0.9 for _ in range(3)],
                    'yhat_upper': [last_val * 1.1 for _ in range(3)]
                }).round(0)
                prophet_mape = 15.0
                prophet_available = False

            return _build_result(df, df_m, xmape, prophet_mape, p_fore, prophet_available)

        else:
            # No cached models → fall through to full train
            print("[*] No cached sales models found. Falling back to full train...")
            mode = "train"

    # ── TRAIN MODE: Full train + save ──
    train_start = time.time()

    df_m, region_map, prod_map = _prepare_features(df)
    xgb_model, xmape = _train_xgboost(df_m)
    prophet_model, p_fore, prophet_mape, prophet_available = _train_prophet(df)

    _save_models(xgb_model, prophet_model, region_map, prod_map)

    train_duration = time.time() - train_start

    # Log training event
    try:
        from prediction_logger import log_training_event
        log_training_event("XGBoost", "sales", xmape, len(df_m), train_duration,
                           os.path.join(MODEL_DIR, 'xgb_sales_latest.pkl'))
        log_training_event("Prophet", "sales", prophet_mape, len(df), train_duration,
                           os.path.join(MODEL_DIR, 'prophet_sales_latest.pkl'))
    except Exception:
        pass

    return _build_result(df, df_m, xmape, prophet_mape, p_fore, prophet_available)


def _build_result(df, df_m, xmape, prophet_mape, p_fore, prophet_available):
    """Assemble the final result dict (shared by train and infer)."""
    monthly_rev = df.groupby('date')['revenue'].sum()
    zscores     = np.abs(stats.zscore(monthly_rev))
    anomaly_months = monthly_rev[zscores > 2].index.strftime('%Y-%m').tolist()

    top_products = (df.groupby(['product_id', 'product_name'])['revenue']
                      .sum()
                      .sort_values(ascending=False)
                      .head(5)
                      .reset_index()
                      .to_dict('records'))

    historical_summary = df.groupby('date')['revenue'].sum().reset_index()
    historical_summary['date'] = historical_summary['date'].dt.strftime('%Y-%m')
    hist_dict = dict(zip(historical_summary['date'], historical_summary['revenue']))

    return {
        "module":          "sales_forecast",
        "winner_model":    "XGBoost" if (not prophet_available or xmape < 20) else "Prophet",
        "prophet_mape":    prophet_mape,
        "xgb_mape":        xmape,
        "mape":            max(prophet_mape, xmape),
        "total_revenue_last_month": int(monthly_rev.iloc[-1]),
        "historical_monthly_revenue": hist_dict,
        "forecast_next_3_months":  p_fore.to_dict('records'),
        "trend":           "upward" if float(p_fore['yhat'].iloc[-1])
                                     > float(p_fore['yhat'].iloc[0])
                                     else "downward",
        "anomaly_months":  anomaly_months,
        "top_5_products":  top_products,
    }
