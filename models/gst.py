import os
import time
import pandas as pd
import joblib
from sklearn.ensemble import IsolationForest
import warnings
warnings.filterwarnings('ignore')

GST_FILE  = 'data/nk_gst_data_2022_2026_feb.csv'
MODEL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'saved_models')

ISO_FEATURES = ['taxable_value', 'cgst_amount', 'sgst_amount', 'total_tax_amount']


def _train_isolation_forest(df):
    """Train IsolationForest and return model + anomaly count."""
    iso_feats = df[ISO_FEATURES]
    iso = IsolationForest(contamination=0.12, random_state=42)
    iso.fit(iso_feats)
    predictions = iso.predict(iso_feats)
    anomaly_count = int((predictions == -1).sum())
    return iso, predictions, anomaly_count


def _save_model(iso_model):
    """Save IsolationForest model to disk."""
    os.makedirs(MODEL_DIR, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M")
    joblib.dump(iso_model, os.path.join(MODEL_DIR, f'isoforest_gst_{ts}.pkl'))
    joblib.dump(iso_model, os.path.join(MODEL_DIR, 'isoforest_gst_latest.pkl'))
    print(f"[✓] GST IsolationForest saved to {MODEL_DIR} (timestamp: {ts})")


def _load_model():
    """Load cached IsolationForest from disk."""
    path = os.path.join(MODEL_DIR, 'isoforest_gst_latest.pkl')
    if os.path.exists(path):
        return joblib.load(path)
    return None


def run_gst(df=None, mode="infer"):
    """
    Dual-mode GST compliance analysis.
    
    mode="infer" : Load cached IsolationForest, predict anomalies only.
                   Falls back to train if no cached model exists.
    mode="train" : Full train, save model to disk, return results.
    """
    if df is None:
        df = pd.read_csv(GST_FILE)

    mismatches = df[df['mismatch_flag'] == 1]

    type_breakdown = df[df['mismatch_flag'] == 1]['mismatch_reason'].value_counts().to_dict()

    # ── INFER MODE ──
    if mode == "infer":
        cached_iso = _load_model()
        if cached_iso is not None:
            iso_feats = df[ISO_FEATURES]
            df['iso_flag'] = cached_iso.predict(iso_feats)
            iso_mismatches = int((df['iso_flag'] == -1).sum())
        else:
            # No cache → fall through to train
            print("[*] No cached GST IsolationForest found. Falling back to full train...")
            mode = "train"

    # ── TRAIN MODE ──
    if mode == "train":
        train_start = time.time()
        iso_model, predictions, iso_mismatches = _train_isolation_forest(df)
        df['iso_flag'] = predictions
        _save_model(iso_model)
        train_duration = time.time() - train_start

        # Log training event
        try:
            from prediction_logger import log_training_event
            log_training_event(
                "IsolationForest", "gst", None, len(df),
                train_duration, os.path.join(MODEL_DIR, 'isoforest_gst_latest.pkl')
            )
        except Exception:
            pass

    supplier_risk = (mismatches.groupby(['counterparty_id', 'counterparty_name'])
                               .agg(invoice_count=('invoice_no', 'count'),
                                    total_itc_risk=('total_tax_amount', 'sum'))
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
        "mismatch_pct":       round(len(mismatches) / len(df) * 100, 1),
        "total_itc_at_risk":  int(mismatches['total_tax_amount'].sum()),
        "isolation_forest_flags": iso_mismatches,
        "mismatch_type_breakdown": type_breakdown,
        "top_supplier_risks": supplier_risk,
        "monthly_mismatch_trend": monthly,
    }
