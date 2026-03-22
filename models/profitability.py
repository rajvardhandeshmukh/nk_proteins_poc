import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

PROF_FILE = 'data/nk_sales_data_2022_2026_feb.csv'

def run_profitability():
    df = pd.read_csv(PROF_FILE)
    
    # Adapt column names from sales data
    df = df.rename(columns={'margin_pct': 'net_margin_pct', 'discount_pct': 'discount'})

    # Synthesize Decisions
    def get_decision(row):
        if row['net_margin_pct'] > 12: return 'Promote'
        if row['net_margin_pct'] < 4: return 'Discontinue'
        return 'Renegotiate'
    
    df['decision'] = df.apply(get_decision, axis=1)

    promote     = df[df['decision']=='Promote']
    discontinue = df[df['decision']=='Discontinue']
    renegotiate = df[df['decision']=='Renegotiate']

    top_products = (df.groupby(['product_id','product_name'])
                      .agg(avg_margin=('net_margin_pct','mean'),
                           total_revenue=('revenue','sum'))
                      .sort_values('avg_margin', ascending=False)
                      .head(5).reset_index().round(2).to_dict('records'))

    low_margin_customers = (df.groupby(['customer_id','customer_name'])
                              .agg(avg_margin=('net_margin_pct','mean'),
                                   total_revenue=('revenue','sum'))
                              .sort_values('avg_margin')
                              .head(5).reset_index().round(2).to_dict('records'))

    cust_feat = (df.groupby('customer_id')
                    .agg(revenue=('revenue','sum'),
                         margin=('net_margin_pct','mean'),
                         discount=('discount','mean'))
                    .reset_index())
    scaler   = StandardScaler()
    X_scaled = scaler.fit_transform(cust_feat[['revenue','margin','discount']])
    km       = KMeans(n_clusters=3, random_state=42, n_init=10)
    cust_feat['segment'] = km.fit_predict(X_scaled)

    seg_means  = cust_feat.groupby('segment')['margin'].mean().sort_values(ascending=False)
    seg_labels = {seg_means.index[0]:'High Value',
                  seg_means.index[1]:'Mid Tier',
                  seg_means.index[2]:'Low Margin Risk'}
    cust_feat['segment_label'] = cust_feat['segment'].map(seg_labels)
    seg_summary = cust_feat['segment_label'].value_counts().to_dict()

    return {
        "module":               "profitability",
        "promote_count":        int(len(promote['product_id'].unique())),
        "discontinue_count":    int(len(discontinue['product_id'].unique())),
        "renegotiate_count":    int(len(renegotiate['customer_id'].unique())),
        "top_5_products":       top_products,
        "low_margin_customers": low_margin_customers,
        "products_to_promote":  promote[['product_name','net_margin_pct']]
                                .drop_duplicates()
                                .head(3).round(2).to_dict('records'),
        "products_to_discontinue": df.sort_values('net_margin_pct', ascending=True)[['product_name','net_margin_pct']]
                                   .drop_duplicates(subset=['product_name'])
                                   .head(3).round(2).to_dict('records'),
        "customer_segments":    seg_summary,
    }
