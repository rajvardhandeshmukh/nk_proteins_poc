import pandas as pd
import warnings
warnings.filterwarnings('ignore')

INV_FILE = 'data/nk_inventory_2022_2026_feb.csv'

def run_inventory(df=None):
    if df is None:
        df = pd.read_csv(INV_FILE)

    dead     = df[df['is_dead_stock'] == 1]
    reorder  = df[df['needs_reorder'] == 1]

    dead_by_cat = (dead.groupby('category')
                       .agg(count=('sku','count'),
                            value=('total_value_inr','sum'))
                       .sort_values('value', ascending=False)
                       .round(2)
                       .to_dict('records'))

    top_dead = (dead.nlargest(5, 'total_value_inr')
                    [['sku','category','warehouse',
                      'days_no_movement','current_stock_kg','total_value_inr']]
                    .round(2)
                    .to_dict('records'))

    critical_reorder = (reorder.nsmallest(5, 'current_stock_kg')
                               [['sku','category','current_stock_kg',
                                 'reorder_point','lead_time_days','ideal_stock']]
                               .to_dict('records'))

    return {
        "module":              "inventory",
        "total_skus":          int(len(df)),
        "dead_stock_count":    int(len(dead)),
        "dead_stock_pct":      round(len(dead)/len(df)*100, 1),
        "total_capital_locked":int(dead['total_value_inr'].sum()),
        "reorder_alerts":      int(len(reorder)),
        "dead_by_category":    dead_by_cat,
        "top_dead_skus":       top_dead,
        "critical_reorders":   critical_reorder,
    }
