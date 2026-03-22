import pandas as pd
df = pd.read_csv(r'd:\projects\nk_protein_poc\data\nk_sales_data_2022_2026_feb.csv', nrows=0)
print(list(df.columns))
