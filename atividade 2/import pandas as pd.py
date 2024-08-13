import pandas as pd

filepath = r'C:\Users\teu20\Documents\Poli\Dados\Trusted\bancos_final_test.parquet'

df = pd.read_parquet(filepath, engine='pyarrow')

print(df.head())