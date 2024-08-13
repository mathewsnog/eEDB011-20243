from IPython.display import display
import pandas as pd
import numpy as np
#import os
#import glob

pathraw = r'C:\Users\teu20\Documents\Poli\Dados\Raw'
pathtrusted = r'C:\Users\teu20\Documents\Poli\Dados\Trusted'


df = pd.read_csv(pathraw+"/"+'bancos_final_test.csv' , sep=',', encoding = 'utf-8')
df.rename(columns={"Instituição financeira" : "nome_do_banco"}, inplace=True)
df['CNPJ_test'] = pd.concat([df['CNPJ'], df['CNPJ IF']]), keep='first'

#df['CNPJ_test'] = df['CNPJ IF'].astype(str) + df['CNPJ'].astype(str)


df.to_csv(pathtrusted+"/"+'bancos_final_test.csv', sep=',', encoding='utf-8', index=False, header=True)

#display(df)