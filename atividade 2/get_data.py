from IPython.display import display
import pandas as pd
import numpy as np
import os
#import mysql.connector
from sqlalchemy import create_engine
from unidecode import unidecode

#mysql connection credentials
creds = {'usr': 'root',
             'pwd': r'Ejwkh24$',
             'hst': 'localhost',
             'prt': 3306,
             'dbn': 'mydb'}

connstr = 'mysql+mysqlconnector://{usr}:{pwd}@{hst}:{prt}/{dbn}'
engine = create_engine(connstr.format(**creds))

#file path variables
path = r'C:\Users\teu20\Documents\Poli\Dados\Raw\Reclamações'
path2 = r'C:\Users\teu20\Documents\Poli\Dados\Raw\Empregados'
path3 = r'C:\Users\teu20\Documents\Poli\Dados\Raw\Bancos'
#pathraw = r'C:\Users\teu20\Documents\Poli\Dados\Raw'
pathtrusted = r'C:\Users\teu20\Documents\Poli\Dados\Trusted'
files = [file for file in os.listdir(path) if not file.startswith('.')]
files2 = [file for file in os.listdir(path2) if not file.startswith('.')]
files3 = [file for file in os.listdir(path3) if not file.startswith('.')]
all_data = pd.DataFrame()


#get data Reclamações
for file in files:
    df = pd.read_csv(path+"/"+file , sep=';', encoding = "ISO-8859-1")
    #df.columns = df.columns.astype("str")
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.strip()
    #df.columns = map(lambda x : x.replace(" - ", " ").replace(" – ", "_").replace("  ", "_").replace("   ", "_"), df.columns)
    df.rename(columns={"cnpj if" : "cnpj", "instituição financeira" : "nome_do_banco", 
                       "índice" : "indice", "quantidade de reclamações reguladas procedentes" : "quantidade_reclamacoes_reguladas_procedentes",
                       "quantidade de reclamações reguladas - outras" : "quantidade_reclamacoes_reguladas_outras",
                       "quantidade de reclamações não reguladas" : "quantidade_reclamacoes_nao_reguladas",
                       "quantidade total de reclamações" : "quantidade_total_reclamacoes"}, inplace=True)
    df.columns.values[11] = "quantidade_total_clientes_ccs_scr"
    df.columns.values[12] = "quantidade_clientes_ccs"
    df.columns.values[13] = "quantidade_clientes_scr"
    df.drop(df.columns[df.columns.str.contains('unnamed', case = False)], axis = 1, inplace = True)
    df = df.astype({"cnpj": str})
    #df.replace(r'^\s*$', np.nan, regex=True)
    all_data = pd.concat([all_data,df])


#get data Empregados
for file in files2:
    df2 = pd.read_csv(path2+"/"+file , sep='|', encoding = "ISO-8859-1")
    #df2.columns = df2.columns.astype("str")
    df2.columns = df2.columns.str.lower()
    df2.columns = df2.columns.str.strip()
    df2.columns = map(lambda x : x.replace(" ", "_").replace("-", "_").replace("(%)", ""), df2.columns)
    #df2.rename(columns={"employer_name" : "nome_do_banco"}, inplace=True)
    df2.columns.values[0] = "nome_do_banco"
    df2.columns.values[13] = "diversidade_e_inclusao"
    df2.columns.values[15] = "alta_lideranca"
    df2.columns.values[16] = "remuneracao_e_beneficios"
    #df = df.astype({"cnpj": str})
    all_data = pd.concat([all_data,df2])


#get data Bancos
for file in files3:
    df3 = pd.read_csv(path3+"/"+file , sep='\t', encoding = "utf-8")
    #df3.columns = df3.columns.astype("str")
    df3.columns = df3.columns.str.lower()
    df3.columns = df3.columns.str.strip()
    df3.columns.values[2] = "nome_do_banco"
    df3 = df3.astype({"cnpj": str})
    all_data = pd.concat([all_data,df3])


#final transformations
all_data['quantidade_total_clientes_ccs_scr'] = all_data['quantidade_total_clientes_ccs_scr'].replace(r'^\s*$', np.nan, regex=True)
all_data['quantidade_total_clientes_ccs_scr'] = all_data["quantidade_total_clientes_ccs_scr"].fillna(0)
all_data['quantidade_clientes_ccs'] = all_data['quantidade_clientes_ccs'].replace(r'^\s*$', np.nan, regex=True)
all_data['quantidade_clientes_ccs'] = all_data["quantidade_clientes_ccs"].fillna(0)
all_data['quantidade_clientes_scr'] = all_data['quantidade_clientes_scr'].replace(r'^\s*$', np.nan, regex=True)
all_data['quantidade_clientes_scr'] = all_data["quantidade_clientes_scr"].fillna(0)
all_data['cnpj'] = all_data['cnpj'].replace(np.nan, '', regex=True)
#all_data['nome_do_banco']= all_data['nome_do_banco'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
#all_data['nome_do_banco'] = all_data['nome_do_banco'].apply(unidecode)
all_data = all_data.astype({"cnpj": str})
all_data = all_data.astype({"nome_do_banco": str})
all_data = all_data.astype({"quantidade_total_clientes_ccs_scr": float})
all_data = all_data.astype({"quantidade_clientes_ccs": float})
all_data = all_data.astype({"quantidade_clientes_scr": float})

#all_data = all_data['quantidade_total_clientes_ccs_scr'].astype('float')

#save parquet+csv trusted zone
all_data.to_csv(pathtrusted+"/"+'bancos_final_test.csv', sep=',', encoding='utf-8', index=False, header=True)
all_data.to_parquet(pathtrusted+"/"+'bancos_final_test.parquet')

#insert data delivery zone
all_data.to_sql(name='bancos_python', con=engine, if_exists='replace', index=False)
#display(all_data)