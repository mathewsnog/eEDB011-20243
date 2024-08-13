import pandas as pd
from pandas.errors import EmptyDataError 
from pandas.errors import IntCastingNaNError
import os
import glob
import numpy as np
import mysql.connector as mysql
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#Extracting data - Bancos
banksFileLocation = "C:/Users/anack/OneDrive/Documentos/Pós_eEDB/Visual studio/Dados/Bancos/EnquadramentoInicia_v2.tsv"
banksRawFile = pd.read_csv(banksFileLocation, sep='\t')
#banksRawFile = pd.read_csv(banksFileLocation, sep=";", encoding="latin_1")
#print(banksRawFile)
banksRawFile.to_csv ("banksRawFile.csv", index=False)

#Extracting data - Empregados
#read the path
employeesFilesLocation = glob.glob("C:/Users/anack/OneDrive/Documentos/Pós_eEDB/Visual studio/Dados/Empregados/*.csv", recursive=True)
#print(employeesFilesList)
employeesRawFile = pd.DataFrame()
#append all files together
for file in employeesFilesLocation:
            df_temp = pd.read_csv(file, sep="|", encoding="latin_1")
            employeesRawFile = employeesRawFile._append(df_temp, ignore_index=True)
#print(employeesRawFile)
employeesRawFile = employeesRawFile.dropna(subset=['CNPJ'])
employeesRawFile.to_csv ("employeesRawFile.csv", index=False)


#Extracting data - Reclamações
#read the path
claimsFilesLocation = glob.glob("C:/Users/anack/OneDrive/Documentos/Pós_eEDB/Visual studio/Dados/Reclamações/*.csv", recursive=True)
#print(employeesFilesList)
claimsRawFile = pd.DataFrame()
#append all files together
for file in claimsFilesLocation:
    try:
        df_temp = pd.read_csv(file, sep=";", encoding="latin_1")
        claimsRawFile = claimsRawFile._append(df_temp, ignore_index=True)
    except EmptyDataError:
        print(f"No columns to parse from file {file}")

#print(employeesRawFile)
claimsRawFile.to_csv ("claimsRawFile.csv", index=False)

banksEmployeesMerge = banksRawFile.merge(employeesRawFile, on="CNPJ", how="outer")
#banksEmployeesMerge = banksEmployeesMerge.astype({'CNPJ':'int'})
banksEmployeesMerge.to_csv ("banksEmployeesMerge.csv", index=False)

claimsFile1 = pd.DataFrame()
claimsFile1 = claimsRawFile.rename(columns={'CNPJ IF':'CNPJ'})
#claimsFile1["CNPJ"] = pd.to_numeric(claimsFile1["CNPJ"], errors='ignore')
#claimsFile1 = claimsFile1.astype({'CNPJ':'float64'}, errors='ignore')
claimsFile1['CNPJ'] = pd.to_numeric(claimsFile1['CNPJ'], errors ='coerce')
claimsFile1 = claimsFile1.dropna(subset=['CNPJ'])
claimsFile1.to_csv ("claimsFile1.csv", index=False)

#banksEmployeesClaimsMerge = pd.concat([banksEmployeesMerge, claimsFile1])
#banksEmployeesClaimsMerge.to_csv ("banksEmployeesClaimsMerge.csv", index=False)
#banksEmployeesClaimsMerge = banksEmployeesMerge.merge(claimsFile1, on="CNPJ", how="outer")

banksEmployeesClaimsMerge = pd.merge(banksEmployeesMerge,claimsFile1.assign(**{'CNPJ': pd.to_numeric(claimsFile1['CNPJ'], errors ='coerce')}), on='CNPJ', how='outer')
banksEmployeesClaimsMerge["Índice"] = banksEmployeesClaimsMerge["Índice"].replace(',','.')
banksEmployeesClaimsMerge.to_csv ("banksEmployeesClaimsMerge.csv", index=False)

db = mysql.connect(
     host = "localhost",
     user = "root",
     passwd = "benji",
 )

# cursor = db.cursor()
# cursor.execute("CREATE DATABASE atividade21")
# cursor.execute("USE atividade21")

#cursor.execute("SHOW DATABASES")

creds = {'usr': 'root',
             'pwd': 'benji',
             'hst': 'localhost',
             'prt': 3306,
             'db' : 'atividade21'
             }


connstr = 'mysql+mysqlconnector://{usr}:{pwd}@{hst}:{prt}/{db}'
engine2 = create_engine(connstr.format(**creds))
banksEmployeesClaimsMerge.to_sql(name='atividade21', con=engine2, if_exists='replace', index=False)
#banksEmployeesClaimsMerge.to_sql("atividade2-1", db, if_exists='replace', index=True)

#cursor = db.cursor()
#cursor.execute("CREATE DATABASE atividade2")
#cursor.execute("USE atividade2")
# cursor.execute("""CREATE TABLE banksEmployeesClaims (
#     Segmento_x VARCHAR(10),
#     CNPJ DECIMAL(20, 0),
#     Nome_x VARCHAR(255),
#     employer_name VARCHAR(255),
#     reviews_count INT,
#     culture_count INT,
#     salaries_count INT,
#     benefits_count INT,
#     employer_website VARCHAR(255),
#     employer_headquarters VARCHAR(255),
#     employer_founded INT,
#     employer_industry VARCHAR(255),
#     employer_revenue VARCHAR(255),
#     url VARCHAR(255),
#     Geral VARCHAR(255),
#     Cultura_e_valores VARCHAR(255),
#     Diversidade_e_inclusao VARCHAR(255),
#     Qualidade_de_vida VARCHAR(255),
#     Alta_lideranca VARCHAR(255),
#     Remuneracao_e_beneficios VARCHAR(255),
#     Oportunidades_de_carreira VARCHAR(255),
#     Recomendam_para_outras_pessoas_percent DECIMAL(5, 2),
#     Perspectiva_positiva_da_empresa_percent DECIMAL(5, 2),
#     Nome_y VARCHAR(255),
#     match_percent DECIMAL(5, 2),
#     Segmento_y VARCHAR(10),
#     Ano INT,
#     Trimestre VARCHAR(10),
#     Categoria VARCHAR(255),
#     Tipo VARCHAR(255),
#     Instituicao_financeira VARCHAR(255),
#     Indice DECIMAL(10, 2),
#     Quantidade_de_reclamacoes_reguladas_procedentes INT,
#     Quantidade_de_reclamacoes_reguladas_outras INT,
#     Quantidade_de_reclamacoes_nao_reguladas INT,
#     Quantidade_total_de_reclamacoes INT,
#     Quantidade_total_de_clientes_CCS_SCR INT,
#     Quantidade_de_clientes_CCS INT,
#     Quantidade_de_clientes_SCR INT,
#     Unnamed_14 VARCHAR(255)
# );""")




# cursor.execute("SHOW DATABASES") 
# databases = cursor.fetchall() 
# print(databases)
