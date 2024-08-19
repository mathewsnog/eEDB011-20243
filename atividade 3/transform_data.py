from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#import re

sourcepathbancos = "C:\\Users\\teu20\\Documents\\Poli\\Dados\\Raw\\bancos"
sourcepathempregados = "C:\\Users\\teu20\\Documents\\Poli\\Dados\\Raw\\empregados"
sourcepathreclamacoes = "C:\\Users\\teu20\\Documents\\Poli\\Dados\\Raw\\reclamacoes"
outputpath = "C:\\Users\\teu20\\Documents\\Poli\\Dados\\Trusted"
url = "jdbc:mysql://localhost:3306/mydb"

myproperties = {
    "user": "root",
    "password": "Ejwkh24$",
    "driver": "com.mysql.cj.jdbc.Driver"
}

spark = SparkSession.builder.master("local")\
        .appName("bancos_etl_trusted")\
        .config("spark.jars", r"C:\Users\teu20\Documents\Poli\mysql-connector-j-9.0.0\mysql-connector-j-9.0.0.jar")\
        .getOrCreate()


dfbancos = spark.read\
    .option("header","true")\
    .option("recursiveFileLookup","true")\
    .option("enconding", "ISO-8859-1")\
    .parquet(sourcepathbancos)

for col in dfbancos.columns:
        dfbancos = dfbancos.withColumnRenamed(col, col.lower())

dfbancos = dfbancos.withColumn('nome', regexp_replace('nome', ' - PRUDENCIAL', ''))


dfempregados = spark.read\
    .option("header","true")\
    .option("recursiveFileLookup","true")\
    .option("enconding", "ISO-8859-1")\
    .parquet(sourcepathempregados)

for col in dfempregados.columns:
        dfempregados = dfempregados.withColumnRenamed(col, col.lower())

dfinnername = dfbancos.join(dfempregados, dfempregados.nome == dfbancos.nome, 'inner')\
                    .drop(dfempregados.cnpj, dfempregados.nome, dfempregados.segmento, dfempregados.employer_name)

dfreclamacoes = spark.read\
    .option("header","true")\
    .option("recursiveFileLookup","true")\
    .option("enconding", "ISO-8859-1")\
    .parquet(sourcepathreclamacoes)

for col in dfreclamacoes.columns:
        dfreclamacoes = dfreclamacoes.withColumnRenamed(col, col.lower())
        dfreclamacoes = dfreclamacoes.withColumnRenamed("cnpj if", "cnpj")


dfinnerfinal = dfinnername.join(dfreclamacoes, dfreclamacoes.cnpj == dfinnername.cnpj, 'inner')\
                    .drop(dfreclamacoes.cnpj)


for col in dfinnerfinal.columns:
        dfinnerfinal = dfinnerfinal.withColumnRenamed(col, col.replace('-', '_'))\
        .withColumnRenamed(col, col.replace(' ', '_'))\
        .withColumnRenamed(col, col.replace('(%)', ''))

dfinnerfinal = dfinnerfinal.drop(dfinnerfinal.columns[27])\
    .withColumnRenamed(dfinnerfinal.columns[28], "indice")\
    .withColumnRenamed(dfinnerfinal.columns[29], "quantidade_reclamacoes_reguladas_procedentes")\
    .withColumnRenamed(dfinnerfinal.columns[30], "quantidade_reclamacoes_reguladas_outras")\
    .withColumnRenamed(dfinnerfinal.columns[31], "quantidade_reclamacoes_nao_reguladas")\
    .withColumnRenamed(dfinnerfinal.columns[32], "quantidade_total_reclamacoes")\
    .withColumnRenamed(dfinnerfinal.columns[33], "quantidade_total_clientes_ccs_scr")\
    .withColumnRenamed(dfinnerfinal.columns[34], "quantidade_clientes_ccs")\
    .withColumnRenamed(dfinnerfinal.columns[35], "quantidade_clientes_scr")\
    .withColumnRenamed(dfinnerfinal.columns[35], "quantidade_clientes_scr")\
    .withColumnRenamed(dfinnerfinal.columns[21], "perspectiva_positiva_da_empresa")\
    .withColumnRenamed(dfinnerfinal.columns[20], "recomendam_para_outras_pessoas")
    

dfinnerfinal.write.mode("overwrite").parquet(outputpath + "\\final")
dfinnerfinal.write.jdbc(url, "bancos", mode="overwrite", properties=myproperties)

#dfinnerfinal.show()
#dfinnerfinal.printSchema()
# rowsa = df.count()
# print (rowsa)