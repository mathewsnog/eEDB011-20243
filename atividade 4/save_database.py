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

dfbancos.write.jdbc(url, "bancos", mode="overwrite", properties=myproperties)

dfempregados = spark.read\
    .option("header","true")\
    .option("recursiveFileLookup","true")\
    .option("enconding", "ISO-8859-1")\
    .parquet(sourcepathempregados)

dfempregados.write.jdbc(url, "empregados", mode="overwrite", properties=myproperties)

dfreclamacoes = spark.read\
    .option("header","true")\
    .option("recursiveFileLookup","true")\
    .option("enconding", "ISO-8859-1")\
    .parquet(sourcepathreclamacoes)

dfreclamacoes =    dfreclamacoes.withColumnRenamed(dfreclamacoes.columns[6], "indice")\
    .withColumnRenamed(dfreclamacoes.columns[5], "instituicao_financeira")\
    .withColumnRenamed(dfreclamacoes.columns[7], "quantidade_reclamacoes_reguladas_procedentes")\
    .withColumnRenamed(dfreclamacoes.columns[8], "quantidade_reclamacoes_reguladas_outras")\
    .withColumnRenamed(dfreclamacoes.columns[9], "quantidade_reclamacoes_nao_reguladas")\
    .withColumnRenamed(dfreclamacoes.columns[10], "quantidade_total_reclamacoes")\
    .withColumnRenamed(dfreclamacoes.columns[11], "quantidade_total_clientes_ccs_scr")\
    .withColumnRenamed(dfreclamacoes.columns[12], "quantidade_clientes_ccs")\
    .withColumnRenamed(dfreclamacoes.columns[13], "quantidade_clientes_scr")
    
dfreclamacoes.write.jdbc(url, "reclamacoes", mode="overwrite", properties=myproperties)
#dfinnerfinal.write.mode("overwrite").parquet(outputpath + "\\final")
#dfinnerfinal.show()
#dfinnerfinal.printSchema()
# rowsa = df.count()
# print (rowsa)