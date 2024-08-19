from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re

sourcepathbancos = "C:\\Users\\teu20\\Documents\\Poli\\Dados\\Raw\\bancos"
sourcepathempregados = "C:\\Users\\teu20\\Documents\\Poli\\Dados\\Raw\\empregados"
sourcepathreclamacoes = "C:\\Users\\teu20\\Documents\\Poli\\Dados\\Raw\\reclamacoes"
outputpath = "C:\\Users\\teu20\\Documents\\Poli\\Dados\\Trusted\\final"
url = "jdbc:mysql://localhost:3306/mydb"

# myproperties = {
#     "user": "root",
#     "password": "Ejwkh24$",
#     "driver": "com.mysql.cj.jdbc.Driver"
# }

spark = SparkSession.builder.master("local")\
         .appName("bancos_etl_trusted")\
         .getOrCreate()


dfbancos = spark.read\
    .option("header","true")\
    .option("recursiveFileLookup","true")\
    .parquet(outputpath)

dfbancos.show()