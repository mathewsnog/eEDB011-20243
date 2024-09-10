from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

sourcepathbancos = r"/mnt/c/users/teu20/Documents/Poli/Dados/Source/bancos"
sourcepathempregados = r"/mnt/c/users/teu20/Documents/Poli/Dados/Source/empregados"
sourcepathreclamacoes = r"/mnt/c/users/teu20/Documents/Poli/Dados/Source/reclamacoes"
outputpath = r"/mnt/c/users/teu20/Documents/Poli/Dados/Raw"
glob_pattern = "*.csv"

spark = SparkSession.builder.master("local").appName("bancos_etl_raw").getOrCreate()

# read bancos
df = spark.read.format("csv")\
      .option("recursiveFileLookup", True)\
      .option("header", True)\
      .option("delimiter", "\t")\
      .option("encoding", "ISO-8859-1")\
      .csv(sourcepathbancos)

df = df.withColumn("Segmento", col("Segmento").cast(StringType()))\
      .withColumn("CNPJ", col("CNPJ").cast(StringType()))\
      .withColumn("Nome", col("Nome").cast(StringType()))

df.write.mode("overwrite").parquet(outputpath + r"/bancos")

# read empregados
dfemp1 = spark.read.options(header='True', delimiter='|').csv(r"/mnt/c/users/teu20/Documents/Poli/Dados/Source/empregados/glassdoor_consolidado_join_match_v2.csv")
dfemp2 = spark.read.options(header='True', delimiter='|').csv(r"/mnt/c/users/teu20/Documents/Poli/Dados/Source/empregados/glassdoor_consolidado_join_match_less_v2.csv")
dfempfinal = dfemp1.unionByName(dfemp2, allowMissingColumns=True)
dfempfinal = dfempfinal.withColumn("employer_name", col("employer_name").cast(StringType()))\
                        .withColumn("reviews_count", col("reviews_count").cast(IntegerType()))\
                        .withColumn("culture_count", col("culture_count").cast(IntegerType()))\
                        .withColumn("salaries_count", col("salaries_count").cast(IntegerType()))\
                        .withColumn("benefits_count", col("benefits_count").cast(IntegerType()))\
                        .withColumn("employer-website", col("employer-website").cast(StringType()))\
                        .withColumn("employer-headquarters", col("employer-headquarters").cast(StringType()))\
                        .withColumn("employer-founded", col("employer-founded").cast(IntegerType()))\
                        .withColumn("employer-industry", col("employer-industry").cast(StringType()))\
                        .withColumn("employer-revenue", col("employer-revenue").cast(StringType()))\
                        .withColumn("url", col("url").cast(StringType()))\
                        .withColumn("Geral", col("Geral").cast(FloatType()))\
                        .withColumn("Cultura e valores", col("Cultura e valores").cast(FloatType()))\
                        .withColumn("Diversidade e inclusão", col("Diversidade e inclusão").cast(FloatType()))\
                        .withColumn("Qualidade de vida", col("Qualidade de vida").cast(FloatType()))\
                        .withColumn("Alta liderança", col("Alta liderança").cast(FloatType()))\
                        .withColumn("Remuneração e benefícios", col("Remuneração e benefícios").cast(FloatType()))\
                        .withColumn("Oportunidades de carreira", col("Oportunidades de carreira").cast(FloatType()))\
                        .withColumn("Recomendam para outras pessoas(%)", col("Recomendam para outras pessoas(%)").cast(FloatType()))\
                        .withColumn("Perspectiva positiva da empresa(%)", col("Perspectiva positiva da empresa(%)").cast(FloatType()))\
                        .withColumn("Segmento", col("Segmento").cast(StringType()))\
                        .withColumn("Nome", col("Nome").cast(StringType()))\
                        .withColumn("match_percent", col("match_percent").cast(FloatType()))\
                        .withColumn("CNPJ", col("CNPJ").cast(StringType()))

dfempfinal.write.mode("overwrite").parquet(outputpath + r"/empregados")


# read reclamacoes
df3 = spark.read.option("recursiveFileLookup", True)\
      .option("header", True)\
      .option("delimiter", ";")\
      .option("encoding", "ISO-8859-1")\
      .csv(sourcepathreclamacoes)
df3 = df3.drop("_c14")
df3 = df3.withColumn("Ano", col("Ano").cast(IntegerType()))\
      .withColumn("Trimestre", col("Trimestre").cast(StringType()))\
      .withColumn("Categoria", col("Categoria").cast(StringType()))\
      .withColumn("Tipo", col("Tipo").cast(StringType()))\
      .withColumn("CNPJ IF", col("CNPJ IF").cast(StringType()))\
      .withColumn("Instituição financeira", col("Instituição financeira").cast(StringType()))\
      .withColumn("Índice", col("Índice").cast(StringType()))\
      .withColumn("Quantidade de reclamações reguladas procedentes", col("Quantidade de reclamações reguladas procedentes").cast(IntegerType()))\
      .withColumn("Quantidade de reclamações reguladas - outras", col("Quantidade de reclamações reguladas - outras").cast(IntegerType()))\
      .withColumn("Quantidade de reclamações não reguladas", col("Quantidade de reclamações não reguladas").cast(IntegerType()))\
      .withColumn("Quantidade total de reclamações", col("Quantidade total de reclamações").cast(IntegerType()))\
      .withColumn("Quantidade total de clientes  CCS e SCR", col("Quantidade total de clientes  CCS e SCR").cast(IntegerType()))\
      .withColumn("Quantidade de clientes  CCS", col("Quantidade de clientes  CCS").cast(IntegerType()))\
      .withColumn("Quantidade de clientes  SCR", col("Quantidade de clientes  SCR").cast(IntegerType()))

df3.write.mode("overwrite").parquet(outputpath + r"/reclamacoes")

spark.stop()


#  empregadossch = StructType ([StructField('employer_name',StringType(),True),
#  StructField('reviews_count',StringType(),True),
#  StructField('culture_count',StringType(),True),
#  StructField('salaries_count',StringType(),True),
#  StructField('benefits_count',StringType(),True),
#  StructField('employer-website',StringType(),True),
#  StructField('employer-headquarters',StringType(),True),
#  StructField('employer-founded',StringType(),True),
#  StructField('employer-industry',StringType(),True),
#  StructField('employer-revenue',StringType(),True),
#  StructField('url',StringType(),True),
#  StructField('Geral',StringType(),True),
#  StructField('Cultura e valores',StringType(),True),
#  StructField('Diversidade e inclusão',StringType(),True),
#  StructField('Qualidade de vida',StringType(),True),
#  StructField('Alta liderança',StringType(),True),
#  StructField('Remuneração e benefícios',StringType(),True),
#  StructField('Oportunidades de carreira',StringType(),True),
#  StructField('Recomendam para outras pessoas(%)',StringType(),True),
#  StructField('Perspectiva positiva da empresa(%)',StringType(),True),
#  StructField('Nome',StringType(),True),
#  StructField('match_percent',StringType(),True),
#  StructField('CNPJ',StringType(),True),
#  StructField('Segmento',StringType(),True)])

# df2 = spark.read.format("csv")\
#       .schema(empregadossch)\
#       .option("recursiveFileLookup", True)\
#       .option("header", True)\
#       .option("delimiter", "|")\
#       .option("enconding", "ISO-8859-1")\
#       .load(sourcepathempregados)

# df2.printSchema()

# df2.show()