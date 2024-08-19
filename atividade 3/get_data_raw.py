#from IPython.display import display
from pyspark.sql import SparkSession
from pyspark.sql.types import *

sourcepathbancos = "C:\\Users\\teu20\\Documents\\Poli\\Dados\\Source\\bancos"
sourcepathempregados = "C:\\Users\\teu20\\Documents\\Poli\\Dados\\Source\\empregados"
sourcepathreclamacoes = "C:\\Users\\teu20\\Documents\\Poli\\Dados\\Source\\reclamacoes"
outputpath = "C:\\Users\\teu20\\Documents\\Poli\\Dados\\Raw"
glob_pattern = "*.csv"

spark = SparkSession.builder.master("local").appName("bancos_etl_raw").getOrCreate()

# read bancos
df = spark.read.format("csv")\
      .option("recursiveFileLookup", True)\
      .option("header", True)\
      .option("delimiter", "\t")\
      .csv(sourcepathbancos)

df.write.mode("overwrite").parquet(outputpath + "\\bancos")

# empregadossch = StructType ([StructField('employer_name',StringType(),True),
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

# read empregados
dfemp1 = spark.read.options(header='True', delimiter='|').csv(r"C:\Users\teu20\Documents\Poli\Dados\Source\empregados\glassdoor_consolidado_join_match_v2.csv")
dfemp2 = spark.read.options(header='True', delimiter='|').csv(r"C:\Users\teu20\Documents\Poli\Dados\Source\empregados\glassdoor_consolidado_join_match_less_v2.csv")
dfempfinal = dfemp1.unionByName(dfemp2, allowMissingColumns=True)
dfempfinal.write.mode("overwrite").parquet(outputpath + "\\empregados")


# read reclamacoes
df3 = spark.read.format("csv")\
      .option("recursiveFileLookup", True)\
      .option("header", True)\
      .option("delimiter", ";")\
      .option("enconding", "ISO-8859-1")\
      .csv(sourcepathreclamacoes)
df3 = df3.drop("_c14")

df3.write.mode("overwrite").parquet(outputpath + "\\reclamacoes")

spark.stop()