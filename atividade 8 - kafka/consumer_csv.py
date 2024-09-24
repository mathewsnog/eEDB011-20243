from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

jarpath = r"C:\Users\teu20\Documents\Poli\Dados\jars"

spark = SparkSession.builder\
    .config("spark.jars", jarpath + r"\spark-sql-kafka-0-10_2.12-3.5.2.jar" + "," + jarpath + r"\kafka-clients-2.8.1.jar" + "," + jarpath + r"\spark-token-provider-kafka-0-10_2.12-3.5.2.jar" + "," + jarpath + r"\commons-pool2-2.11.1.jar")\
    .appName('KafkaStructuredStreaming')\
    .getOrCreate()

kafka_df = spark \
.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers", "localhost:9092") \
.option("subscribe", "bancos_data") \
.option("includeHeaders", "true")\
.load()
# kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
# transformed_df = kafka_df.withColumn("transformedMessage", upper(kafka_df.message))

query = (
    kafka_df
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "dest-topic")
    .option("checkpointLocation", "checkpoint_folder")  # Specify the checkpoint location
    .start()
)

kafka_df.writeStream \
  .format("parquet") \
  .option("startingOffsets", "earliest") \
  .option("path", r"C:\Users\teu20\Documents\Poli\Dados\test") \
  .option("checkpointLocation", r"C:\Users\teu20\Documents\Poli\Dados\test") \
  .start()

query.awaitTermination()

# text_stream = kafka_df.selectExpr("CAST(value AS STRING)") \
#     .select(from_json("value", schema).alias("data")) \
#     .select("data.message")


# query = text_stream.writeStream \
#     .format("console") \
#     .start()

# query.awaitTermination()

# query = kafka_df.writeStream \
#     .format("console") \
#     .start()

# query.awaitTermination()

# wordCounts = kafka_df \
# .selectExpr('CAST(value AS STRING)') \
# .groupBy('value') \
# .count()