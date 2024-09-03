import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node reclamacoes
reclamacoes_node1725311077895 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ";", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://328145136678/Source/reclamacoes/"], "recurse": True}, transformation_ctx="reclamacoes_node1725311077895")

# Script generated for node bancos
bancos_node1725311040025 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": "\t", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://328145136678/Source/bancos/"], "recurse": True}, transformation_ctx="bancos_node1725311040025")

# Script generated for node empregados
empregados_node1725311097119 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": "|", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://328145136678/Source/empregados/"], "recurse": True}, transformation_ctx="empregados_node1725311097119")


# Save node reclamacoes to parquet
glueContext.write_dynamic_frame.from_options(
 frame=reclamacoes_node1725311077895,
 connection_type="s3",
 connection_options={"path": "s3://328145136678/Raw/reclamacoes"},
 format="parquet"
)

# Save node bancos to parquet
glueContext.write_dynamic_frame.from_options(
 frame=bancos_node1725311040025,
 connection_type="s3",
 connection_options={"path": "s3://328145136678/Raw/bancos"},
 format="parquet"
)

# Save node empregados to parquet
glueContext.write_dynamic_frame.from_options(
 frame=empregados_node1725311097119,
 connection_type="s3",
 connection_options={"path": "s3://328145136678/Raw/empregados"},
 format="parquet"
)


job.commit()