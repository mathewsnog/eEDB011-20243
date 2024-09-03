import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from recipe_transforms import *
from awsglue.dynamicframe import DynamicFrame

# Generated recipe steps for remove_prudencial_node1725314418731
def applyRecipe_node1725314418731(inputFrame, glueContext, transformation_ctx):
    frame = inputFrame.toDF()
    gc = glueContext
    df1 = DataCleaning.RemoveCombined.apply(
        data_frame=frame,
        glue_context=gc,
        transformation_ctx="remove_prudencial_node1725314418731-df1",
        source_column="Nome",
        remove_special_chars=False,
        remove_custom_chars=False,
        remove_numbers=False,
        remove_letters=False,
        remove_custom_value=True,
        remove_all_whitespace=False,
        remove_all_quotes=False,
        remove_all_punctuation=False,
        collapse_consecutive_whitespace=False,
        remove_leading_trailing_whitespace=False,
        remove_leading_trailing_quotes=False,
        remove_leading_trailing_punctuation=False,
        custom_value=" - PRUDENCIAL",
    )
    return DynamicFrame.fromDF(df1, gc, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node reclamacoes
reclamacoes_node1725312965566 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://328145136678/Raw/reclamacoes/"], "recurse": True}, transformation_ctx="reclamacoes_node1725312965566")

# Script generated for node empregados
empregados_node1725312972275 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://328145136678/Raw/empregados/"], "recurse": True}, transformation_ctx="empregados_node1725312972275")

# Script generated for node bancos
bancos_node1725312942608 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://328145136678/Raw/bancos/"], "recurse": True}, transformation_ctx="bancos_node1725312942608")

# Script generated for node change_schema_reclamacoes
change_schema_reclamacoes_node1725313142911 = ApplyMapping.apply(frame=reclamacoes_node1725312965566, mappings=[("ano", "string", "ano", "string"), ("trimestre", "string", "trimestre", "string"), ("categoria", "string", "categoria", "string"), ("tipo", "string", "tipo", "string"), ("cnpj_if", "string", "cnpj", "string"), ("instituição_financeira", "string", "instituicao_financeira", "string"), ("índice", "string", "indice", "string"), ("quantidade_de_reclamações_reguladas_procedentes", "string", "quantidade_reclamacoes_reguladas_procedentes", "string"), ("quantidade_de_reclamações_reguladas_-_outras", "string", "quantidade_reclamacoes_reguladas_outras_outras", "string"), ("quantidade_de_reclamações_não_reguladas", "string", "quantidade_reclamacoes_nao_reguladas", "string"), ("quantidade_total_de_reclamações", "string", "quantidade_total_reclamacoes", "string"), ("quantidade_total_de_clientes_–_ccs_e_scr", "string", "quantidade_total_clientes_ccs_scr", "string"), ("quantidade_de_clientes_–_ccs", "string", "quantidade_clientes_ccs", "string"), ("quantidade_de_clientes_–_scr", "string", "quantidade_clientes_scr", "string")], transformation_ctx="change_schema_reclamacoes_node1725313142911")

# Script generated for node change_schema_empregados
change_schema_empregados_node1725313283735 = ApplyMapping.apply(frame=empregados_node1725312972275, mappings=[("employer_name", "string", "employer_name", "string"), ("reviews_count", "string", "reviews_count", "string"), ("culture_count", "string", "culture_count", "string"), ("salaries_count", "string", "salaries_count", "string"), ("benefits_count", "string", "benefits_count", "string"), ("employer-website", "string", "employer_website", "string"), ("employer-headquarters", "string", "employer_headquarters", "string"), ("employer-founded", "string", "employer_founded", "string"), ("employer-industry", "string", "employer_industry", "string"), ("employer-revenue", "string", "employer_revenue", "string"), ("url", "string", "url", "string"), ("geral", "string", "geral", "string"), ("cultura_e_valores", "string", "cultura_e_valores", "string"), ("diversidade_e_inclusão", "string", "diversidade_e_inclusao", "string"), ("qualidade_de_vida", "string", "qualidade_de_vida", "string"), ("alta_liderança", "string", "alta_lideranca", "string"), ("remuneração_e_benefícios", "string", "remuneracao_e_beneficios", "string"), ("oportunidades_de_carreira", "string", "oportunidades_de_carreira", "string"), ("recomendam_para_outras_pessoas_%_", "string", "recomendam_para_outras_pessoas", "string"), ("perspectiva_positiva_da_empresa_%_", "string", "perspectiva_positiva_da_empresa", "string"), ("segmento", "string", "segmento", "string"), ("nome", "string", "nome", "string"), ("match_percent", "string", "match_percent", "string"), ("cnpj", "string", "cnpj", "string")], transformation_ctx="change_schema_empregados_node1725313283735")

# Script generated for node remove_prudencial
# Adding configuration for certain Data Preparation recipe steps to run properly
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# Recipe name: remove_prudencial_node1725314418731
remove_prudencial_node1725314418731 = applyRecipe_node1725314418731(
    inputFrame=bancos_node1725312942608,
    glueContext=glueContext,
    transformation_ctx="remove_prudencial_node1725314418731")

# Script generated for node change_schema_bancos
change_schema_bancos_node1725315940215 = ApplyMapping.apply(frame=remove_prudencial_node1725314418731, mappings=[("Segmento", "string", "segmento", "string"), ("CNPJ", "string", "cnpj", "string"), ("Nome", "string", "nome", "string")], transformation_ctx="change_schema_bancos_node1725315940215")

# Script generated for node reclamacoes_target
reclamacoes_target_node1725313357154 = glueContext.getSink(path="s3://328145136678/Trusted/reclamacoes/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="reclamacoes_target_node1725313357154")
reclamacoes_target_node1725313357154.setCatalogInfo(catalogDatabase="bancos",catalogTableName="reclamacoes_catalog")
reclamacoes_target_node1725313357154.setFormat("glueparquet", compression="snappy")
reclamacoes_target_node1725313357154.writeFrame(change_schema_reclamacoes_node1725313142911)
# Script generated for node empregados_target
empregados_target_node1725313357537 = glueContext.getSink(path="s3://328145136678/Trusted/empregados/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="empregados_target_node1725313357537")
empregados_target_node1725313357537.setCatalogInfo(catalogDatabase="bancos",catalogTableName="empregados_catalog")
empregados_target_node1725313357537.setFormat("glueparquet", compression="uncompressed")
empregados_target_node1725313357537.writeFrame(change_schema_empregados_node1725313283735)
# Script generated for node Amazon S3
AmazonS3_node1725314683555 = glueContext.getSink(path="s3://328145136678/Trusted/bancos/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1725314683555")
AmazonS3_node1725314683555.setCatalogInfo(catalogDatabase="bancos",catalogTableName="bancos_catalog")
AmazonS3_node1725314683555.setFormat("glueparquet", compression="uncompressed")
AmazonS3_node1725314683555.writeFrame(change_schema_bancos_node1725315940215)
job.commit()