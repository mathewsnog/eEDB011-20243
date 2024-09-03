import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsglue import DynamicFrame

def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split('.')[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
            elif isinstance(distinct_, str) :
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output

def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
    nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
    return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node bancos_trusted
bancos_trusted_node1725316500660 = glueContext.create_dynamic_frame.from_catalog(database="bancos", table_name="bancos_catalog", transformation_ctx="bancos_trusted_node1725316500660")

# Script generated for node empregados_trusted
empregados_trusted_node1725316545687 = glueContext.create_dynamic_frame.from_catalog(database="bancos", table_name="empregados_catalog", transformation_ctx="empregados_trusted_node1725316545687")

# Script generated for node reclamacoes_trusted
reclamacoes_trusted_node1725316522572 = glueContext.create_dynamic_frame.from_catalog(database="bancos", table_name="reclamacoes_catalog", transformation_ctx="reclamacoes_trusted_node1725316522572")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1725316582583 = ApplyMapping.apply(frame=empregados_trusted_node1725316545687, mappings=[("employer_name", "string", "right_employer_name", "string"), ("reviews_count", "string", "right_reviews_count", "string"), ("culture_count", "string", "right_culture_count", "string"), ("salaries_count", "string", "right_salaries_count", "string"), ("benefits_count", "string", "right_benefits_count", "string"), ("employer_website", "string", "right_employer_website", "string"), ("employer_headquarters", "string", "right_employer_headquarters", "string"), ("employer_founded", "string", "right_employer_founded", "string"), ("employer_industry", "string", "right_employer_industry", "string"), ("employer_revenue", "string", "right_employer_revenue", "string"), ("url", "string", "right_url", "string"), ("geral", "string", "right_geral", "string"), ("cultura_e_valores", "string", "right_cultura_e_valores", "string"), ("diversidade_e_inclusao", "string", "right_diversidade_e_inclusao", "string"), ("qualidade_de_vida", "string", "right_qualidade_de_vida", "string"), ("alta_lideranca", "string", "right_alta_lideranca", "string"), ("remuneracao_e_beneficios", "string", "right_remuneracao_e_beneficios", "string"), ("oportunidades_de_carreira", "string", "right_oportunidades_de_carreira", "string"), ("recomendam_para_outras_pessoas", "string", "right_recomendam_para_outras_pessoas", "string"), ("perspectiva_positiva_da_empresa", "string", "right_perspectiva_positiva_da_empresa", "string"), ("segmento", "string", "right_segmento", "string"), ("nome", "string", "right_nome", "string"), ("match_percent", "string", "right_match_percent", "string"), ("cnpj", "string", "right_cnpj", "string")], transformation_ctx="RenamedkeysforJoin_node1725316582583")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1725317097690 = ApplyMapping.apply(frame=reclamacoes_trusted_node1725316522572, mappings=[("ano", "string", "right_ano", "string"), ("trimestre", "string", "right_trimestre", "string"), ("categoria", "string", "right_categoria", "string"), ("tipo", "string", "right_tipo", "string"), ("cnpj", "string", "right_cnpj", "string"), ("instituicao_financeira", "string", "right_instituicao_financeira", "string"), ("indice", "string", "right_indice", "string"), ("quantidade_reclamacoes_reguladas_procedentes", "string", "right_quantidade_reclamacoes_reguladas_procedentes", "string"), ("quantidade_reclamacoes_reguladas_outras_outras", "string", "right_quantidade_reclamacoes_reguladas_outras_outras", "string"), ("quantidade_reclamacoes_nao_reguladas", "string", "right_quantidade_reclamacoes_nao_reguladas", "string"), ("quantidade_total_reclamacoes", "string", "right_quantidade_total_reclamacoes", "string"), ("quantidade_total_clientes_ccs_scr", "string", "right_quantidade_total_clientes_ccs_scr", "string"), ("quantidade_clientes_ccs", "string", "right_quantidade_clientes_ccs", "string"), ("quantidade_clientes_scr", "string", "right_quantidade_clientes_scr", "string")], transformation_ctx="RenamedkeysforJoin_node1725317097690")

# Script generated for node bancos_empregados
bancos_empregados_node1725316571007 = Join.apply(frame1=bancos_trusted_node1725316500660, frame2=RenamedkeysforJoin_node1725316582583, keys1=["nome"], keys2=["right_nome"], transformation_ctx="bancos_empregados_node1725316571007")

# Script generated for node bancos_empregados_drop_null
bancos_empregados_drop_null_node1725316818134 = drop_nulls(glueContext, frame=bancos_empregados_node1725316571007, nullStringSet={"", "null"}, nullIntegerSet={}, transformation_ctx="bancos_empregados_drop_null_node1725316818134")

# Script generated for node change_schema_bancos_empregados
change_schema_bancos_empregados_node1725316963885 = ApplyMapping.apply(frame=bancos_empregados_drop_null_node1725316818134, mappings=[("segmento", "string", "segmento", "string"), ("right_remuneracao_e_beneficios", "string", "remuneracao_e_beneficios", "string"), ("right_geral", "string", "geral", "string"), ("right_employer_revenue", "string", "employer_revenue", "string"), ("right_oportunidades_de_carreira", "string", "oportunidades_de_carreira", "string"), ("right_employer_founded", "string", "employer_founded", "string"), ("right_cultura_e_valores", "string", "cultura_e_valores", "string"), ("right_perspectiva_positiva_da_empresa", "string", "perspectiva_positiva_da_empresa", "string"), ("right_url", "string", "url", "string"), ("right_employer_website", "string", "employer_website", "string"), ("right_salaries_count", "string", "salaries_count", "string"), ("right_employer_headquarters", "string", "employer_headquarters", "string"), ("right_employer_industry", "string", "employer_industry", "string"), ("right_diversidade_e_inclusao", "string", "diversidade_e_inclusao", "string"), ("right_match_percent", "string", "match_percent", "string"), ("right_benefits_count", "string", "benefits_count", "string"), ("right_recomendam_para_outras_pessoas", "string", "recomendam_para_outras_pessoas", "string"), ("right_reviews_count", "string", "reviews_count", "string"), ("right_qualidade_de_vida", "string", "qualidade_de_vida", "string"), ("right_culture_count", "string", "culture_count", "string"), ("right_alta_lideranca", "string", "alta_lideranca", "string"), ("nome", "string", "nome", "string"), ("right_employer_name", "string", "employer_name", "string"), ("cnpj", "string", "cnpj", "string")], transformation_ctx="change_schema_bancos_empregados_node1725316963885")

# Script generated for node bancos_empregados_reclamacoes
bancos_empregados_reclamacoes_node1725317087015 = Join.apply(frame1=change_schema_bancos_empregados_node1725316963885, frame2=RenamedkeysforJoin_node1725317097690, keys1=["cnpj"], keys2=["right_cnpj"], transformation_ctx="bancos_empregados_reclamacoes_node1725317087015")

# Script generated for node Drop Null Fields
DropNullFields_node1725317237455 = drop_nulls(glueContext, frame=bancos_empregados_reclamacoes_node1725317087015, nullStringSet={"", "null"}, nullIntegerSet={}, transformation_ctx="DropNullFields_node1725317237455")

# Script generated for node bancos_final_schema
bancos_final_schema_node1725317309464 = ApplyMapping.apply(frame=DropNullFields_node1725317237455, mappings=[("segmento", "string", "segmento", "string"), ("remuneracao_e_beneficios", "string", "remuneracao_e_beneficios", "string"), ("geral", "string", "geral", "string"), ("employer_revenue", "string", "employer_revenue", "string"), ("oportunidades_de_carreira", "string", "oportunidades_de_carreira", "string"), ("employer_founded", "string", "employer_founded", "string"), ("cultura_e_valores", "string", "cultura_e_valores", "string"), ("perspectiva_positiva_da_empresa", "string", "perspectiva_positiva_da_empresa", "string"), ("url", "string", "url", "string"), ("employer_website", "string", "employer_website", "string"), ("salaries_count", "string", "salaries_count", "string"), ("employer_headquarters", "string", "employer_headquarters", "string"), ("employer_industry", "string", "employer_industry", "string"), ("diversidade_e_inclusao", "string", "diversidade_e_inclusao", "string"), ("match_percent", "string", "match_percent", "string"), ("benefits_count", "string", "benefits_count", "string"), ("recomendam_para_outras_pessoas", "string", "recomendam_para_outras_pessoas", "string"), ("reviews_count", "string", "reviews_count", "string"), ("qualidade_de_vida", "string", "qualidade_de_vida", "string"), ("culture_count", "string", "culture_count", "string"), ("alta_lideranca", "string", "alta_lideranca", "string"), ("nome", "string", "nome", "string"), ("employer_name", "string", "employer_name", "string"), ("cnpj", "string", "cnpj", "string"), ("right_ano", "string", "ano", "string"), ("right_trimestre", "string", "trimestre", "string"), ("right_categoria", "string", "categoria", "string"), ("right_tipo", "string", "tipo", "string"), ("right_indice", "string", "right_indice", "string"), ("right_quantidade_reclamacoes_reguladas_procedentes", "string", "quantidade_reclamacoes_reguladas_procedentes", "string"), ("right_quantidade_reclamacoes_reguladas_outras_outras", "string", "quantidade_reclamacoes_reguladas_outras_outras", "string"), ("right_quantidade_reclamacoes_nao_reguladas", "string", "quantidade_reclamacoes_nao_reguladas", "string"), ("right_quantidade_total_reclamacoes", "string", "quantidade_total_reclamacoes", "string"), ("right_quantidade_total_clientes_ccs_scr", "string", "quantidade_total_clientes_ccs_scr", "string"), ("right_quantidade_clientes_ccs", "string", "right_quantidade_clientes_ccs", "string"), ("right_quantidade_clientes_scr", "string", "quantidade_clientes_scr", "string")], transformation_ctx="bancos_final_schema_node1725317309464")

# Script generated for node parquet_final
parquet_final_node1725317444896 = glueContext.getSink(path="s3://328145136678/Delivery/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="parquet_final_node1725317444896")
parquet_final_node1725317444896.setCatalogInfo(catalogDatabase="bancos",catalogTableName="bancos_final")
parquet_final_node1725317444896.setFormat("glueparquet", compression="uncompressed")
parquet_final_node1725317444896.writeFrame(bancos_final_schema_node1725317309464)
# Script generated for node MySQL
MySQL_node1725321241718 = glueContext.write_dynamic_frame.from_catalog(frame=bancos_final_schema_node1725317309464, database="bancos_mysql", table_name="bancos_final", transformation_ctx="MySQL_node1725321241718")

job.commit()