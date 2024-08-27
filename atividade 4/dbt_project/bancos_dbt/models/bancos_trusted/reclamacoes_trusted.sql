SELECT 
Ano as ano
,REPLACE(Trimestre, '�', '') as trimestre
,Categoria as categoria
,Tipo as tipo
,`CNPJ IF` as cnpj
,REPLACE(instituicao_financeira, ' (conglomerado)','') as nome_banco  
,indice  
,quantidade_reclamacoes_reguladas_procedentes  
,quantidade_reclamacoes_reguladas_outras  
,quantidade_reclamacoes_nao_reguladas  
,quantidade_total_reclamacoes  
,quantidade_total_clientes_ccs_scr  
,quantidade_clientes_ccs  
,quantidade_clientes_scr 
FROM {{ ref('reclamacoes_raw') }}