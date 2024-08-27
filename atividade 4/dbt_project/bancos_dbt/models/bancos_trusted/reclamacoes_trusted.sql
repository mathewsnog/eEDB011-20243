SELECT 
Ano as ano
,REPLACE(Trimestre, '�', '') as trimestre
,Categoria as categoria
,Tipo as tipo
,`CNPJ IF` as cnpj
,REPLACE(`Instituição financeira`, ' (conglomerado)','') as nome_banco  
,`Índice` as indice  
,`Quantidade de reclamações reguladas procedentes` as quantidade_reclamacoes_reguladas_procedentes  
,`Quantidade de reclamações reguladas - outras` as quantidade_reclamacoes_reguladas_outras  
,`Quantidade de reclamações não reguladas` as quantidade_reclamacoes_nao_reguladas  
,`Quantidade total de reclamações` as quantidade_total_reclamacoes  
,`Quantidade total de clientes  CCS e SCR` as quantidade_total_clientes_ccs_scr  
,`Quantidade de clientes  CCS` as quantidade_clientes_ccs  
,`Quantidade de clientes  SCR` as quantidade_clientes_scr 
FROM {{ ref('reclamacoes_raw') }}