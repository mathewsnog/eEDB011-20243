SELECT 
 bt.ano
,bt.trimestre
,bt.categoria
,bt.tipo
,bt.cnpj
,bt.nome_banco
,bt.quantidade_reclamacoes_reguladas_procedentes
,bt.quantidade_reclamacoes_reguladas_outras
,bt.quantidade_reclamacoes_nao_reguladas
,bt.quantidade_total_reclamacoes
,bt.quantidade_total_clientes_ccs_scr
,bt.quantidade_clientes_ccs
,bt.quantidade_clientes_scr
,rt.segmento
,et.employer_name
,et.reviews_count
,et.culture_count
,et.salaries_count
,et.benefits_count
,et.employer_website
,et.employer_headquarters
,et.employer_industry
,et.employer_revenue
,et.url
,et.geral
,et.cultura_e_valores
,et.diversidade_e_inclusao
,et.qualidade_de_vida
,et.alta_lideranca
,et.remuneracao_e_beneficios
,et.oportunidades_de_carreira
,et.recomendam_para_outras_pessoas
,et.perspectiva_positiva_da_empresa
,et.match_percent
FROM {{ ref('reclamacoes_trusted') }} bt  
INNER JOIN {{ ref('bancos_trusted') }} rt ON rt.cnpj = bt.cnpj  
INNER JOIN {{ ref('empregados_trusted') }} et ON bt.nome_banco = et.nome_banco