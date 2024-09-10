SELECT 
 employer_name  
,reviews_count  
,culture_count  
,salaries_count  
,benefits_count  
,`employer-website` as employer_website   
,`employer-headquarters` as employer_headquarters
,`employer-founded` as employer_founded
,`employer-industry` as employer_industry  
,`employer-revenue` as employer_revenue
,`url`  
,Geral as geral 
,`Cultura e valores` as cultura_e_valores
,`Diversidade e inclusão` as diversidade_e_inclusao
,`Qualidade de vida` as qualidade_de_vida
,`Alta liderança` as alta_lideranca
,`Remuneração e benefícios` as remuneracao_e_beneficios
,`Oportunidades de carreira` as oportunidades_de_carreira
,`Recomendam para outras pessoas(%)` as recomendam_para_outras_pessoas
,`Perspectiva positiva da empresa(%)` as perspectiva_positiva_da_empresa
,Segmento as segmento
,Nome as nome_banco
,match_percent as match_percent  
,CNPJ as cnpj
FROM {{ ref('empregados_raw') }}