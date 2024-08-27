SELECT
    Segmento as segmento,
    CNPJ as cnpj,
    REPLACE(Nome, ' - PRUDENCIAL', '') as nome_banco
FROM {{ ref('bancos_raw') }}