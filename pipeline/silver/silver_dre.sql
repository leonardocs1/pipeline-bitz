CREATE OR REFRESH MATERIALIZED VIEW bitz.silver.silver_dre 
AS
SELECT
    _pk AS dre_sk,
    _erathos_execution_id AS erathos_execution_id,
    CAST(_erathos_synced_at AS TIMESTAMP) AS erathos_synced_at,
    CAST(cod_filial AS INT) AS cod_filial,
    UPPER(centro_custo) AS centro_custo,

    CASE
        WHEN UPPER(descricao) = 'TV POR ASSISNATURA' THEN 'TV POR ASSINATURA'
        WHEN UPPER(descricao) = 'EPI''S' THEN 'EPIS'
        WHEN UPPER(descricao) = 'EPI´S' THEN 'EPIS'
        WHEN UPPER(descricao) = 'ICMS (IMPOSTO SOBRE CIRCULACAO DE MERCADORIAS E PRESTACAO DE SERVICOS)' THEN 'ICMS'
        WHEN UPPER(descricao) = '13° SALARIO' THEN '13 SALARIO'
        WHEN UPPER(descricao) = 'SPOTIFY / GOOGLE' THEN 'SPOTIFY'
        ELSE UPPER(descricao)
    END AS descricao,

    TO_DATE(dtaconta) AS dtaconta,

    CAST(ABS(valor) AS DECIMAL(18,2)) AS vlr_total,

    CASE
        WHEN valor < 0 THEN 'DESPESA'
        ELSE 'RECEITA'
    END AS tipo,

    current_timestamp() AS ingestion_time,
    'erathos' AS source_system

FROM bitz.raw_gin_premier.dre;
