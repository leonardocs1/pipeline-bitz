CREATE OR REFRESH LIVE TABLE bitz.silver.silver_fact_dre 
PARTITIONED BY (ano_mes_conta)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS
SELECT
    t1._pk AS dre_sk,
    t1._erathos_execution_id AS erathos_execution_id,
    CAST(t1._erathos_synced_at AS TIMESTAMP) AS erathos_synced_at,
    CAST(t1.cod_filial AS INT) AS cod_filial,
    UPPER(TRIM(t1.centro_custo)) AS centro_custo,
    
    -- Normalização via tabela
    UPPER(TRIM(t1.descricao)) AS descricao_original,
    COALESCE(
      t2.descricao_normalizada,
      UPPER(TRIM(t1.descricao))
    ) AS descricao,
    
    TO_DATE(t1.dtaconta) AS dtaconta,
    CAST(ABS(t1.valor) AS DECIMAL(18,2)) AS vlr_total,
    
    CASE
        WHEN t1.valor < 0 THEN 'DESPESA'
        ELSE 'RECEITA'
    END AS tipo,
    
    current_timestamp() AS ingestion_time,
    'erathos' AS source_system,
    DATE_FORMAT(TO_DATE(t1.dtaconta), 'yyyy-MM') AS ano_mes_conta

FROM bitz.raw_gin_premier.dre AS t1
LEFT JOIN bitz.silver.dim_normalizacao_descricao AS t2
  ON UPPER(TRIM(t1.descricao)) = t2.descricao_original_upper
WHERE t1.dtaconta IS NOT NULL;