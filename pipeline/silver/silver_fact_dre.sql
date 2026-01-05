CREATE OR REFRESH MATERIALIZED VIEW bitz.silver.silver_fact_dre 
PARTITIONED BY (ano_mes_conta)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS
SELECT
    _pk AS dre_sk,
    _erathos_execution_id AS erathos_execution_id,
    CAST(_erathos_synced_at AS TIMESTAMP) AS erathos_synced_at,
    CAST(cod_filial AS INT) AS cod_filial,
    UPPER(TRIM(centro_custo)) AS centro_custo,
    UPPER(TRIM(descricao)) AS descricao_original,
    TO_DATE(dtaconta) AS dtaconta,
    CAST(ABS(valor) AS DECIMAL(18,2)) AS vlr_total,
    
    CASE
        WHEN valor < 0 THEN 'DESPESA'
        ELSE 'RECEITA'
    END AS tipo,
    
    current_timestamp() AS ingestion_time,
    'erathos' AS source_system,
    
    -- Coluna de particionamento
    DATE_FORMAT(TO_DATE(dtaconta), 'yyyy-MM') AS ano_mes_conta

FROM bitz.raw_gin_premier.dre
WHERE dtaconta IS NOT NULL;