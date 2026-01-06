CREATE OR REFRESH LIVE TABLE bitz.silver.silver_fact_dfc
PARTITIONED BY (ano_mes_pgto)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS
SELECT
    t1._pk AS dfc_sk,
    t1._erathos_execution_id AS erathos_execution_id,
    CAST(t1._erathos_synced_at AS TIMESTAMP) AS erathos_synced_at,
    CAST(t1.cod_filial AS INT) AS cod_filial,
    UPPER(TRIM(t1.centro_custo)) AS centro_custo,
    UPPER(TRIM(t1.descricao)) AS descricao_original,
    
    COALESCE(
      t2.descricao_normalizada,  -- Usa normalização se existir
      UPPER(TRIM(t1.descricao))  -- Senão, usa original
    ) AS descricao,
    
    TO_DATE(t1.dtapgto) AS dtapgto,
    CAST(ABS(t1.valor) AS DECIMAL(18,2)) AS vlr_total,
    
    CASE
        WHEN CAST(t1.valor AS DECIMAL(18, 2)) < 0 THEN 'DESPESA'
        ELSE 'RECEITA'
    END AS tipo,
    
    current_timestamp() AS ingestion_time,
    'erathos' AS source_system,
    DATE_FORMAT(TO_DATE(t1.dtapgto), 'yyyy-MM') AS ano_mes_pgto

FROM bitz.raw_gin_premier.dfc AS t1
LEFT JOIN bitz.silver.dim_normalizacao_descricao AS t2
  ON UPPER(TRIM(t1.descricao)) = UPPER(t2.descricao_original)
WHERE t1.dtapgto IS NOT NULL;