CREATE OR REFRESH LIVE TABLE bitz.silver.silver_fact_saldo
PARTITIONED BY (ano_mes)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS
SELECT 
  id,
  TO_DATE(data) AS data,
  UPPER(TRIM(descricao)) AS descricao,
  CAST(saldo_dia AS DECIMAL(18, 2)) AS saldo_dia,
  CAST(_ingestion_timestamp AS TIMESTAMP) AS ingestion_time,
  ano_mes
FROM bitz.bronze.fact_saldo
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY UPPER(TRIM(descricao)), TO_DATE(data)
  ORDER BY _ingestion_timestamp DESC
) = 1;