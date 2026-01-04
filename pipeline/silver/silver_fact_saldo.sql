CREATE OR REFRESH MATERIALIZED VIEW bitz.silver.silver_fact_saldo
AS
SELECT 
  id,
  TO_DATE(data) AS data,
  UPPER(descricao) AS descricao,
  CAST(saldo_dia AS DECIMAL(18, 2)) AS saldo_dia,
  CAST(ingestion_time AS TIMESTAMP) AS ingestion_time
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY descricao, data 
      ORDER BY data DESC NULLS LAST, ingestion_time DESC NULLS LAST
    ) AS rn
  FROM bitz.bronze.fact_saldo
)
WHERE rn = 1;