CREATE OR REFRESH STREAMING TABLE bitz.bronze.fact_saldo
PARTITIONED BY (ano_mes) 
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.logRetentionDuration' = '30 days',
  'delta.deletedFileRetentionDuration' = '7 days',
  'delta.enableChangeDataFeed' = 'true'
)
AS
SELECT
  id,
  data,
  descricao,
  CAST(
    REPLACE(saldo_dia, ',', '.') AS DOUBLE
    ) AS saldo_dia,
  _data_referencia,
  _ingestion_timestamp,
  
  -- Metadados de ingestão
  _metadata.file_path AS source_file,
  _metadata.file_name AS source_file_name,
  _metadata.file_modification_time AS file_modified_time,
  
  -- Coluna de particionamento (formato YYYY-MM)
  DATE_FORMAT(TO_DATE(_data_referencia), 'yyyy-MM') AS ano_mes,
  
  -- Timestamp de processamento
  current_timestamp() AS processed_at

FROM STREAM read_files(
  '/Volumes/bitz/raw/saldo',
  format => 'json',
  inferColumnTypes => true
);