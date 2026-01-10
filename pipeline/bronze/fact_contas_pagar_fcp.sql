CREATE OR REFRESH STREAMING TABLE bitz.bronze.fact_contas_pagar_fcp
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
  tipo::STRING,
  dtaconta::STRING,
  id_empresa::STRING,
  id_codigo_plano::STRING,
  planoconta::STRING,
  id_fornecedor::STRING,
  nome_fornecedor::STRING,
  dtavcto::STRING,
  dtapgto::STRING,
  dtcompet::STRING,
  valor::STRING,
  valor_pago::STRING,
  valor_item_rateio::STRING,
  vlr_pago_item_rateio::STRING,
  nrodoc::STRING,
  historico::STRING,
  tipo_conta::STRING,
  tipo_conta_descricao::STRING,
  id_centro_custo::STRING,
  centro_custo_descricao::STRING,
  conta_baixa_descricao::STRING,
  _data_referencia::STRING,
  _ingestion_timestamp::STRING,
  _metadata.file_path AS _source_file,
  _metadata.file_modification_time AS _file_timestamp,

   -- Coluna de particionamento (formato YYYY-MM)
  DATE_FORMAT(TO_DATE(dtavcto), 'yyyy-MM') AS ano_mes,

  current_timestamp() AS _ingestion_timestamp_table
FROM STREAM(
  read_files(
  '/Volumes/bitz/raw/contas_pagar_fcp',
  format => 'json',
  multiLine => true,
  inferSchema => false
));