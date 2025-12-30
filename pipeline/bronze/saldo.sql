CREATE OR REFRESH STREAMING TABLE bitz.bronze.saldo AS
SELECT
  *,
  _metadata.file_path           AS source_file,
  _metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  '/Volumes/bitz/raw/saldo',
  format => 'json',
  inferColumnTypes => false
);