CREATE OR REFRESH MATERIALIZED VIEW bitz.gold.dim_centro_custo AS
SELECT DISTINCT
    centro_custo
FROM bitz.silver.silver_dre
WHERE centro_custo IS NOT NULL
