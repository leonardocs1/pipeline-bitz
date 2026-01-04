CREATE OR REFRESH MATERIALIZED VIEW bitz.gold.dim_centro_custo_dre AS
SELECT DISTINCT
    centro_custo
FROM bitz.silver.silver_fact_dre
WHERE centro_custo IS NOT NULL
