CREATE OR REFRESH MATERIALIZED VIEW bitz.gold.fact_dre AS

SELECT
    t1.cod_filial,
    t1.centro_custo,
    t1.descricao AS nome_plano,
    t1.dtaconta,
    t1.vlr_total,
    t1.tipo,
    t2.cod_plano,
    t2.cod_raiz,
    t2.cod_grupo,
    t2.nivel_1,
    t2.nivel_2,
    t2.nivel_3
FROM bitz.silver.fact_dre AS t1
LEFT JOIN bitz.silver.dim_planos_dre AS t2
       ON t1.descricao = t2.nivel_3;
