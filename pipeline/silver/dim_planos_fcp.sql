CREATE OR REFRESH MATERIALIZED VIEW bitz.silver.dim_planos_fcp
AS

WITH tab_nivel_2 AS (
    SELECT
        cod_raiz,
        cod_grupo,
        nsubgrupo AS nivel_2
    FROM bitz.raw_gin_premier.mov_vinc_fcp
    WHERE ngrupo <> 'SubGrupo'
),

tab_nivel_3 AS (
    SELECT
        cod_plano,
        cod_raiz,
        cod_grupo,
        nsubgrupo AS nivel_3
    FROM bitz.raw_gin_premier.mov_vinc_fcp
    WHERE ngrupo = 'SubGrupo'
)

SELECT
    CAST(t1.cod_plano AS INT) AS cod_plano,
    CAST(t1.cod_raiz  AS INT) AS cod_raiz,
    CAST(t1.cod_grupo AS INT) AS cod_grupo,
    t3.nome                  AS nivel_1,
    t2.nivel_2,
    t1.nivel_3
FROM tab_nivel_3 AS t1
LEFT JOIN tab_nivel_2 AS t2
       ON t1.cod_raiz  = t2.cod_raiz
      AND t1.cod_grupo = t2.cod_grupo
LEFT JOIN bitz.silver.dim_fcp_nivel_0 AS t3
       ON t1.cod_raiz = t3.codigo;
