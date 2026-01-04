CREATE OR REFRESH MATERIALIZED VIEW bitz.silver.dim_planos_dre AS

WITH tab_nivel_2 AS (
    SELECT
        codigo,
        grupo,
        nsubgrupo AS nivel_2
    FROM bitz.raw_gin_premier.mov_vinc_dre
    WHERE ngrupo <> 'SubGrupo'
),

tab_nivel_3 AS (
    SELECT
        id,
        codigo,
        grupo,
        nsubgrupo AS nivel_3
    FROM bitz.raw_gin_premier.mov_vinc_dre
    WHERE ngrupo = 'SubGrupo'
)

SELECT
    CAST(t1.id     AS INT) AS cod_plano,
    CAST(t1.codigo AS INT) AS cod_raiz,
    CAST(t1.grupo  AS INT) AS cod_grupo,
    t3.nome                 AS nivel_1,
    t2.nivel_2,
    t1.nivel_3
FROM tab_nivel_3 AS t1
LEFT JOIN tab_nivel_2 AS t2
       ON t1.codigo = t2.codigo
      AND t1.grupo  = t2.grupo
LEFT JOIN bitz.silver.dim_dre_nivel_0 AS t3
       ON t1.codigo = t3.codigo;
