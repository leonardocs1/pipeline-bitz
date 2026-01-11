CREATE OR REFRESH MATERIALIZED VIEW bitz.gold.fact_fcp AS

WITH tabela AS (

    SELECT
        t1.tipo,
        t1.dtaconta,
        t1.dtavcto,
        t1.dtapgto,
        t1.dtacompet,
        t1.data_referencia,
        t1.id_empresa,
        t1.id_codigo_plano,
        t1.planoconta,
        t1.id_cliente,
        t1.nome_cliente,
        CAST(NULL AS DECIMAL(18, 2)) AS valor_doc,
        CAST(NULL AS DECIMAL(18, 2)) AS valor_parc,
        t1.valor,
        t1.valor_item_rateio,
        t1.vlrpago,
        t1.vlr_pago_item_rateio,
        t1.nrodoc,
        t1.historico,
        t1.tipo_conta,
        t1.tipo_conta_descricao,
        t1.id_centro_custo,
        t1.centro_custo_descricao,
        t1.conta_baixa_descricao
    FROM bitz.silver.fact_contas_pagar_fcp AS t1

    UNION ALL

    SELECT
        t2.tipo,
        t2.dtaconta,
        t2.dtavcto,
        t2.dtapgto,
        t2.dtacompet,
        t2.data_referencia,
        t2.id_empresa,
        t2.id_codigo_plano,
        t2.planoconta,
        t2.id_cliente,
        t2.nome_cliente,
        t2.valor_doc,
        t2.valor_parc,
        t2.valor,
        t2.valor_item_rateio,
        t2.vlrpago,
        t2.vlr_pago_item_rateio,
        t2.nrodoc,
        t2.historico,
        t2.tipo_conta,
        t2.tipo_conta_descricao,
        t2.id_centro_custo,
        t2.centro_custo_descricao,
        t2.conta_baixa_descricao
    FROM bitz.silver.fact_contas_receber_fcp AS t2

)

SELECT
    t1.tipo,
    t1.dtaconta,
    t1.dtavcto,
    t1.dtapgto,
    t1.dtacompet,
    t1.data_referencia,
    t1.id_empresa,
    t1.id_codigo_plano,
    t1.planoconta,
    t1.id_cliente,
    t1.nome_cliente,
    t1.valor_doc,
    t1.valor_parc,
    t1.valor,
    t1.valor_item_rateio,
    t1.vlrpago,
    t1.vlr_pago_item_rateio,
    t1.nrodoc,
    t1.historico,
    t1.tipo_conta,
    t1.tipo_conta_descricao,
    t1.id_centro_custo,
    t1.centro_custo_descricao,
    t1.conta_baixa_descricao,
    t2.cod_plano,
    t2.cod_raiz,
    t2.cod_grupo,
    t2.nivel_1,
    t2.nivel_2,
    t2.nivel_3
FROM tabela AS t1
LEFT JOIN bitz.silver.dim_planos_fcp AS t2
    ON t1.id_codigo_plano = t2.cod_plano;
