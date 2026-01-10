CREATE OR REFRESH LIVE TABLE bitz.silver.silver_fact_contas_pagar_fcp
PARTITIONED BY (ano_mes)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'  = 'true'
)
AS
SELECT
    TRIM(UPPER(tipo))                          AS tipo,
    TO_DATE(dtaconta)                          AS dtaconta,
    TO_DATE(dtavcto)                           AS dtavcto,
    TO_DATE(dtapgto)                           AS dtapgto,
    TO_DATE(dtcompet)                          AS dtacompet,
    TO_DATE(_data_referencia)                  AS data_referencia,
    CAST(id_empresa AS INT)                    AS id_empresa,
    CAST(id_codigo_plano AS INT)               AS id_codigo_plano,
    TRIM(UPPER(planoconta))                    AS planoconta,
    CAST(id_fornecedor AS INT)                 AS id_cliente,
    TRIM(UPPER(nome_fornecedor))               AS nome_cliente,
    CAST(valor AS DECIMAL(18, 2))              AS valor,
    CAST(valor_item_rateio AS DECIMAL(18, 2))  AS valor_item_rateio,
    CAST(valor_pago AS DECIMAL(18, 2))             AS vlrpago,
    CAST(vlr_pago_item_rateio AS DECIMAL(18, 2)) AS vlr_pago_item_rateio,
    CAST(nrodoc AS INT)                        AS nrodoc,
    TRIM(UPPER(historico))                     AS historico,
    TRIM(UPPER(tipo_conta))                    AS tipo_conta,
    TRIM(UPPER(tipo_conta_descricao))          AS tipo_conta_descricao,
    CAST(id_centro_custo AS INT)               AS id_centro_custo,
    TRIM(UPPER(centro_custo_descricao))        AS centro_custo_descricao,
    TRIM(UPPER(conta_baixa_descricao))         AS conta_baixa_descricao,
    CAST(_ingestion_timestamp AS TIMESTAMP)    AS ingestion_time,
    ano_mes
FROM bitz.bronze.fact_contas_pagar_fcp
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY
            nrodoc,
            id_cliente,
            id_empresa,
            dtavcto
        ORDER BY _ingestion_timestamp DESC
    ) = 1;
    
