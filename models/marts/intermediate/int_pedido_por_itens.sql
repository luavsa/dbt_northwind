WITH 
    ordens AS (
        SELECT *
        FROM {{ ref('stg_erp__ordens') }}
    ),

    ordem_detalhes AS (
        SELECT *
        FROM {{ ref('stg_erp__ordem_detalhes') }}
    ),

    joined AS (
        SELECT 
            ordem_detalhes.FK_PEDIDO,
            ordem_detalhes.FK_PRODUTO,
            ordens.FK_FUNCIONARIO,
            ordens.FK_CLIENTE,
            ordens.FK_TRANSPORTADORA,
            ordem_detalhes.DESCONTO_PERC,
            ordem_detalhes.PRECO_DA_UNIDADE,
            ordem_detalhes.QUANTIDADE,
            ordens.DATA_DO_PEDIDO,
            ordens.FRETE,
            ordens.NM_DESTINATARIO,
            ordens.CIDADE_DESTINATARIO,
            ordens.REGIAO_DESTINATARIO,
            ordens.PAIS_DESTINATARIO,
            ordens.DATA_DO_ENVIO,
            ordens.DATA_REQUERIDA_ENTREGA
        FROM ordem_detalhes
        LEFT JOIN ordens
            ON ordem_detalhes.fk_pedido = ordens.pk_pedido
    ),

    criada_chave_primaria AS (
        SELECT
            CAST(FK_PEDIDO AS varchar) || '-' || FK_PRODUTO::varchar AS sk_vendas
            , *
        FROM joined
    )

SELECT *
FROM criada_chave_primaria