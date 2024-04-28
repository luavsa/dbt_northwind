WITH 
    fonte_produtos AS (
        SELECT 
            CAST(ID as int) as pk_produto,
            CAST(SUPPLIERID as int) as fk_fornecedor,
            CAST(CATEGORYID as int) as fk_categoria,
            CAST(PRODUCTNAME as string) as nm_produto,
            CAST(QUANTITYPERUNIT as string) as quantidade_por_unidade,
            CAST(UNITPRICE as numeric(18,2)) as preco_por_unidade,
            CAST(UNITSINSTOCK as int) as unidade_em_estoque,
            CAST(UNITSONORDER as int) as unidade_por_pedido,
            CAST(REORDERLEVEL as int) as nivel_de_pedido,
        CASE
            WHEN DISCONTINUED = 0 THEN 'Não'
            WHEN DISCONTINUED = 1 THEN 'Sim'
            END AS is_discontinuado
        FROM {{ source('erp', 'product') }}
    )
SELECT *
FROM fonte_produtos