WITH 
    fonte_ordem_detalhes AS (
        SELECT 
            CAST(orderid as int) as fk_pedido,
            CAST(productid as int) as fk_produto,
            CAST(discount as numeric(18,2)) as desconto_perc,
            CAST(unitprice as numeric(18,2)) as preco_da_unidade,
            CAST(quantity as int) as quantidade
        FROM {{ source('erp', 'orderdetail') }}
    )

SELECT *
FROM fonte_ordem_detalhes