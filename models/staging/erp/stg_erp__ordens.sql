WITH 
    fonte_ordens AS (
        SELECT 
            CAST(id AS int) pk_pedido,
            CAST(employeeid AS int) AS fk_funcionario,
            CAST(customerid AS string) AS fk_cliente,
            CAST(shipvia AS int) AS fk_transportadora,
            CAST(orderdate AS date) AS data_do_pedido,
            CAST(freight AS numeric) AS frete,
            CAST(shipname AS string) AS nm_destinatario,
            CAST(shipcity AS string) AS cidade_destinatario,
            CAST(shipregion AS string) AS regiao_destinatario,
            CAST(shipcountry AS string) AS pais_destinatario,
            CAST(shippeddate AS date) AS data_do_envio,
            CAST(requireddate AS date) AS data_requerida_entrega
        FROM {{ source('erp', '_order_') }}
    )
SELECT *
FROM fonte_ordens