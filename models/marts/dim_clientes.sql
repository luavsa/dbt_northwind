WITH
    clientes AS (
        SELECT *
        FROM {{ ref('stg_erp__clientes') }}
    ),

    joined AS (
        SELECT
            clientes.PK_CLIENTE,
            clientes.NM_CLIENTE,
            clientes.CONTATO_CARGO_CLIENTE,
            clientes.CIDADE_CLIENTE,
            clientes.PAIS_CLIENTE,
            clientes.REGIAO_CLIENTE
        FROM clientes
    )

SELECT *
FROM joined

