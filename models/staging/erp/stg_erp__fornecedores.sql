WITH 
    fonte_fornecedores AS (
        SELECT 
            cast(C1 as int) as pk_fornecedor, 
            cast(C2 as varchar) as nm_fornecedor, 
            cast(C6 as varchar) as cidade_fornecedor,
            cast(C9 as varchar) as pais_fornecedor
        FROM {{ source('erp', 'supplier') }}
        WHERE c1 != 'Id'
    )
SELECT *
FROM fonte_fornecedores