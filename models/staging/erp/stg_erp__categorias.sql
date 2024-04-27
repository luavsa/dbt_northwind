WITH 
    fonte_categorias AS (
        SELECT 
            cast(id AS int) AS pk_categoria,
            cast(categoryname AS varchar) AS nm_categoria,
            cast(description AS varchar) AS descricao_categoria
        FROM {{ source('erp', 'category') }}
    )
SELECT *
FROM fonte_categorias