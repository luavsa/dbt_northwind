WITH 
    fonte_clientes AS (
        SELECT
            CAST(ID AS varchar) AS pk_cliente,
            CAST(COMPANYNAME AS varchar) AS nm_cliente,
            CAST(CONTACTNAME AS varchar) AS nm_contato_cliente,
            CAST(CONTACTTITLE AS varchar) AS contato_cargo_cliente,
            CAST(ADDRESS AS string) AS endereco_cliente,
            CAST(CITY AS varchar) AS cidade_cliente,
            CAST(REGION AS varchar) AS regiao_cliente,
            CAST(POSTALCODE AS string) AS cep_cliente,
            CAST(COUNTRY AS varchar) AS pais_cliente,
            CAST(PHONE AS string) AS telefone_cliente,
            CAST(FAX AS string) AS fax_cliente
        FROM {{ source('erp', 'customer') }}
    )

SELECT *
FROM fonte_clientes