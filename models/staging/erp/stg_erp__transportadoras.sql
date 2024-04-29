WITH 
    fonte_transportadoras AS (
        SELECT
            CAST(ID AS int) AS pk_transportadora,
            CAST(COMPANYNAME AS varchar) AS nm_transportadora,
            CAST(PHONE AS string) AS telefone_transportadora
        FROM {{ source('erp', 'shipper') }}
    )

SELECT *
FROM fonte_transportadoras