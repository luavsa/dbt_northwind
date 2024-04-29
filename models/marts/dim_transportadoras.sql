WITH
    transportadoras AS (
        SELECT *
        FROM {{ ref('stg_erp__transportadoras') }}
    ),

    joined AS (
        SELECT
            transportadoras.PK_TRANSPORTADORA,
            transportadoras.NM_TRANSPORTADORA
        FROM transportadoras
    )

SELECT *
FROM joined

