WITH
    funcionarios AS (
        SELECT *
        FROM {{ ref('stg_erp__funcionarios') }}
    ),

    joined AS (
        SELECT
            funcionarios.PK_FUNCIONARIO,
            funcionarios.NM_FUNCIONARIO,
            funcionarios.CARGO_FUNCIONARIO,
            funcionarios.DT_NASCIMENTO_FUNCIONARIO,
            funcionarios.DT_CONTRATACAO,
            gerentes.NM_FUNCIONARIO as nm_gerente,
            gerentes.CARGO_FUNCIONARIO as cargo_gerente
        FROM funcionarios
        LEFT JOIN funcionarios AS gerentes 
            ON  funcionarios.fk_gerente = gerentes.pk_funcionario
    )

SELECT *
FROM joined