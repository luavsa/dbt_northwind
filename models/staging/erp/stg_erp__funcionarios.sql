WITH 
    fonte_funcionarios AS (
        SELECT 
            CAST(id as int) as pk_funcionario,
            CAST(reportsto as int) as fk_gerente, -- Coluna "reportsto" é o id que o funcionário tem que de superior
            CAST(firstname as string) || ' ' || CAST(lastname as string) as nm_funcionario,
            CAST(title as string) as cargo_funcionario,
            CAST(birthdate as date) as dt_nascimento_funcionario,
            CAST(hiredate as date) as dt_contratacao
        FROM {{ source('erp', 'employee') }}
    )
SELECT *
FROM fonte_funcionarios