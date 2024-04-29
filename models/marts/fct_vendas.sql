WITH 
    pedido_por_itens AS (
        SELECT *
        FROM {{ ref('int_pedido_por_itens') }}
    ),

    dim_produtos AS (
        SELECT *
        FROM {{ ref('dim_produtos') }}
    ),

    dim_funcionarios AS (
        SELECT *
        FROM {{ ref('dim_funcionarios') }}
    ),

    dim_clientes AS (
        SELECT *
        FROM {{ ref('dim_clientes') }}
    ),

    dim_transportadoras AS (
        SELECT * 
        FROM {{ ref('dim_transportadoras') }}
    ),

    joined AS (
        SELECT
            fatos.sk_vendas,
            fatos.FK_PEDIDO as numero_nota_fiscal,
            fatos.FK_PRODUTO,
            fatos.FK_FUNCIONARIO,
            fatos.FK_CLIENTE,
            fatos.FK_TRANSPORTADORA,
            fatos.DESCONTO_PERC,
            fatos.PRECO_DA_UNIDADE,
            fatos.QUANTIDADE,
            fatos.DATA_DO_PEDIDO,
            fatos.FRETE,
            dim_clientes.NM_CLIENTE,
            dim_clientes.CIDADE_CLIENTE,
            dim_clientes.PAIS_CLIENTE,
            dim_clientes.REGIAO_CLIENTE,
            fatos.DATA_DO_ENVIO,
            fatos.DATA_REQUERIDA_ENTREGA,
            dim_produtos.NM_PRODUTO,
            dim_produtos.IS_DISCONTINUADO,
            dim_produtos.NM_CATEGORIA,
            dim_produtos.DESCRICAO_CATEGORIA,
            dim_produtos.NM_FORNECEDOR,
            dim_produtos.CIDADE_FORNECEDOR,
            dim_produtos.PAIS_FORNECEDOR,
            dim_transportadoras.NM_TRANSPORTADORA,
            dim_funcionarios.NM_FUNCIONARIO,
            dim_funcionarios.CARGO_FUNCIONARIO,
            dim_funcionarios.DT_CONTRATACAO,
            dim_funcionarios.NM_GERENTE
        FROM pedido_por_itens AS fatos
        LEFT JOIN dim_produtos 
            ON fatos.fk_produto = dim_produtos.pk_produto
        LEFT JOIN dim_funcionarios
            ON fatos.fk_funcionario = dim_funcionarios.pk_funcionario
        LEFT JOIN dim_clientes
            ON fatos.FK_CLIENTE = dim_clientes.pk_cliente
        LEFT JOIN dim_transportadoras
            ON fatos.fk_transportadora = dim_transportadoras.PK_TRANSPORTADORA
    ),

    metricas AS (
        SELECT
            sk_vendas,
            FK_PRODUTO,
            FK_FUNCIONARIO,
            FK_CLIENTE,
            FK_TRANSPORTADORA,
            numero_nota_fiscal,
            DESCONTO_PERC,
            PRECO_DA_UNIDADE,
            QUANTIDADE,
            DATA_DO_PEDIDO,
            DATA_DO_ENVIO,
            DATA_REQUERIDA_ENTREGA,
            NM_CLIENTE,
            CIDADE_CLIENTE,
            PAIS_CLIENTE,
            REGIAO_CLIENTE,
            NM_PRODUTO,
            IS_DISCONTINUADO,
            NM_CATEGORIA,
            DESCRICAO_CATEGORIA,
            NM_FORNECEDOR,
            CIDADE_FORNECEDOR,
            PAIS_FORNECEDOR,
            NM_TRANSPORTADORA,
            NM_FUNCIONARIO,
            CARGO_FUNCIONARIO,
            DT_CONTRATACAO,
            NM_GERENTE,
            quantidade * preco_da_unidade AS valor_bruto,
            quantidade * (1 - desconto_perc) * preco_da_unidade AS valor_liquido,
            CAST(
                (dt_contratacao - data_do_pedido) / 365 
                AS numeric(18,2)
            )  AS senioridade_em_anos,
            CAST(
                frete / COUNT(numero_nota_fiscal) OVER (PARTITION BY numero_nota_fiscal)
                AS numeric(18,2)
            ) AS frete_rateado
        FROM joined
    )

SELECT *
FROM metricas

