with
    cabecalho as (
        select *
        from {{ ref('stg_salesorderheader') }}
    )

    , pedido_itens as (
        select *
        from {{ ref('stg_salesorderdetail') }}
    )

    produto as (
        select *
        from {{ ref('dim_product') }}
    )

    , territorio as (
        select *
        from {{ ref('dim_address') }}
    )

    , motivo_venda as (
        select * 
        from {{ ref('dim_salesreason')}}
    )

    , cartao as (
        select * 
        from {{ ref('dim_creditcard')}}
    )

    , motivo_venda_cabecalho as (
        select * 
        from {{ ref('stg_salesorderheadersalesreason')}}
    )

    , uniao_tabelas as (
        select
            row_number() over (order by id_cliente) as pk_pedido
            , cabecalho.id_pedido
            , cabecalho.id_cliente
            , pedido_itens.id_produto            
            , cabecalho.id_territorio      
            , cabecalho.id_cartao 
            , cabecalho.data_pedido        
            , cabecalho.status_pedido       
            , cabecalho.valor_total
            , pedido_itens.quantidade
            , pedido_itens.preco_unitario
            , pedido_itens.desconto_unitario
        from cabecalho
        left join pedido_itens on cabecalho.id_pedido = pedido_itens.id_pedido
    )

select *
from uniao_tabelas