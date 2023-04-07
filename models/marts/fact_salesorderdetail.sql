with

    dim_address as (
        select
            id_endereco
            , id_estado
            , cidade
        from {{ ref('dim_address') }}
    )
    , dim_creditcard as (
        select 
            pk_cartao
            , id_cartao
            , tipo_cartao
        from {{ ref('dim_creditcard')}}
    )
    , dim_customer as (
        select
            pk_cliente
            , id_cliente
            , cliente_nome
            , nome_completo
        from {{ ref('dim_customer') }}
    )
    , dim_product as (
        select 
            pk_produto
            , id_produto
            , nome_produto 
        from {{ ref('dim_product') }}
    )
    , dim_salesreason as (
        select 
            pk_motivo
            , id_motivo
            , id_pedido
        from {{ ref('dim_salesreason')}}
    )
    , pedido_itens as (
        select
            id_pedido
            , id_produto
            , quantidade
            , preco_unitario
            , desconto_unitario
        from {{ ref('stg_salesorderdetail') }}
    )
    , cabecalho as (
        select 
            id_pedido
            , id_cliente
            , id_territorio
            , id_shiptoaddress
            , id_billtoaddress
            , id_cartao
            , data_pedido
            , status_pedido
            , valor_total
        from {{ ref('stg_salesorderheader') }}
    )
    , motivo_venda_cabecalho as (
        select 
            id_pedido
            , id_motivo
        from {{ ref('stg_salesorderheadersalesreason')}}
    )
    , joining_tables as (
        select
            pedido_itens.id_pedido
            , cabecalho.id_cliente
            , cabecalho.id_shiptoaddress
            , cabecalho.id_billtoaddress
            , cabecalho.id_cartao
            , cabecalho.data_pedido
            , cabecalho.status_pedido
            , pedido_itens.id_produto
            , pedido_itens.quantidade
            , pedido_itens.preco_unitario
            , pedido_itens.desconto_unitario
            , preco_unitario * quantidade as sales_gross_value
            , preco_unitario * (1 - desconto_unitario) * quantidade as sales_net_value
        from pedido_itens
        left join cabecalho on pedido_itens.id_pedido = cabecalho.id_pedido
    )
    , transformacoes as (
        select 
           joining_tables.id_pedido
          , case
                when dim_customer.pk_cliente is null then -1
                else dim_customer.pk_cliente
            end as pk_cliente
            , case
                when ship_address.id_endereco is null then -1
                else ship_address.id_endereco
            end as ship_address_pk
            , case
                when bill_address.id_endereco is null then -1
                else bill_address.id_endereco
            end as bill_address_pk
            , case
                when dim_creditcard.pk_cartao is null then -1
                else dim_creditcard.pk_cartao
            end as pk_cartao
            , case
                when dim_product.id_produto is null then -1
                else dim_product.id_produto
            end as id_produto
            , case
                when dim_salesreason.pk_motivo is null then -1
                else dim_salesreason.pk_motivo
            end as pk_motivo
            , joining_tables.data_pedido
            , joining_tables.status_pedido
            , joining_tables.quantidade
            , joining_tables.preco_unitario
            , joining_tables.desconto_unitario
            , joining_tables.sales_gross_value
            , joining_tables.sales_net_value
        from joining_tables
--        /* Join com a tabela de endereço 2 vezes para pegar a sk do endereço de cobrança e o de entrega */
          left join dim_address as ship_address on joining_tables.id_shiptoaddress = ship_address.id_endereco
          left join dim_address as bill_address on joining_tables.id_billtoaddress = bill_address.id_endereco
          left join dim_customer on joining_tables.id_cliente = dim_customer.id_cliente
          left join dim_creditcard on joining_tables.id_cartao = dim_creditcard.id_cartao
          left join dim_product on joining_tables.id_produto = dim_product.id_produto
          left join dim_salesreason on joining_tables.id_pedido = dim_salesreason.id_pedido
    )
select *
from transformacoes