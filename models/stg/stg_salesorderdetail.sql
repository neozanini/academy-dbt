with
    sap_adw_salesorderdetail as(
        select
            cast(salesorderid as int) id_pedido
            , cast(productid as int) as id_produto
            , cast(orderqty as int) as quantidade
            , unitprice as preco_unitario
            , unitpricediscount as desconto_unitario
        from {{ source('adw', 'salesorderdetail') }}
    )

select *
from sap_adw_salesorderdetail