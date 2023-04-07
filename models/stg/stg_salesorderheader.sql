with
    sap_adw_salesorderheader as(
        select
            cast(salesorderid as int) as id_pedido
            , cast(customerid as int) as id_cliente
            , cast(territoryid as int) as id_territorio
            , cast(shiptoaddressid as int) as id_shiptoaddress
            , cast(billtoaddressid as int) as id_billtoaddress
            , cast(creditcardid as int) as id_cartao
            , cast(orderdate as datetime) as data_pedido
            , cast(status as int) as status_pedido
            , totaldue as valor_total
        from {{ source('adw', 'salesorderheader') }}
    )

select*
from sap_adw_salesorderheader