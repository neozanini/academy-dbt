with
    sap_adw_salesorderheadersalesreason as(
        select
            cast(salesorderid as int) as id_pedido
            , cast(salesreasonid as int) as id_motivo
        from {{ source('adw', 'salesorderheadersalesreason') }}
    )

select *
from sap_adw_salesorderheadersalesreason