with
    sap_adw_salesreason as(
        select
            cast(salesreasonid as int) as id_motivo
            , cast(name as string) as motivo
            , cast(reasontype as  string) as tipo_motivo
        from {{ source('adw', 'salesreason') }}
    )

select *
from sap_adw_salesreason