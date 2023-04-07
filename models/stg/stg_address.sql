with
    sap_adw_address as(
        select
            cast(addressid as int64) as id_endereco
            , cast(stateprovinceid as int64) as id_estado
            , cast(city as string) cidade
        from {{ source('adw', 'address') }}
    )

select *
from sap_adw_address


