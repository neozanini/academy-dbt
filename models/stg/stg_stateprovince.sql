with
    sap_adw_stateprovince as(
        select
            cast(stateprovinceid as int) as id_estado
            , cast(territoryid as int) as id_territorio
            , cast(countryregioncode as string) as codigo_pais
            , cast(name as string) as estado
        from {{ source('adw', 'stateprovince') }}
    )

select *
from sap_adw_stateprovince