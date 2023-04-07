with
    sap_adw_countryregion as(
        select
            cast(countryregioncode as string) as codigo_pais
            , cast(name as string) as pais
        from {{ source('adw', 'countryregion') }}
    )

select *
from sap_adw_countryregion