with
    sap_adw_person as(
        select
            cast(businessentityid as int) as id_entidade
            , cast(firstname as string) as nome
            , cast(lastname as string) as sobrenome
        from {{ source('adw', 'person') }}
    )

select *
from sap_adw_person