with
    sap_adw_customer as(
        select
            cast(customerid as int) as id_cliente
            , cast(personid as int) as id_pessoa
            , cast(storeid as int) as id_loja
            , cast(territoryid as int) as id_territorio
        from {{ source('adw', 'customer') }}
    )

select *
from sap_adw_customer