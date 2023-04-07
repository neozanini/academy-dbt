with
    sap_adw_product as(
        select
            cast(productid as int) as id_produto
            , cast(name as string) as nome_produto
        from {{ source('adw', 'product') }}
    )

select *
from sap_adw_product