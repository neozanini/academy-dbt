with
    sap_adw_creditcard as(
        select
            cast(creditcardid as int) as id_cartao
            , cast(cardtype as string) as tipo_cartao
        from {{ source('adw', 'creditcard') }}
    )

select *
from sap_adw_creditcard