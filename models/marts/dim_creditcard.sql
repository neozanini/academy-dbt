with
    creditcard as (
        select *
        from {{ ref('stg_creditcard') }}
    )

    , transformacoes as (
        select
            row_number() over (order by id_cartao) as pk_cartao
            , creditcard.id_cartao
            , creditcard.tipo_cartao
        from creditcard
    )

select *
from transformacoes