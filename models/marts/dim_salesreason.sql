with
    salesorderheadersalesreason as (
        select *
        from {{ ref('stg_salesorderheadersalesreason') }}
    )

    , salesreason as (
        select *
        from {{ ref('stg_salesreason') }}
    )

    , uniao_tabelas as (
        select             
            salesreason.motivo as id_motivo
            , salesorderheadersalesreason.id_pedido
            , salesreason.motivo
            , salesreason.tipo_motivo
        from salesorderheadersalesreason
        left join salesreason on salesorderheadersalesreason.id_motivo = salesreason.id_motivo
    )

    , transformacoes as (
        select
            row_number() over (order by id_motivo) as pk_motivo
            , *
        from uniao_tabelas
    )

select *
from transformacoes