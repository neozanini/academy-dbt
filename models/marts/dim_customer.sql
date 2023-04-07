with
    person as (
        select *
        from {{ ref('stg_person') }}
    )
    
    , customer as (
        select *
        from {{ ref('stg_customer') }}
    )

    , store as (
        select *
        from {{ ref('stg_salesstore') }}
    )

    , uniao_tabelas as (
        select 
            customer.id_cliente
            ,case
                when customer.id_loja is not null then store.store_name
                else person.nome
            end as cliente_nome
            ,customer.id_territorio
            ,customer.id_loja
            , person.nome
            , person.sobrenome
        from customer
        left join person on customer.id_pessoa = person.id_entidade
        left join store on customer.id_loja = store.store_business_entity_id
    )

    , transformacoes as (
        select
            row_number() over (order by id_cliente) as pk_cliente
            , id_cliente
            , cliente_nome
            , concat(nome,' ', sobrenome) as nome_completo
            , id_territorio
        from uniao_tabelas
    )

select *
from transformacoes