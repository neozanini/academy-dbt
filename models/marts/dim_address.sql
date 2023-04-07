with
    address as (
        select *
        from {{ ref('stg_address') }}
    )

    , stateprovince as (
        select *
        from {{ ref('stg_stateprovince') }}
    )

    , countryregion as (
        select *
        from {{ ref('stg_countryregion') }}
    )

    , uniao_tabelas as (
        select 
            address.id_endereco
            , address.cidade
            , stateprovince.id_estado
            , stateprovince.estado
            , stateprovince.id_territorio
            , countryregion.codigo_pais
            , countryregion.pais
        from address
        left join stateprovince on address.id_estado = stateprovince.id_estado
        left join countryregion on stateprovince.codigo_pais = countryregion.codigo_pais
    )
    , transformacoes as (
        select
            row_number() over (order by id_endereco) as pk_endereco
            , *
        from uniao_tabelas
    )

select *
from transformacoes