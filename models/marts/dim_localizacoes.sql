with
    paises as (
        select *
        from {{ ref('stg_sap__paises') }}
    )
    , estados as (
        select *
        from {{ ref('stg_sap__estados') }}
    )
    , join_tabelas as (
        select
          paises.countryregioncode_pais
            , estados.rowguid_estado
            , estados.stateprovinceid_estado
            , estados.countryregioncode_estado
            , estados.estado_estado as estado
            , paises.pais_pais as pais

        from paises
        left join estados on
            paises.countryregioncode_pais = estados.countryregioncode_estado
    )
    , transformacoes as (
        select
            row_number() over (order by countryregioncode_pais) as sk_localizacoes
            , *
        from join_tabelas
    )
select *
from transformacoes