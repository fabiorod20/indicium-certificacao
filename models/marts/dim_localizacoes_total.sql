with
    cidades as (
        select *
        from {{ ref('stg_sap__cidades') }}
    )
    , estados as (
        select *
        from {{ ref('stg_sap__estados') }}
    )
    , paises as (
        select *
        from {{ ref('stg_sap__paises') }}
    )
    , join_tabelas as (
        select
           estados.stateprovinceid_estado
            , estados.territoryid_estado
            , estados.countryregioncode_estado
            , paises.countryregioncode_pais
            , estados.estado_estado as estado
            , paises.pais_pais as pais

        from paises
        right join estados on
            paises.countryregioncode_pais = estados.countryregioncode_estado
    )
    , join_tabelao as (
        select
           join_tabelas.stateprovinceid_estado
            , join_tabelas.territoryid_estado
            , join_tabelas.countryregioncode_estado
            , cidades.stateprovinceid_cidade
            , join_tabelas.countryregioncode_pais
            , cidades.addressid_cidade
            , join_tabelas.estado
            , join_tabelas.pais
            , cidades.cidade_cidade as cidade

        from cidades
        left join join_tabelas on
            cidades.stateprovinceid_cidade = join_tabelas.stateprovinceid_estado
    )
    , transformacoes as (
        select
           row_number() over (order by stateprovinceid_cidade) as sk_localizacao
            , *
        from join_tabelao
    )
select *
from transformacoes