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
           row_number() over (order by stateprovinceid_cidade) as sk_localizacao
            , cidades.rowguid_cidade
            , cidades.addressid_cidade
            , estados.stateprovinceid_estado
            , estados.countryregioncode_estado
            , estados.territoryid_estado as territoryid
            , cidades.stateprovinceid_cidade
            , cidades.cidade_cidade as cidade
            , estados.estado_estado as estado
        
        from cidades
        left join estados on
            cidades.stateprovinceid_cidade = estados.stateprovinceid_estado
    )
    , transformacoes as (
        select
            row_number() over (order by stateprovinceid_cidade) as sk_localizacao
            , join_tabelas.rowguid_cidade
            , join_tabelas.addressid_cidade
            , join_tabelas.stateprovinceid_estado
            , join_tabelas.countryregioncode_estado
            , paises.countryregioncode_pais
            , join_tabelas.territoryid
            , join_tabelas.stateprovinceid_cidade
            , join_tabelas.cidade
            , join_tabelas.estado
            , paises.pais_pais as pais
       
        from join_tabelas
        left join paises on
            join_tabelas.countryregioncode_estado = paises.countryregioncode_pais
    )

select *
from transformacoes