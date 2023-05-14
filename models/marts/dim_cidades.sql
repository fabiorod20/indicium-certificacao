with
    cidades as (
        select *
        from {{ ref('stg_sap__cidades') }}
    )
    , transformacoes as (
        select
            row_number() over (order by rowguid_cidade) as sk_cidade
            , cidades.rowguid_cidade
            , cidades.stateprovinceid_cidade
            , cidades.cidade_cidade as cidade
        from cidades
    )
select *
from transformacoes