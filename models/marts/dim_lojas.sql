with
    lojas as (
        select *
        from {{ ref('stg_sap__lojas') }}
    )
    , transformacoes as (
        select
            row_number() over (order by businessentityid_loja) as sk_loja
            , lojas.rowguid_loja
            , lojas.businessentityid_loja
            , lojas.loja_loja as loja
        from lojas
    )
select *
from transformacoes