with
    chaverazoes as (
        select *
        from {{ ref('stg_sap__chaves_razoes_de_venda') }}
    )
    , razoes as (
        select *
        from {{ ref('stg_sap__razoes_de_venda') }}
    )
    , join_tabelas as (
        select
          chaverazoes.salesorderid_chaverazaodevenda
            , chaverazoes.salesreasonid_chaverazaodevenda
            , razoes.salesreasonid_razaodevenda
            , razoes.razaodevenda_razaodevenda as razao_de_venda

        from chaverazoes
        left join razoes on
            chaverazoes.salesreasonid_chaverazaodevenda = razoes.salesreasonid_razaodevenda
    )
    , transformacoes as (
        select
            row_number() over (order by salesorderid_chaverazaodevenda) as sk_razao
            , *
        from join_tabelas
    )
select *
from transformacoes