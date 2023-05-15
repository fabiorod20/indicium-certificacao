with
    cartoes as (
        select *
        from {{ ref('stg_sap__cartoes_de_credito') }}
    )
    , transformacoes as (
        select
            row_number() over (order by creditcardid_cartaodecredito) as sk_cartao
            , cartoes.creditcardid_cartaodecredito
            , cartoes.cardtype_cartaodecredito as card_type
        from cartoes
    )
select *
from transformacoes