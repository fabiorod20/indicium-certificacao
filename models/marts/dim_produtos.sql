with
    produtos as (
        select *
        from {{ ref('stg_sap__produtos') }}
    )
    , transformacoes as (
        select
            row_number() over (order by productid_produto) as sk_produto
            , produtos.rowguid_produto
            , produtos.productid_produto
            , produtos.produto_produto as produto
        from produtos
    )
select *
from transformacoes