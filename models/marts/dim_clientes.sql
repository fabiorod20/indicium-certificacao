with
    pessoas as (
        select *
        from {{ ref('stg_sap__pessoas') }}
    )
    , clientes as (
        select *
        from {{ ref('stg_sap__clientes') }}
    )
    , join_tabelas as (
        select
           pessoas.rowguid_pessoa
            , pessoas.businessentityid_pessoa
            , clientes.id_cliente
            , clientes.rowguid_cliente
            , pessoas.nome_pessoa as cliente

        from clientes
        left join pessoas on
            clientes.id_cliente = pessoas.businessentityid_pessoa
        
    )
    , transformacoes as (
        select
            row_number() over (order by rowguid_pessoa) as sk_cliente
            , *
        from join_tabelas
    )
select *
from transformacoes