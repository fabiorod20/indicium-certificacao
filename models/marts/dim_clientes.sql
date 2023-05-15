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

        from pessoas
        Left join clientes on
            pessoas.rowguid_pessoa = clientes.rowguid_cliente
        
    )
    , transformacoes as (
        select
            row_number() over (order by rowguid_pessoa) as sk_cliente
            , *
        from join_tabelas
    )
select *
from transformacoes