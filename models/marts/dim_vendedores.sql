with
    pessoas as (
        select *
        from {{ ref('stg_sap__pessoas') }}
    )
    , funcionarios as (
        select *
        from {{ ref('stg_sap__funcionarios') }}
    )
    , vendedores as (
        select *
        from {{ ref('stg_sap__vendedores') }}
    )
    , join_tabelas as (
        select
           pessoas.rowguid_pessoa
            , pessoas.businessentityid_pessoa
            , funcionarios.rowguid_funcionarios
            , funcionarios.businessentityid_funcionarios
            , vendedores.rowguid_vendedor
            , vendedores.businessentityid_vendedor
            , pessoas.nome_pessoa as vendedor

        from pessoas
        right join funcionarios on
            pessoas.rowguid_pessoa = funcionarios.rowguid_funcionarios

        right join vendedores on
            pessoas.rowguid_pessoa = vendedores.rowguid_vendedor
    )
    , transformacoes as (
        select
            row_number() over (order by rowguid_vendedor) as sk_vendedores
            , *
        from join_tabelas
    )
select *
from transformacoes