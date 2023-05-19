{{
    config(
        severity = 'error'
    )
}}
with
    vendas_em_2013 as (
        select sum(quantidade_de_itens) as soma_quantidade_de_itens
        from {{ ref('fct_vendas') }}
        where data_do_pedido between '2013-01-01' and '2013-12-31'
    )
select soma_quantidade_de_itens
from vendas_em_2013
where soma_quantidade_de_itens not between 129720 and 129730