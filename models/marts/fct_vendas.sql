with
    cartoes as (
        select *
        from {{ ref('dim_cartoes_de_credito') }}
    )
    , localizacoes_total as (
        select *
        from {{ ref('dim_localizacoes_total') }}
    )
    , clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )
    , produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )
    , razoes as (
        select *
        from {{ ref('dim_razoes_de_venda') }}
    )
    , pedido_item as (
        select *
        from {{ ref('int_vendas__pedidos_itens') }}
    )
    , joined_tabelas as (
        select
            pedido_item.salesorderid_ordens
            , pedido_item.rowguid_ordens
            , cartoes.sk_cartao as fk_cartao
            , localizacoes_total.sk_localizacao as fk_localizacao
            , clientes.sk_cliente as fk_cliente
            , produtos.sk_produto as fk_produto
            , razoes.sk_razao as fk_razao
            , pedido_item.customerid
            , pedido_item.territoryid
            , pedido_item.salespersonid
            , pedido_item.creditcardid
            , pedido_item.productid
            , pedido_item.data_do_pedido
            , pedido_item.status
            , pedido_item.numero_do_pedido
            , pedido_item.subtotal
            , pedido_item.taxas
            , pedido_item.frete
            , pedido_item.total
            , pedido_item.quantidade_de_itens
            , pedido_item.preco_por_unidade
            , pedido_item.desconto_por_unidade
            , cartoes.card_type
            , localizacoes_total.cidade
            , clientes.cliente
            , localizacoes_total.estado
            , produtos.produto
            , razoes.razao_de_venda
            , localizacoes_total.pais
        from pedido_item
        left join cartoes on pedido_item.creditcardid = cartoes.creditcardid_cartaodecredito
        left join localizacoes_total on pedido_item.territoryid = localizacoes_total.addressid_cidade
        left join clientes on pedido_item.customerid = clientes.id_cliente
        left join produtos on pedido_item.productid = produtos.productid_produto
        left join razoes on pedido_item.salesorderid_ordens = razoes.salesorderid_chaverazaodevenda
    )
    , transformacoes as (
        select
            {{ dbt_utils.generate_surrogate_key(['productid', 'fk_produto']) }} as sk_venda
            , *
            , quantidade_de_itens * preco_por_unidade as total_bruto
            , (1 - desconto_por_unidade) * quantidade_de_itens * preco_por_unidade as total_liquido
            , case
                when desconto_por_unidade > 0 then true
                when desconto_por_unidade = 0 then false
                else false
                end as eh_desconto
        from joined_tabelas
    )
select *
from transformacoes