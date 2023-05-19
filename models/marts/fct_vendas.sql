with
    cartoes as (
        select *
        from {{ ref('dim_cartoes_de_credito') }}
    )
    , localizacoes as (
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
            cartoes.sk_cartao as fk_cartao
            , localizacoes.sk_localizacao as fk_localizacao
            , clientes.sk_cliente as fk_cliente
            , produtos.sk_produto as fk_produto
            , razoes.sk_razao as fk_razao
            , pedido_item.salesorderid_ordens as salesorderid
            , pedido_item.customerid_ordens
            , pedido_item.billtoaddressid_ordens
            , pedido_item.salesorderdetailid_detalhesordens
            , pedido_item.shiptoaddressid_ordens
            , pedido_item.territoryid_ordens
            , pedido_item.revisionnumber
            , razoes.salesreasonid_chaverazaodevenda
            , pedido_item.creditcardid_ordens
            , pedido_item.salesorderid_detalhesordens
            , razoes.salesorderid_chaverazaodevenda
            , pedido_item.productid_detalhesordens
            , pedido_item.data_do_pedido
            , pedido_item.status
            , pedido_item.subtotal
            , pedido_item.taxas
            , pedido_item.frete
            , pedido_item.total
            , pedido_item.quantidade_de_itens
            , pedido_item.preco_por_unidade
            , pedido_item.desconto_por_unidade
            , cartoes.card_type as tipo_de_cartao
            , localizacoes.cidade
            , clientes.cliente
            , localizacoes.estado
            , produtos.produto
            , razoes.razao_de_venda
            , localizacoes.pais
        from pedido_item
        left join cartoes on pedido_item.creditcardid_ordens = cartoes.creditcardid_cartaodecredito
        left join localizacoes on pedido_item.shiptoaddressid_ordens = localizacoes.addressid_cidade
        left join clientes on pedido_item.customerid_ordens = clientes.id_cliente
        left join razoes on pedido_item.salesorderid_detalhesordens = razoes.salesorderid_chaverazaodevenda
        left join produtos on pedido_item.productid_detalhesordens = produtos.productid_produto
    )
    , transformacoes as (
        select
            row_number() over (order by salesorderid_detalhesordens) as sk_venda
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