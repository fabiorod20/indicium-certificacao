with
    cartoes as (
        select *
        from {{ ref('dim_cartoes_de_credito') }}
    )
    , cidades as (
        select *
        from {{ ref('dim_cidades') }}
    )
    , clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )
    , localizacoes as (
        select *
        from {{ ref('dim_localizacoes') }}
    )
    , lojas as (
        select *
        from {{ ref('dim_lojas') }}
    )
    , produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )
    , razoes as (
        select *
        from {{ ref('dim_razoes_de_venda') }}
    )
    , vendedores as (
        select *
        from {{ ref('dim_vendedores') }}
    )
    , pedido_item as (
        select *
        from {{ ref('int_vendas__pedidos_itens') }}
    )
    , joined_tabelas as (
        select
            pedido_item.salesorderid_ordens
            , pedido_item.rowguid
            , cartoes.sk_cartao as fk_cartao
            , cidades.sk_cidade as fk_cidade
            , clientes.sk_cliente as fk_cliente
            , localizacoes.sk_localizacao as fk_localizacao
            , lojas.sk_loja as fk_loja
            , produtos.sk_produto as fk_produto
            , razoes.sk_razao as fk_razao
            , vendedores.sk_vendedor as fk_vendedor
            , pedido_item.customerid
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
            , cidades.cidade
            , clientes.cliente
            , localizacoes.estado
            , localizacoes.pais
            , lojas.loja
            , produtos.produto
            , razoes.razao_de_venda
            , vendedores.vendedor
        from pedido_item
        left join cartoes on pedido_item.creditcardid = cartoes.creditcardid_cartaodecredito
        left join cidades on pedido_item.rowguid = cidades.rowguid_cidade
        left join clientes on pedido_item.customerid = clientes.id_cliente
        left join localizacoes on pedido_item.rowguid = localizacoes.rowguid_estado
        left join lojas on pedido_item.rowguid = lojas.rowguid_loja
        left join produtos on pedido_item.rowguid = produtos.rowguid_produto
        left join razoes on pedido_item.salesorderid_ordens = razoes.salesorderid_chaverazaodevenda
        left join vendedores on pedido_item.rowguid = vendedores.rowguid_pessoa
    )
    , transformacoes as (
        select
            {{ dbt_utils.generate_surrogate_key(['salesorderid_ordens', 'fk_produto']) }} as sk_venda
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