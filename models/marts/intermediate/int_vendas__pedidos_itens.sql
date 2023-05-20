with
    pedidos as (
        select *
        from {{ ref('stg_sap__ordens_de_venda') }}
    )
    , pedido_itens as (
        select *
        from {{ ref('stg_sap__detalhes_ordens_de_venda') }}
    )
    , join_tabelas as (
        select
            pedidos.salesorderid_ordens
            , pedidos.rowguid_ordens
            , pedidos.customerid_ordens
            , pedidos.billtoaddressid_ordens
            , pedidos.shiptoaddressid_ordens
            , pedidos.salespersonid_ordens
            , pedidos.creditcardid_ordens
            , pedidos.territoryid_ordens
            , pedidos.revisionnumber
            , pedido_itens.salesorderid_detalhesordens
            , pedido_itens.salesorderdetailid_detalhesordens
            , pedido_itens.productid_detalhesordens
            , pedido_itens.rowguid_detalhesordens
            , pedidos.orderdate_ordens as data_do_pedido
            , pedidos.status_ordens as status
            , pedidos.purchaseordernumber_ordens as numero_do_pedido
            , pedidos.subtotal_ordens as subtotal
            , pedidos.taxamt_ordens as taxas
            , pedidos.freight_ordens as frete
            , pedidos.totaldue_ordens as total
            , pedido_itens.orderqty_detalhesordens as quantidade_de_itens
            , pedido_itens.unitprice_detalhesordens as preco_por_unidade
            , pedido_itens.unitpricediscount_detalhesordens as desconto_por_unidade
        from pedido_itens
        left join pedidos on
            pedido_itens.salesorderid_detalhesordens = pedidos.salesorderid_ordens
    )
select *
from join_tabelas