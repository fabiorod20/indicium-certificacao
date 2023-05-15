with
    fonte_detalhesordens as (
        select
            cast(salesorderid as int) as salesorderid_detalhesordens
            , cast(rowguid as string) as rowguid_detalhesordens
            , cast(salesorderdetailid as int) as salesorderdetailid_detalhesordens
            , cast(productid as int) as productid_detalhesordens
            , cast(orderqty as int) as orderqty_detalhesordens
            , cast(unitprice as numeric) as unitprice_detalhesordens
            , cast(unitpricediscount as numeric) as unitpricediscount_detalhesordens
        from {{ source('sap', 'salesorderdetail') }}
    )
select *
from fonte_detalhesordens