with
    fonte_ordens as (
        select
            cast(salesorderid as int) as salesorderid_ordens
            , cast(rowguid as string) as rowguid_ordens
            , cast(revisionnumber as int) as revisionnumber
            , cast(customerid as int) as customerid_ordens
            , cast(salespersonid as int) as salespersonid_ordens
            , cast(creditcardid as int) as creditcardid_ordens
            , cast(territoryid as int) as territoryid_ordens
            , cast(shiptoaddressid as int) as shiptoaddressid_ordens
            , cast(billtoaddressid as int) as billtoaddressid_ordens
            , cast(orderdate as string) as orderdate_ordens
            , cast(duedate as string) as duedate_ordens
            , cast(status as int) as status_ordens
            , cast(purchaseordernumber as string) as purchaseordernumber_ordens
            , cast(accountnumber as string) as accountnumber_ordens
            , cast(creditcardapprovalcode as string) as creditcardapprovalcode_ordens
            , cast(subtotal as numeric) as subtotal_ordens
            , cast(taxamt as numeric) as taxamt_ordens
            , cast(freight as numeric) as freight_ordens
            , cast(totaldue as numeric) as totaldue_ordens
        from {{ source('sap', 'salesorderheader') }}
    )
select *
from fonte_ordens