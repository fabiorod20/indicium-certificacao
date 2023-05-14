with 
    fonte_vendedor as (
        select * 
        from {{ source("sap", "salesperson") }}
)

 , renomear_vendedor as (
        select
            cast(rowguid as string) as rowguid_vendedor
            , cast(businessentityid as int) as businessentityid_vendedor
        from fonte_vendedor
    )
select *
from renomear_vendedor