with 
    fonte_loja as (
        select * 
        from {{ source("sap", "store") }}
)

 , renomear_loja as (
        select
            cast(rowguid as string) as rowguid_loja
            , cast(businessentityid as int) as businessentityid_loja
            , cast(name as string) as loja_loja
        from fonte_loja
    )
select *
from renomear_loja