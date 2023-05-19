with 
    fonte_razaodevenda as (
        select * 
        from {{ source("sap", "salesreason") }}
)

 , renomear_razaodevenda as (
        select
            cast(salesreasonid as int) as salesreasonid_razaodevenda
            , cast(name as string) as name_razaodevenda
        from fonte_razaodevenda
    )
select *
from renomear_razaodevenda