with 
    fonte_chaverazaodevenda as (
        select * 
        from {{ source("sap", "salesorderheadersalesreason") }}
)

 , renomear_chaverazaodevenda as (
        select
            cast(salesorderid as int) as salesorderid_chaverazaodevenda
            , cast(salesreasonid as int) as salesreasonid_chaverazaodevenda
        from fonte_chaverazaodevenda
    )
select *
from renomear_chaverazaodevenda