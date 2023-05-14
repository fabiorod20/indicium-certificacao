with 
    fonte_cartaodecredito as (
        select * 
        from {{ source("sap", "creditcard") }}
)

 , renomear_cartaodecredito as (
        select
            cast(creditcardid as int) as creditcardid_cartaodecredito
            , cast(cardtype as string) as cardtype_cartaodecredito
        from fonte_cartaodecredito
    )
select *
from renomear_cartaodecredito