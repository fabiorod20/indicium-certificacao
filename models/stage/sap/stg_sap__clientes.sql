with 
    fonte_clientes as (
        select * 
        from {{ source("sap", "customer") }}
)

 , renomear_clientes as (
        select
            cast(customerid as int) as id_cliente
            , cast(rowguid as string) as rowguid_cliente
            , cast(personid as int) as personid_cliente
        from fonte_clientes
    )
select *
from renomear_clientes