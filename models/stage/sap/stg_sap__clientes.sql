with 
    fonte_clientes as (
        select * 
        from {{ source("sap", "person") }}
)

select *
from fonte_clientes
