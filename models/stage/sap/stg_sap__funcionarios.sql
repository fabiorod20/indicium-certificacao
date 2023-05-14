with 
    fonte_funcionarios as (
        select * 
        from {{ source("sap", "employee") }}
)

 , renomear_funcionarios as (
        select
            cast(rowguid as string) as rowguid_funcionarios
            , cast(businessentityid as int) as businessentityid_funcionarios
        from fonte_funcionarios
    )
select *
from renomear_funcionarios