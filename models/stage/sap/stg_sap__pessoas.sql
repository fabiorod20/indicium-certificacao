with 
    fonte_pessoa as (
        select * 
        from {{ source("sap", "person") }}
)

 , renomear_pessoa as (
        select
            cast(rowguid as string) as rowguid_pessoa
            , cast(businessentityid as int) as businessentityid_pessoa
            , cast((firstname || ' ' || lastname) as string) as nome_pessoa
        from fonte_pessoa
    )
select *
from renomear_pessoa