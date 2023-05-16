with 
    fonte_cidade as (
        select * 
        from {{ source("sap", "address") }}
)

 , renomear_cidade as (
        select
            cast(rowguid as string) as rowguid_cidade
            , cast(stateprovinceid as int) as stateprovinceid_cidade
            , cast(addressid as int) as addressid_cidade
            , cast(city as string) as cidade_cidade
        from fonte_cidade
    )
select *
from renomear_cidade