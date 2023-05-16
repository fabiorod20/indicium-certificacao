with 
    fonte_estado as (
        select * 
        from {{ source("sap", "stateprovince") }}
)

 , renomear_estado as (
        select
            cast(rowguid as string) as rowguid_estado
            , cast(stateprovinceid as int) as stateprovinceid_estado
            , cast(territoryid as int) as territoryid_estado
            , cast(countryregioncode as string) as countryregioncode_estado
            , cast(name as string) as estado_estado
        from fonte_estado
    )
select *
from renomear_estado