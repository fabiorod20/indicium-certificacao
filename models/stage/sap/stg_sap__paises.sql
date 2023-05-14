with 
    fonte_pais as (
        select * 
        from {{ source("sap", "countryregion") }}
)

 , renomear_pais as (
        select
            cast(countryregioncode as string) as countryregioncode_pais
            , cast(name as string) as pais_pais
        from fonte_pais
    )
select *
from renomear_pais