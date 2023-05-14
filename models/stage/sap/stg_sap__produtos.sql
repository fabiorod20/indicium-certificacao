with 
    fonte_produto as (
        select * 
        from {{ source("sap", "product") }}
)

 , renomear_produto as (
        select
            cast(rowguid as string) as rowguid_produto
            , cast(productid as int) as productid_produto
            , cast(name as string) as produto_produto
        from fonte_produto
    )
select *
from renomear_produto