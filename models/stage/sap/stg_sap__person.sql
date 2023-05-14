with fonte_person as (select * from {{ source("sap", "person") }})
select *
from fonte_person
