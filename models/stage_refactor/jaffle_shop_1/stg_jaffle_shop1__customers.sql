with source as(
    select 
    * 
    from {{ source('jaffle_shop', 'customers') }}
),
customers as(
select
     id as customer_id,
     first_name ||' '|| last_name as full_name,
     last_name as surname,
     first_name as givenname
from source
)
select * from customers