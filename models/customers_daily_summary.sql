{{
    config(
        materialized='ephemeral'
    )
}}

select
   {{ dbt_utils.generate_surrogate_key(['customer_id','order_date']) }} as id,
   customer_id,
   order_date,
   count(*)
from {{ ref('stg_jaffle_shop__orders') }}
group by 1,2,3