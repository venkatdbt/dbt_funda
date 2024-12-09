{{
    config(
        materialized='ephemeral'
    )
}}
select * from {{ source('jaffle_shop','orders') }}
{{ limit_data_in_dev('order_date',3) }}

