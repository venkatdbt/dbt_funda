with source as(
    select
     *
    from {{ source('jaffle_shop', 'payment') }}
),
payments as(
    select
    id as payment_id,
    orderid as order_id,
    status as payment_status,
    created,
    amount,
    round(amount/100.0,2) as payment_amount
    from source
)
select * from payments