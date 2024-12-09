--import ctes

with base_customers as(
    select 
    *
    from raw.jaffle_shop_original.customers
),
base_orders as(
    select
    *
    from raw.jaffle_shop_original.orders
),
payments as(
select 
* 
from raw.jaffle_shop_original.payment    
),
---staging
customers as(
select
     id as customer_id,
     first_name ||' '|| last_name as full_name,
     last_name as surname,
     first_name as givenname
from base_customers
), 

--logical ctes
orders as(
     select 
        id as order_id,
        user_id as customer_id,
        order_date,
        status as order_status,
        row_number() over (partition by user_id order by order_date, id) as user_order_seq
      from base_orders
),

--marts
customer_order_history as(
select 
        customers.customer_id,
        customers.full_name,
        customers.surname,
        customers.givenname,
        min(order_date) as first_order_date,
        min(case when orders.order_status NOT IN ('returned','return_pending') then order_date end) as first_non_returned_order_date,
        max(case when orders.order_status NOT IN ('returned','return_pending') then order_date end) as most_recent_non_returned_order_date,
        COALESCE(max(user_order_seq),0) as order_count,
        COALESCE(count(case when orders.order_status != 'returned' then 1 end),0) as non_returned_order_count,
        sum(case when orders.order_status NOT IN ('returned','return_pending') then ROUND(c.amount/100.0,2) else 0 end) as total_lifetime_value,
        sum(case when orders.order_status NOT IN ('returned','return_pending') then ROUND(c.amount/100.0,2) else 0 end)/NULLIF(count(case when orders.order_status NOT IN ('returned','return_pending') then 1 end),0) as avg_non_returned_order_value,
        array_agg(distinct orders.order_id) as order_ids

    from  orders
    join  customers
        on orders.customer_id = customers.customer_id
    left join payments c
        on orders.order_id = c.orderid
    where orders.order_status NOT IN ('pending') and c.status != 'fail'
    group by customers.customer_id, customers.full_name, customers.surname, customers.givenname

),

--final ctes 

final as(
select 
    orders.order_id,
    orders.customer_id,
    customers.surname,
    customers.givenname,
    customer_order_history.first_order_date,
    customer_order_history.order_count,
    total_lifetime_value,
    round(payments.amount/100.0,2) as order_value_dollars,
    orders.order_status as order_status,
    payments.status as payment_status
from orders
join  customers
     on orders.customer_id = customers.customer_id
join  customer_order_history
     on orders.customer_id = customer_order_history.customer_id
left outer join  payments
     on orders.order_id = payments.orderid
where payments.status != 'fail'
)
select * from final