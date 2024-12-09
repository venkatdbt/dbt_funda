with orders as(
    select
    *
    from {{ ref('int_orders1') }}
),
customers as(
    select
    *
    from {{ ref('stg_jaffle_shop1__customers') }}
),
int_orders as (
    select 
    *
    from {{ ref('int_orders1') }}
),
customer_orders as( 
    select
        customers.customer_id,
        customers.full_name,
        customers.surname,
        customers.givenname,
        orders.order_id,
        orders.order_value_dollars,
        orders.order_status,
        orders.payment_status,
   -- customer level aggrigations
        min(orders.order_date) over(partition by orders.customer_id)  as customer_first_order_date,

        min(orders.valid_order_date) over(partition by orders.customer_id) as customer_first_non_returned_order_date,
        
        max(orders.valid_order_date) over(partition by orders.customer_id) as customer_most_recent_non_returned_order_date,
        
        count(*) over(partition by orders.customer_id) as customer_order_count,

        sum(nvl2(orders.valid_order_date,1,0)) over(partition by orders.customer_id) as customer_non_returned_order_count,
        
        sum(nvl2(orders.valid_order_date,orders.order_value_dollars,0)) over(partition by orders.customer_id) as customer_total_lifetime_value,

        array_agg(distinct orders.order_id) over(partition by orders.customer_id) as customer_order_ids


    from int_orders as  orders
    inner join customers
    on orders.customer_id=customers.customer_id

),
add_avg_order_values as(
    select

    *,

    customer_total_lifetime_value/customer_non_returned_order_count as customer_avg_non_returned_order_value
    from customer_orders
),
--final ctes 

final as(
select 
    order_id,
    customer_id,
    surname,
    givenname,
    customer_first_order_date  as first_order_date,
    customer_order_count  as order_count,
    customer_total_lifetime_value  as total_lifetime_value,
    order_value_dollars,
    order_status, 
    payment_status
from add_avg_order_values
)
select * from final