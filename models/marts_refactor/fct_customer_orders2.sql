with int_orders2 as (

  select * 
  from {{ ref('int_orders2') }}

),

final as(
select
    int_orders2.order_id,
    int_orders2.customer_id,
    int_orders2.order_placed_at,
    int_orders2.order_status,
    int_orders2.total_amount_paid,
    int_orders2.payment_finalized_date,
    int_orders2.customer_first_name,
    int_orders2.customer_last_name,

    --sales transaction sequence
    row_number() over (order by int_orders2.order_id) as transaction_seq,

    -- customer sales sequence
    row_number() over (partition by int_orders2.customer_id order by int_orders2.order_id) as customer_sales_seq,

    --new vs returning customer
    case when (
         rank() over(partition by int_orders2.customer_id order by int_orders2.order_placed_at,int_orders2.order_id)=1)
    then 'new'
    else 'return' end as nvsr,

 --- customer lifetime value
    sum(int_orders2.total_amount_paid)
     over(partition by int_orders2.customer_id order by int_orders2.order_placed_at) as customer_lifetime_value,

    first_value(int_orders2.order_placed_at) over(
        partition by int_orders2.customer_id
        order by int_orders2.order_placed_at
    ) as fdos
from int_orders2 
order by order_id
)
select * from final
