with payemts as(
    select * from {{ ref('stg_stripe__payments') }}
),
pivoted as(
    select 
    order_id,
    -- sum(case when payment_method= 'credit_card' then amount else 0 end )
    --  as credit_card_amount,
    -- sum(case when payment_method= 'coupon' then amount else 0 end )
    --  as coupon_amount,
    --  sum(case when payment_method= 'bank_transfer' then amount else 0 end ) 
    --  as bank_transfer_amount ,
    --  sum(case when payment_method= 'gift_card' then amount else 0 end ) 
    --  as gift_card_amount
        {%- set payment_method = ['credit_card','coupon','bank_transfer','gift_card'] -%}
        {% for i in payment_method %}
        sum(case when payment_method= '{{i}}' then amount else 0 end ) 
        as {{i}}_amount {% if not loop.end %},{%- endif -%}
        {%- endfor -%}
    from payemts 
    where status = 'success'
    group by 1
)
select * from pivoted
