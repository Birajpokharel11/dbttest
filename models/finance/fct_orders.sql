with orders as (
    select * from {{ref('stg_orders')}}
),
payments as (
    select * from {{ref('stg_stripe__payments')}}
),
order_payments as  (
    select
    order_id,
    sum(case when status='success' then amount end ) as amount
    from payments
    group by 1
)

final as (
    select orders.order_id,
    orders.customer_id,
    orders.order_date,
    coalesce(order_payments.amount)
    from orders
    left join order_payents using (order_id)

)

select * from final