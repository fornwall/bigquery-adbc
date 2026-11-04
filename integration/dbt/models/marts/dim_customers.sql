{{ config(cluster_by=['region']) }}

select
    customers.customer_id,
    customers.customer_name,
    customers.region,
    customers.signup_date,
    customers.is_active,
    count(orders.order_id) as order_count,
    countif(orders.status = 'completed') as completed_order_count,
    coalesce(sum(if(orders.status = 'completed', orders.order_total, 0)), 0) as lifetime_value,
    max(orders.ordered_at) as last_ordered_at
from {{ ref('stg_customers') }} as customers
left join {{ ref('fct_orders') }} as orders using (customer_id)
group by
    customers.customer_id,
    customers.customer_name,
    customers.region,
    customers.signup_date,
    customers.is_active

