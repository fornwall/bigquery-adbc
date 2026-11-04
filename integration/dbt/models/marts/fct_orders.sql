{{ config(
    materialized='table',
    partition_by={'field': 'order_date', 'data_type': 'date'},
    cluster_by=['status', 'customer_id'],
    labels={'component': 'adbc_integration', 'materialization': 'table'}
) }}

select
    orders.order_id,
    orders.customer_id,
    orders.ordered_at,
    orders.order_date,
    orders.status,
    orders.revenue_state,
    orders.shipping_amount,
    count(*) as item_count,
    sum(items.gross_amount) as gross_amount,
    sum(items.discount_amount) as discount_amount,
    sum(items.net_amount) as item_net_amount,
    sum(items.net_amount) + orders.shipping_amount as order_total,
    array_agg(
        struct(
            items.line_number as line_number,
            items.sku as sku,
            items.quantity as quantity,
            items.net_amount as net_amount
        ) order by items.line_number
    ) as line_items
from {{ ref('stg_orders') }} as orders
inner join {{ ref('int_order_items') }} as items using (order_id)
group by
    orders.order_id,
    orders.customer_id,
    orders.ordered_at,
    orders.order_date,
    orders.status,
    orders.revenue_state,
    orders.shipping_amount

