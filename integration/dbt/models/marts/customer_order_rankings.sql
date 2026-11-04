{{ config(materialized='view') }}

select
    customer_id,
    order_id,
    ordered_at,
    order_total,
    row_number() over (
        partition by customer_id
        order by ordered_at desc, order_id desc
    ) as recency_rank
from {{ ref('fct_orders') }}
qualify recency_rank <= 2

