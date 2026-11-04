select
    order_id,
    customer_id,
    ordered_at,
    date(ordered_at) as order_date,
    lower(status) as status,
    case
        when lower(status) = 'completed' then 'recognized'
        when lower(status) = 'cancelled' then 'lost'
        else 'open'
    end as revenue_state,
    shipping_amount
from {{ ref('orders') }}

