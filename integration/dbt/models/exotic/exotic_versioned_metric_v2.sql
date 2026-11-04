select
    order_id,
    item_net_amount as metric_value,
    'net_after_discounts' as metric_definition
from {{ ref('fct_orders') }}
