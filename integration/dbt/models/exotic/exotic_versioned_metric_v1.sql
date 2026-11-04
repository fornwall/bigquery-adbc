select
    order_id,
    gross_amount as metric_value,
    'gross_before_discounts' as metric_definition
from {{ ref('fct_orders') }}
