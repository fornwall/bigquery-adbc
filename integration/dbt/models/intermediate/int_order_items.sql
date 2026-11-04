select
    order_id,
    line_number,
    upper(sku) as sku,
    quantity,
    unit_price,
    quantity * unit_price as gross_amount,
    discount_amount,
    {{ discounted_amount('quantity', 'unit_price', 'discount_amount') }} as net_amount
from {{ ref('order_items') }}

