select order_id
from {{ ref('fct_orders') }}
where item_count != array_length(line_items)
   or exists (
       select 1
       from unnest(line_items) as item
       where item.quantity <= 0 or item.net_amount < 0
   )

