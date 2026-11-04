select order_id, line_number
from {{ ref('order_items') }}
group by order_id, line_number
having count(*) != 1

