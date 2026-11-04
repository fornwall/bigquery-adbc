select
    order_id,
    customer_id,
    lower(status) as normalized_status,
    shipping_amount,
    {{ exotic_fingerprint("concat(cast(order_id as string), ':', lower(status))") }} as row_fingerprint
from {{ ref('orders') }}
