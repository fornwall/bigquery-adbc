-- Reads the orders seed as a source; this ref makes the seed a build dependency.
-- depends_on: {{ ref('orders') }}

select
    order_id,
    customer_id,
    lower(status) as normalized_status,
    shipping_amount
from {{ source('exotic_seed_tables', 'orders') }}
