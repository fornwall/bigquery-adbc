select
    customer_id,
    trim(customer_name) as customer_name,
    lower(region) as region,
    signup_date,
    is_active
from {{ ref('customers') }}

