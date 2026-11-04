{{ config(
    materialized='table',
    contract={'enforced': true}
) }}

select
    customer_id as id,
    cast(customer_id * numeric '2.005' as numeric) as amount,
    region,
    is_active,
    signup_date,
    timestamp(signup_date) as recorded_at,
    [customer_id, customer_id + 1] as ids,
    struct(customer_id as key, customer_name as name) as label
from {{ ref('stg_customers') }}

