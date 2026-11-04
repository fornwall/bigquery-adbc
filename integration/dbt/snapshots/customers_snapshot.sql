{% snapshot customers_snapshot %}

{{ config(
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at',
    tags=['snapshot']
) }}

select customer_id, customer_name, updated_at
from {{ ref('customers_for_snapshot') }}

{% endsnapshot %}

