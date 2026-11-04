{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    partition_by={'field': 'order_date', 'data_type': 'date'},
    cluster_by=['customer_id'],
    persist_docs={'relation': true, 'columns': true},
    tags=['incremental']
) }}

select
    order_id,
    customer_id,
    ordered_at,
    order_date,
    status,
    order_total,
    current_timestamp() as loaded_at,
    cast({{ var('incremental_wave', 1) }} as int64) as load_wave
from {{ ref('fct_orders') }}

{% if is_incremental() %}
where ordered_at >= timestamp_sub(
    (select coalesce(max(ordered_at), timestamp '1970-01-01') from {{ this }}),
    interval 7 day
)
{% endif %}
