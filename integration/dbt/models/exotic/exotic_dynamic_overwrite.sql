{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'order_date', 'data_type': 'date'},
    tags=['incremental'],
    labels={'component': 'adbc_integration', 'coverage': 'dynamic_overwrite'}
) }}

select
    order_date,
    count(*) as order_count,
    sum(order_total) as booked_revenue,
    cast({{ var('incremental_wave', 1) }} as int64) as load_wave
from {{ ref('fct_orders') }}
group by order_date
