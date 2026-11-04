{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    on_schema_change='append_new_columns',
    tags=['incremental'],
    labels={'component': 'adbc_integration', 'coverage': 'schema_evolution'}
) }}

select
    order_id,
    order_total,
    cast({{ var('incremental_wave', 1) }} as int64) as load_wave
    {% if var('incremental_wave', 1) | int >= 2 %}
    , cast(order_total * 2 as numeric) as doubled_total
    {% endif %}
from {{ ref('fct_orders') }}
