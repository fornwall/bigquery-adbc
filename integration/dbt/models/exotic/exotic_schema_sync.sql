{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['customer_id', 'order_id'],
    on_schema_change='sync_all_columns',
    merge_update_columns=['load_wave', 'wave_payload'],
    incremental_predicates=[
      "DBT_INTERNAL_DEST.order_date >= date '2026-01-01'"
    ],
    partition_by={'field': 'order_date', 'data_type': 'date'},
    tags=['incremental'],
    labels={'component': 'adbc_integration', 'coverage': 'schema_sync'}
) }}

{% set wave = var('incremental_wave', 1) | int %}

{#
  Neither component of the merge key is unique. Wave two omits (1, 102), changes the
  source's protected original_wave, and introduces (4, 104). The old (3, 103) lies
  outside the destination predicate, so its new date must insert a second version.
#}
with input as (
    select * from unnest(array<struct<
        customer_id int64, order_id int64, order_date date, amount numeric
    >>[
        (1, 101, date '2026-01-10', 10),
        (2, 101, date '2026-01-10', 20),
        {% if wave == 1 %}
        (1, 102, date '2026-01-11', 30),
        (3, 103, date '2025-12-31', 40)
        {% else %}
        (3, 103, date '2026-01-12', 40),
        (4, 104, date '2026-01-13', 50)
        {% endif %}
    ])
)
select
    customer_id,
    order_id,
    order_date,
    cast({{ wave }} as int64) as load_wave,
    format('wave-%d/customer-%d', {{ wave }}, customer_id) as wave_payload,
    cast({{ wave }} as int64) as original_wave
    {% if wave == 1 %}
    , 'removed by sync_all_columns' as retired_column
    {% else %}
    , struct('wave-2' as status, amount as amount) as current_details
    {% endif %}
from input
