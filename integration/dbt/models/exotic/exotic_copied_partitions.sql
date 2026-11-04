{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      'field': 'order_date',
      'data_type': 'date',
      'copy_partitions': true
    },
    partitions=[
      "date '2026-01-10'",
      "date '2026-01-11'"
    ],
    tags=['incremental'],
    labels={'component': 'adbc_integration', 'coverage': 'copied_partitions'},
    job_execution_timeout_seconds=900
) }}

{#
  copy_partitions replaces each listed partition through adapter.copy_table
  (adbc.bigquery.copy_table.*). The unlisted 2026-01-12 partition must keep its first-wave
  rows, which distinguishes a per-partition copy from a full overwrite.

  The model's job timeout overrides the profile's 300 seconds. The runner requires each copy job
  to carry it and dbt's job labels, which the driver takes from the statement's query options.
#}
select
    order_date,
    count(*) as order_count,
    sum(order_total) as booked_revenue,
    cast({{ var('incremental_wave', 1) }} as int64) as load_wave
from {{ ref('fct_orders') }}
group by order_date
