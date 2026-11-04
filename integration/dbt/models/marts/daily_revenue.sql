{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'order_date', 'data_type': 'date'},
    partitions=[
      "date '2026-01-10'",
      "date '2026-01-11'",
      "date '2026-01-12'"
    ],
    tags=['incremental']
) }}

select
    order_date,
    countif(status = 'completed') as completed_orders,
    sum(if(status = 'completed', order_total, 0)) as recognized_revenue,
    sum(order_total) as booked_revenue
from {{ ref('fct_orders') }}
group by order_date

