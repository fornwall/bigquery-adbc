{{ config(
    materialized='table',
    partition_by={
      'field': 'bucket',
      'data_type': 'int64',
      'range': {'start': 0, 'end': 100, 'interval': 10}
    },
    cluster_by=['parity'],
    hours_to_expiration=48,
    labels={'component': 'adbc_integration', 'coverage': 'range_partition'}
) }}

select
    mod(customer_id * 10, 100) as bucket,
    if(mod(order_id, 2) = 0, 'even', 'odd') as parity,
    order_id,
    order_total
from {{ ref('fct_orders') }}
