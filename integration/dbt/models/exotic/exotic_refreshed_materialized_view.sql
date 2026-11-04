{{ config(
    materialized='materialized_view',
    enable_refresh=true,
    refresh_interval_minutes=90,
    cluster_by=['status'],
    labels={'component': 'adbc_integration', 'coverage': 'materialized_view_refresh'}
) }}

{#
  Rebuilt unchanged after it is created. dbt compares this configuration with what
  describe_relation reads back through the driver's table metadata, including the refresh
  interval as the Go duration 1h30m0s, and only when nothing differs does it refresh the view
  with CALL bq.refresh_materialized_view instead of altering or replacing it. There is no
  max_staleness: dbt compares the configured text with BigQuery's canonical interval spelling,
  so a view that sets it is altered on every run.
#}
select
    status,
    count(*) as order_count
from {{ ref('fct_orders') }}
group by status
