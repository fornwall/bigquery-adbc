{{ config(
    materialized='materialized_view',
    enable_refresh=true,
    refresh_interval_minutes=var('mv_refresh_minutes', 45),
    max_staleness='INTERVAL "8:0:0" HOUR TO SECOND',
    cluster_by=['status'],
    labels={'component': 'adbc_integration', 'coverage': 'materialized_view'},
    on_configuration_change='apply'
) }}

select
    status,
    count(*) as order_count,
    sum(order_total) as booked_revenue
from {{ ref('fct_orders') }}
group by status
