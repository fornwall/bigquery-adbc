{{ config(
    materialized='table',
    pre_hook=[
      "create or replace table `{{ target.database }}.{{ target.schema }}.exotic_hook_audit` (event string, model_name string, observed_rows int64)",
      "insert into `{{ target.database }}.{{ target.schema }}.exotic_hook_audit` values ('pre', '{{ this.identifier }}', 0)"
    ],
    post_hook="insert into `{{ target.database }}.{{ target.schema }}.exotic_hook_audit` select 'post', '{{ this.identifier }}', count(*) from {{ this }}"
) }}

select
    order_id,
    customer_id,
    order_total
from {{ ref('fct_orders') }}
where status = 'completed'
