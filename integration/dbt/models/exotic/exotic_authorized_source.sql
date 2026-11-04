{{ config(
    materialized='table',
    schema='authorized_source',
    labels={'component': 'adbc_integration', 'coverage': 'authorized_view'}
) }}

{# A second dataset, so exotic_authorized_view needs dataset authorization to read it. #}
select
    order_id,
    customer_id,
    order_date,
    order_total
from {{ ref('fct_orders') }}
