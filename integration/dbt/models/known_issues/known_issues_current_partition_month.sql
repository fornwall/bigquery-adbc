{{ config(
    materialized='table',
    partition_by={'field': 'dt', 'data_type': 'date', 'granularity': 'month'},
    tags=['known_dbt_issue', 'current_dbt_issue']
) }}

select date '2026-01-01' as dt, 1 as a, 2 as b
