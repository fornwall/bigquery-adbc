{{ config(materialized='view', tags=['known_dbt_issue']) }}

select
    1 as company_id,
    date '2025-01-01' as closing_date,
    struct(100 as revenue, 99 as debt) as financials
union all
select
    1,
    date '2024-01-01',
    struct(75 as revenue, 76 as debt)
