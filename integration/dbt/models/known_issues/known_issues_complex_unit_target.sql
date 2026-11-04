{{ config(materialized='table', tags=['known_dbt_issue']) }}

select
    company_id,
    struct(
        array_agg(
            struct(closing_date, financials.revenue as amount)
            order by closing_date desc
        ) as revenue,
        array_agg(
            struct(closing_date, financials.debt as amount)
            order by closing_date desc
        ) as debt
    ) as financial_history
from {{ ref('known_issues_complex_input') }}
group by company_id
