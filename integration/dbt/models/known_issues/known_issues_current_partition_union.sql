{{ config(materialized='view', tags=['known_dbt_issue', 'current_dbt_issue']) }}

-- Regression for dbt-labs/dbt-core#14383: pseudo-columns must not enter SELECT *.
select * from {{ ref('known_issues_current_partition_day') }}
union all
select * from {{ ref('known_issues_current_partition_month') }}
