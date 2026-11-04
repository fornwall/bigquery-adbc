{{ config(tags=['known_dbt_issue', 'current_dbt_issue']) }}

select cast('1-2 3 10:20:30.456' as interval) as interval_value
