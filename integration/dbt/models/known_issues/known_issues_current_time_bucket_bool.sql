{{ config(tags=['known_dbt_issue', 'current_dbt_issue']) }}

select
    least(false, true) as boolean_check,
    timestamp_bucket(timestamp '2026-01-01 00:07:00+00', interval 15 minute)
        as timestamp_bucket_check
