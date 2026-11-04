{{ config(tags=['known_dbt_issue', 'current_dbt_issue']) }}

with base as (
    select date '2024-01-01' as status_date, 1 as value
    union all select date '2024-01-03', 2
)
select *
from gap_fill(
    table base,
    ts_column => 'status_date',
    bucket_width => interval 1 day,
    value_columns => [('value', 'null')],
    ignore_null_values => false
)
