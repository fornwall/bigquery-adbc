{{ config(
    materialized='table',
    tags=['known_dbt_issue']
) }}

-- Exact or reduced regressions from dbt-labs/dbt issues:
-- #13561, #13568, #13624, #13793, #13806, #13811, #14089 and #14106.
with timestamp_union as (
    select 0 as row_id, timestamp '2008-12-25 15:30:00+00' as event_at
    union all
    select order_id, ordered_at
    from {{ ref('orders') }}
),

full_union_by_name as (
    select 1 as row_id, 'left' as left_value
    full union all by name
    select 2 as row_id, 'right' as right_value
),

implicit_typeless_structs as (
    select field_in_struct
    from unnest([
        struct('a' as field_in_struct),
        struct('b'),
        struct('c')
    ])
),

implicit_typed_structs as (
    select a, b
    from unnest([
        struct<a string, b string>('A1', 'B1'),
        ('A2', 'B2')
    ])
),

approximate_top_sum as (
    select approx_top_sum(item, weight, 2) as ranked_items
    from unnest([
        struct('apple' as item, 3 as weight),
        ('pear', 2),
        ('banana', 5),
        ('pear', 4)
    ])
)

select
    array_first([11, 22]) as first_array_value,
    (select count(*) from timestamp_union) as timestamp_union_rows,
    (select count(*) from full_union_by_name) as full_union_rows,
    (select string_agg(field_in_struct, '' order by field_in_struct)
     from implicit_typeless_structs) as typeless_struct_values,
    (select string_agg(concat(a, b), ',' order by a)
     from implicit_typed_structs) as typed_struct_values,
    cast(interval 1 year as interval) as interval_value,
    extract(hour from time '12:34:56') as extracted_hour,
    (select ranked_items[safe_offset(0)].sum from approximate_top_sum) as top_weight
