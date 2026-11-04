{{ config(tags=['incremental']) }}

{% set wave = var('incremental_wave', 1) | int %}

with expected_rows as (
    select * from unnest(array<struct<
        customer_id int64, order_id int64, order_date date,
        load_wave int64, original_wave int64, details_amount numeric
    >>[
        {% if wave == 1 %}
        (1, 101, date '2026-01-10', 1, 1, null),
        (2, 101, date '2026-01-10', 1, 1, null),
        {% else %}
        (1, 101, date '2026-01-10', 2, 1, null),
        (2, 101, date '2026-01-10', 2, 1, null),
        (3, 103, date '2026-01-12', 2, 2, 40),
        (4, 104, date '2026-01-13', 2, 2, 50),
        {% endif %}
        (1, 102, date '2026-01-11', 1, 1, null),
        (3, 103, date '2025-12-31', 1, 1, null)
    ])
), expected as (
    select
        customer_id, order_id, order_date, load_wave,
        format('wave-%d/customer-%d', load_wave, customer_id) as wave_payload,
        original_wave,
        if(details_amount is null, null,
           struct('wave-2' as status, details_amount as amount)) as current_details
    from expected_rows
), actual as (
    select
        customer_id, order_id, order_date, load_wave, wave_payload, original_wave,
        {% if wave == 1 %}
        cast(null as struct<status string, amount numeric>) as current_details
        {% else %}
        current_details
        {% endif %}
    from {{ ref('exotic_schema_sync') }}
), expected_counts as (
    select to_json_string(row) as row_value, count(*) as copies
    from expected as row group by 1
), actual_counts as (
    select to_json_string(row) as row_value, count(*) as copies
    from actual as row group by 1
)

-- Compare multiplicities as well as values; NULLs and duplicate rows must not pass silently.
select row_value, expected_counts.copies as expected, actual_counts.copies as actual
from expected_counts full outer join actual_counts using (row_value)
where expected_counts.copies is distinct from actual_counts.copies
