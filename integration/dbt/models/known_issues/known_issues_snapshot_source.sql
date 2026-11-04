{{ config(materialized='table', tags=['snapshot', 'known_dbt_issue']) }}

{% set wave = var('snapshot_wave', 1) | int %}

{% if wave == 1 %}
select
    1 as customer_id,
    'alpha' as customer_value,
    timestamp '2026-03-01 00:00:00+00' as updated_at
{% else %}
select
    1 as customer_id,
    'beta' as customer_value,
    'column added in wave two' as customer_note,
    timestamp '2026-03-02 00:00:00+00' as updated_at
{% endif %}
