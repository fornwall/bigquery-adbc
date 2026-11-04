{{ config(materialized='table', tags=['snapshot']) }}

{% set wave = var('snapshot_wave', 1) | int %}

select 1 as customer_id, 'Alice Andersson' as customer_name,
       timestamp '2026-02-01 00:00:00' as updated_at
union all
{% if wave == 1 %}
select 2, 'Bob Berg', timestamp '2026-02-01 00:00:00'
{% else %}
select 2, 'Robert Berg', timestamp '2026-02-02 00:00:00'
union all
select 3, 'Carol Chen', timestamp '2026-02-02 00:00:00'
{% endif %}

