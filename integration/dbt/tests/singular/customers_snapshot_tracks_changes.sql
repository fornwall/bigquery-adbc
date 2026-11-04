{{ config(tags=['snapshot_final']) }}

with expected as (
    select 1 as customer_id, 'Alice Andersson' as customer_name,
           timestamp '2026-02-01 00:00:00' as valid_from,
           cast(null as timestamp) as valid_to
    union all
    select 2, 'Bob Berg', timestamp '2026-02-01 00:00:00', timestamp '2026-02-02 00:00:00'
    union all
    select 2, 'Robert Berg', timestamp '2026-02-02 00:00:00', cast(null as timestamp)
    union all
    select 3, 'Carol Chen', timestamp '2026-02-02 00:00:00', cast(null as timestamp)
), actual as (
    select customer_id, customer_name, dbt_valid_from as valid_from, dbt_valid_to as valid_to
    from {{ ref('customers_snapshot') }}
)

select 'missing' as problem, *
from (select * from expected except distinct select * from actual)
union all
select 'unexpected' as problem, *
from (select * from actual except distinct select * from expected)

