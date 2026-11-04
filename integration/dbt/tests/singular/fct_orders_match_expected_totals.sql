with expected as (
    select 101 as order_id, numeric '29.00' as order_total union all
    select 102, numeric '10.00' union all
    select 103, numeric '24.25' union all
    select 104, numeric '94.00' union all
    select 105, numeric '42.00'
)

select coalesce(actual.order_id, expected.order_id) as order_id
from {{ ref('fct_orders') }} as actual
full outer join expected using (order_id)
where actual.order_id is null
   or expected.order_id is null
   or actual.order_total != expected.order_total

