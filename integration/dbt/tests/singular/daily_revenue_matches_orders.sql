with expected as (
    select
        order_date,
        countif(status = 'completed') as completed_orders,
        sum(if(status = 'completed', order_total, 0)) as recognized_revenue,
        sum(order_total) as booked_revenue
    from {{ ref('fct_orders') }}
    group by order_date
)

select coalesce(actual.order_date, expected.order_date) as order_date
from {{ ref('daily_revenue') }} as actual
full outer join expected using (order_date)
where actual.order_date is null
   or expected.order_date is null
   or actual.completed_orders != expected.completed_orders
   or actual.recognized_revenue != expected.recognized_revenue
   or actual.booked_revenue != expected.booked_revenue

