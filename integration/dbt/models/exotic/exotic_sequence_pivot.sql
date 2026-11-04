with recursive generated_numbers as (
    select 1 as number
    union all
    select number + 1
    from generated_numbers
    where number < 6
), bucketed as (
    select
        div(number - 1, 2) + 1 as bucket,
        if(mod(number, 2) = 0, 'even', 'odd') as parity,
        number
    from generated_numbers
), pivoted as (
    select *
    from bucketed
    pivot(sum(number) for parity in ('odd' as odd_total, 'even' as even_total))
)

select bucket, metric_name, metric_value
from pivoted
unpivot(metric_value for metric_name in (odd_total, even_total))
