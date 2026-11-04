with expected as (
    select 1 as bucket, 'odd_total' as metric_name, 1 as metric_value union all
    select 1, 'even_total', 2 union all
    select 2, 'odd_total', 3 union all
    select 2, 'even_total', 4 union all
    select 3, 'odd_total', 5 union all
    select 3, 'even_total', 6
)

select 'missing' as problem, *
from (select * from expected except distinct select * from {{ ref('exotic_sequence_pivot') }})
union all
select 'unexpected' as problem, *
from (select * from {{ ref('exotic_sequence_pivot') }} except distinct select * from expected)
