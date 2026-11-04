with expected as (
    select
        1 as row_id,
        'pipe | value' as label,
        cast(null as string) as optional_label,
        'NULL' as literal_null,
        42 as signed_value,
        0.1 as percent_ratio,
        timestamp '2026-01-10 15:00:00+00' as event_ts
    union all
    select 2, '', 'quoted "value"', 'null', -7, -0.5,
           timestamp '2026-01-11 14:59:59+00'
    union all
    select 3, 'Grüße 東京 👩🏽‍💻', 'plain', 'Null', 0, 0.0,
           timestamp '2026-01-12 00:00:00+00'
)

select 'missing' as problem, *
from (select * from expected except distinct select * from {{ ref('exotic_csv_edges') }})
union all
select 'unexpected' as problem, *
from (select * from {{ ref('exotic_csv_edges') }} except distinct select * from expected)
