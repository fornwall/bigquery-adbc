-- depends_on: {{ ref('exotic_hooked_model') }}

with events as (
    select
        event,
        model_name,
        observed_rows
    from `{{ target.database }}.{{ target.schema }}.exotic_hook_audit`
), expected as (
    select 'pre' as event, 'exotic_hooked_model' as model_name, 0 as observed_rows
    union all
    select 'post', 'exotic_hooked_model', 3
)

select 'missing' as problem, *
from (select * from expected except distinct select * from events)
union all
select 'unexpected' as problem, *
from (select * from events except distinct select * from expected)
