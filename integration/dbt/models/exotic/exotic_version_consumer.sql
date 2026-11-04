with legacy as (
    select order_id, metric_value, metric_definition
    from {{ ref('exotic_versioned_metric', version=1) }}
), current_explicit as (
    select order_id, metric_value, metric_definition
    from {{ ref('exotic_versioned_metric', version=2) }}
), current_implicit as (
    select order_id, metric_value, metric_definition
    from {{ ref('exotic_versioned_metric') }}
)

select
    legacy.order_id,
    legacy.metric_value as legacy_value,
    current_explicit.metric_value as current_value,
    legacy.metric_value - current_explicit.metric_value as discount_value,
    legacy.metric_definition as legacy_definition,
    current_explicit.metric_definition as current_definition,
    current_explicit.metric_value = current_implicit.metric_value
        and current_explicit.metric_definition = current_implicit.metric_definition
        as unversioned_ref_uses_latest
from legacy
inner join current_explicit using (order_id)
inner join current_implicit using (order_id)
