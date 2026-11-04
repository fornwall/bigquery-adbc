{{ config(
    materialized='table',
    contract={'enforced': true}
) }}

select
    id as parent_id,
    concat('child-', cast(id as string)) as note
from {{ ref('typed_features') }}

