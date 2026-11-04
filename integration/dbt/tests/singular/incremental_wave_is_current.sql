{{ config(tags=['incremental']) }}

select order_id
from {{ ref('incremental_order_totals') }}
where load_wave != cast({{ var('incremental_wave', 1) }} as int64)
