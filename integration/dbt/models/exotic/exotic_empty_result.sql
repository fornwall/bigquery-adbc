{{ config(
    materialized='view',
    labels={'component': 'adbc_integration', 'coverage': 'empty_result'}
) }}

{# An empty ADBC result must retain the names and types dbt needs to render it. #}
select
    cast(null as int64) as row_id,
    cast(null as string) as label,
    cast(null as array<struct<item_id int64, note string>>) as items
from unnest([1])
where false
