-- BigQuery normalizes JSON object keys into sorted order and prints WKT in its own spelling.
with expected as (
    select
        1 as row_id,
        'c0ffee' as payload_hex,
        '{"nested":{"active":true},"state":"ready"}' as document_json,
        'POINT(-122.4194 37.7749)' as location_wkt
    union all
    select 2, '000100ff', '[1,"two",null]', 'LINESTRING(0 0, 1 1)'
    union all
    select 3, cast(null as string), cast(null as string), cast(null as string)
), actual as (
    select
        row_id,
        to_hex(payload) as payload_hex,
        -- TO_JSON_STRING renders a SQL NULL as the text null.
        if(document is null, null, to_json_string(document)) as document_json,
        st_astext(location) as location_wkt
    from {{ ref('exotic_encoded_types') }}
)

select 'missing' as problem, *
from (select * from expected except distinct select * from actual)
union all
select 'unexpected' as problem, *
from (select * from actual except distinct select * from expected)
