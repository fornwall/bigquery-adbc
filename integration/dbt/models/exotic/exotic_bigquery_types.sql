{{ config(
    materialized='table',
    labels={'component': 'adbc_integration', 'coverage': 'exotic_types'}
) }}

select
    1 as id,
    from_hex('c0ffee') as payload_bytes,
    cast('12345678901234567890.12345678901234567890123456789012345678' as bignumeric) as precise_amount,
    json '{"state":"ready","scores":[1,2,3],"nested":{"active":true}}' as document,
    st_geogfromtext('POINT(-122.4194 37.7749)') as location,
    datetime '2026-08-28 14:30:45.123456' as local_datetime,
    time '14:30:45.123456' as local_time
