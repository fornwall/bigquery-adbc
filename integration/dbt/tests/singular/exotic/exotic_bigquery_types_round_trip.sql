select id
from {{ ref('exotic_bigquery_types') }}
where to_hex(payload_bytes) != 'c0ffee'
   or cast(precise_amount as string) != '12345678901234567890.12345678901234567890123456789012345678'
   or json_value(document, '$.state') != 'ready'
   or json_value(document, '$.nested.active') != 'true'
   or to_json_string(json_query_array(document, '$.scores')) != '[1,2,3]'
   or not st_equals(location, st_geogfromtext('POINT(-122.4194 37.7749)'))
   or local_datetime != datetime '2026-08-28 14:30:45.123456'
   or local_time != time '14:30:45.123456'
