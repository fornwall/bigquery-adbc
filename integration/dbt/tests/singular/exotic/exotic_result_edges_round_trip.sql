select row_id
from {{ ref('exotic_result_edges') }}
where int64_min != cast('-9223372036854775808' as int64)
   or int64_max != cast('9223372036854775807' as int64)
   or numeric_min != numeric '-99999999999999999999999999999.999999999'
   or numeric_max != numeric '99999999999999999999999999999.999999999'
   -- BigQuery stores negative zero as zero; dbt show covers the query-time value.
   or negative_zero != 0
   or empty_text != ''
   or unicode_text != 'Grüße 東京 👩🏽‍💻'
   or length(empty_bytes) != 0
   or to_hex(bytes_with_nul) != '000100ff'
   or date_min != date '0001-01-01'
   or date_max != date '9999-12-31'
   or timestamp_min != timestamp '0001-01-01 00:00:00+00'
   or timestamp_max != timestamp '9999-12-31 23:59:59.999999+00'
   or array_length(empty_array) != 0
   or array_length(null_array) != 0
   or nested_value.child_id != 7
   or nested_value.optional_label is not null
   or null_struct is not null
   or array_length(repeated_struct) != 2
   or repeated_struct[offset(1)].item_id != 2
   or repeated_struct[offset(1)].label is not null
   or null_text is not null
