{{ config(
    materialized='table',
    labels={'component': 'adbc_integration', 'coverage': 'result_edges'}
) }}

{#
  Values easy to truncate or coerce between the REST result, Arrow, and dbt show's JSON.
  Non-finite floats are excluded: JSON has no portable spelling for them, so they would test
  dbt's renderer rather than the driver.
#}
select
    1 as row_id,
    cast('-9223372036854775808' as int64) as int64_min,
    cast('9223372036854775807' as int64) as int64_max,
    numeric '-99999999999999999999999999999.999999999' as numeric_min,
    numeric '99999999999999999999999999999.999999999' as numeric_max,
    cast('-0' as float64) as negative_zero,
    '' as empty_text,
    'Grüße 東京 👩🏽‍💻' as unicode_text,
    b'' as empty_bytes,
    from_hex('000100ff') as bytes_with_nul,
    date '0001-01-01' as date_min,
    date '9999-12-31' as date_max,
    timestamp '0001-01-01 00:00:00+00' as timestamp_min,
    timestamp '9999-12-31 23:59:59.999999+00' as timestamp_max,
    array<int64>[] as empty_array,
    cast(null as array<int64>) as null_array,
    struct(7 as child_id, cast(null as string) as optional_label) as nested_value,
    cast(null as struct<child_id int64, optional_label string>) as null_struct,
    [
      struct(1 as item_id, 'first' as label),
      struct(2 as item_id, cast(null as string) as label)
    ]
        as repeated_struct,
    cast(null as string) as null_text
