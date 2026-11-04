{{ config(
    alias='select',
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    labels={'component': 'adbc_integration', 'coverage': 'quoted_identifiers'}
) }}

{#
  A reserved-keyword alias and columns mixing a keyword with BigQuery flexible names: every
  SQL and metadata reference must keep the display name and quote it.
#}
select
    1 as `order`,
    'mixed case' as `Mixed Case`,
    'hyphen' as `dash-name`,
    '東京' as `名前-列`
