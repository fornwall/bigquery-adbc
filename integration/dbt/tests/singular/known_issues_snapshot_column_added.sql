{{ config(tags=['snapshot', 'snapshot_final', 'known_dbt_issue']) }}

-- Regression for dbt-labs/dbt-core#15618. The second snapshot wave adds customer_note.
select customer_id
from {{ ref('known_issues_snapshot_schema_change') }}
where dbt_valid_to is null
  and customer_note is null
