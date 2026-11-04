-- Regression for dbt-labs/dbt-core#15105. A green query has no rows.
-- depends_on: {{ ref('known_issues_contract_nested_types') }}
with metadata as (
    select column_name, is_nullable
    from `{{ target.database }}.{{ target.schema }}.INFORMATION_SCHEMA.COLUMNS`
    where table_name = 'known_issues_contract_nested_types'
      and column_name = 'id'
),
failures as (
    select 'missing required id metadata' as failure
    from unnest([1])
    where not exists (select 1 from metadata)
    union all
    select 'id remained nullable'
    from metadata
    where is_nullable != 'NO'
)
select * from failures
