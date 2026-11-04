{{ config(tags=['known_dbt_issue', 'current_dbt_issue']) }}

from unnest([struct(1 as id, 'alpha' as name)])
|> select id, name
