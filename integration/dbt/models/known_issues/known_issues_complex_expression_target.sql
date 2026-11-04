{{ config(materialized='view', tags=['known_dbt_issue']) }}

select
    1 as id,
    struct('A' as field1, 'B' as field2) as struct_field,
    st_geogpoint(100, -37) as some_point
