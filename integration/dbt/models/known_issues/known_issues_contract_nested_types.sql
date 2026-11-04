{{ config(
    materialized='table',
    contract={'enforced': true},
    persist_docs={'relation': true, 'columns': true},
    tags=['known_dbt_issue']
) }}

select
    cast(1 as int64) as id,
    [
        struct(7 as d, 21 as e, 5 as b_id, 'color' as a_color),
        struct(11 as d, 13 as e, 3 as b_id, 'different color' as a_color)
    ] as array_of_structs,
    struct(3 as id, 'taste' as taste) as struct_column
