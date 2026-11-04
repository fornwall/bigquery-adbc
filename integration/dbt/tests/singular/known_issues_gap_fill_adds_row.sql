-- dbt-labs/dbt-core#14210: the optional ignore_null_values argument remains accepted.
select count(*) as actual_rows
from {{ ref('known_issues_current_gap_fill') }}
having actual_rows != 3
