{{ config(tags=['known_dbt_issue', 'current_dbt_issue']) }}

with geopoint as (
    select st_geogpoint(-122, 47) as point
)
select st_snaptogrid(point, 0.001) as grid_cell
from geopoint
