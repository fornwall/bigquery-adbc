{{ config(tags=['known_dbt_issue', 'current_dbt_issue']) }}

-- dbt's bundled driver rejects a RANGE model column, though queries can return RANGE values.
-- Cast to STRING so the shared build passes under both drivers.
select cast(range(date '2026-01-01', date '2026-01-02') as string) as date_range
