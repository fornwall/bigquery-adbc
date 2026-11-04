{{ config(enabled=var('validate_failure_reporting', false)) }}

-- Enabled only in the runner's negative control: a zero failure count must fail the suite.
select 1 as deliberately_invalid_row
