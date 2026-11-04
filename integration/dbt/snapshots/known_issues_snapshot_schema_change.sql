{% snapshot known_issues_snapshot_schema_change %}

{{ config(
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at',
    tags=['snapshot', 'known_dbt_issue']
) }}

select * from {{ ref('known_issues_snapshot_source') }}

{% endsnapshot %}
