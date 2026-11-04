{{ config(
    materialized='view',
    grant_access_to=[
      {'project': target.database, 'dataset': target.schema ~ '_authorized_source'}
    ],
    persist_docs={'columns': true},
    tags=['authorized_view']
) }}

{#
  dbt calls adapter.grant_access_to after every build, which runs the driver's
  adbc.bigquery.dataset.authorize_view_to_datasets option. The driver skips a dataset that
  already lists the view, so rebuilds add no duplicate access entries.

  CREATE OR REPLACE VIEW drops column descriptions, so each build's persist_docs must patch
  them onto the view's schema again through adbc.bigquery.table.update_columns_description.
#}
select
    order_date,
    count(*) as order_count,
    sum(order_total) as booked_revenue
from {{ ref('exotic_authorized_source') }}
group by order_date
