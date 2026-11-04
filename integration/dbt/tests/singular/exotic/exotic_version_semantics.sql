select order_id
from {{ ref('exotic_version_consumer') }}
where legacy_definition != 'gross_before_discounts'
   or current_definition != 'net_after_discounts'
   or discount_value < 0
   or not unversioned_ref_uses_latest
