select `order`
from {{ ref('exotic_quoted_identifiers') }}
where `order` != 1
   or `Mixed Case` != 'mixed case'
   or `dash-name` != 'hyphen'
   or `名前-列` != '東京'
