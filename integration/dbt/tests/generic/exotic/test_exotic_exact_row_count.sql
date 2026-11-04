{% test exotic_exact_row_count(model, expected) %}
select count(*) as actual_row_count
from {{ model }}
having actual_row_count != {{ expected }}
{% endtest %}
