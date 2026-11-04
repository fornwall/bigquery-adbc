{% macro discounted_amount(quantity, unit_price, discount) -%}
  ({{ quantity }} * {{ unit_price }}) - {{ discount }}
{%- endmacro %}

