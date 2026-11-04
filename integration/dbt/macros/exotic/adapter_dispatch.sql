{% macro exotic_fingerprint(expression) -%}
  {{ return(adapter.dispatch('exotic_fingerprint', 'bigquery_adbc_integration')(expression)) }}
{%- endmacro %}

{% macro default__exotic_fingerprint(expression) -%}
  md5(cast({{ expression }} as string))
{%- endmacro %}

{% macro bigquery__exotic_fingerprint(expression) -%}
  to_hex(sha256(cast({{ expression }} as bytes)))
{%- endmacro %}
