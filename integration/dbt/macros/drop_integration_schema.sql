{# Drops both suite datasets: the target and the authorized-view source dataset. #}
{% macro drop_integration_schema() %}
  {% for schema in [target.schema, target.schema ~ '_authorized_source'] %}
    {% do adapter.drop_schema(api.Relation.create(database=target.database, schema=schema)) %}
  {% endfor %}
{% endmacro %}

