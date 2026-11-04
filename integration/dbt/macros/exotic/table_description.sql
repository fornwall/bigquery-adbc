{#
  Exercises adapter.update_table_description (adbc.bigquery.table.update_description). No
  project DDL sets the sentinel. The runner checks it again through the REST API.
#}
{% macro assert_table_description_update() %}
  {% if execute %}
    {% set identifier = 'fct_orders' %}
    {% set sentinel = var('table_description_sentinel') %}

    {% do adapter.update_table_description(
        target.database, target.schema, identifier, sentinel) %}

    {% set persisted = run_query(
        "select json_value(option_value) as description from `" ~ target.database ~ "`.`"
        ~ target.schema ~ "`.INFORMATION_SCHEMA.TABLE_OPTIONS where table_name = '"
        ~ identifier ~ "' and option_name = 'description'") %}
    {% if persisted.rows | length != 1 %}
      {% do exceptions.raise_compiler_error(
          'update_table_description left ' ~ identifier ~ ' without a description') %}
    {% endif %}
    {% set actual = persisted.columns[0].values()[0] %}
    {% if actual != sentinel %}
      {% do exceptions.raise_compiler_error(
          'update_table_description did not persist the sentinel: got ' ~ actual) %}
    {% endif %}

    {#- Read the relation back the way dbt itself sees it. -#}
    {% set relation = adapter.get_relation(
        database=target.database, schema=target.schema, identifier=identifier) %}
    {% if relation is none %}
      {% do exceptions.raise_compiler_error(
          'the described relation ' ~ identifier ~ ' disappeared') %}
    {% endif %}

    {% do log('update_table_description persisted the sentinel on ' ~ relation, info=true) %}
  {% endif %}
{% endmacro %}
