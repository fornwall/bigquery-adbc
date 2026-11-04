{#
  Exercises adapter calls that materializations reach only indirectly: relation lookup,
  column introspection, statement/load_result and run_query. Raises on any mismatch.
#}
{% macro assert_adapter_surface() %}
  {% if execute %}
    {% set relation = adapter.get_relation(
        database=target.database, schema=target.schema, identifier='fct_orders') %}
    {% if relation is none %}
      {% do exceptions.raise_compiler_error('adapter.get_relation did not find fct_orders') %}
    {% endif %}

    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% set column_names = columns | map(attribute='name') | list %}
    {% for expected in ['order_id', 'order_date', 'status', 'line_items'] %}
      {% if expected not in column_names %}
        {% do exceptions.raise_compiler_error(
            'get_columns_in_relation missing ' ~ expected ~ ' in ' ~ column_names | join(', ')) %}
      {% endif %}
    {% endfor %}

    {% call statement('order_count', fetch_result=true) %}
      select count(*) as orders from {{ relation }}
    {% endcall %}
    {% set from_statement = load_result('order_count')['data'][0][0] %}

    {% set query_result = run_query('select count(*) as orders from ' ~ relation) %}
    {% set from_run_query = query_result.columns[0].values()[0] %}

    {% if from_statement != from_run_query %}
      {% do exceptions.raise_compiler_error(
          'statement/load_result and run_query disagree: '
          ~ from_statement ~ ' vs ' ~ from_run_query) %}
    {% endif %}
    {% if from_statement | int != 5 %}
      {% do exceptions.raise_compiler_error(
          'expected five fct_orders rows, got ' ~ from_statement) %}
    {% endif %}

    {#
      60005 rows of a 256-byte payload overflow BigQuery's first page. Explicitly disable
      Storage Read on this statement to exercise REST pagination independently of the
      driver's default routing policy. The canonical driver exits on this statement
      (KNOWN_ISSUES.md).
    #}
    {% if not var('canonical_driver', false) %}
    {% set paginated_response, paginated = adapter.execute(
      "select n, if(mod(n, 997) = 0, cast(null as string), repeat('x', 256)) as payload "
      ~ "from unnest(generate_array(1, 60005)) as n order by n",
      auto_begin=false,
      fetch=true,
      options={'adbc.bigquery.sql.query.use_storage_api_disabled_client': 'true'}
    ) %}
    {% if paginated.rows | length != 60005 %}
      {% do exceptions.raise_compiler_error(
          'REST pagination returned ' ~ paginated.rows | length ~ ' rows, expected 60005') %}
    {% endif %}
    {% for row in paginated.rows %}
      {% if row[0] | int != loop.index %}
        {% do exceptions.raise_compiler_error(
            'REST pagination lost row order at row ' ~ loop.index) %}
      {% endif %}
    {% endfor %}
    {% if paginated.rows[996][1] is not none
          or paginated.rows[997][1] | length != 256 %}
      {% do exceptions.raise_compiler_error('REST pagination lost null or string values') %}
    {% endif %}
    {% endif %}

    {#
      200000 narrow rows fill BigQuery's 100,000-row first page and leave a continuation token,
      selecting Storage Read with its default single stream so the order holds. BigQuery
      answers this sequence with a job. The runner requires each
      read path's debug event and fails the phase on any driver warning, such as a fallback to
      REST.
    #}
    {% set storage_read = run_query(
      "select n, if(mod(n, 997) = 0, cast(null as string), concat('row-', cast(n as string))) "
      ~ "as label from unnest(generate_array(1, 200000)) as n order by n"
    ) %}
    {% if storage_read.rows | length != 200000 %}
      {% do exceptions.raise_compiler_error(
          'Storage Read run_query returned ' ~ storage_read.rows | length
          ~ ' rows, expected 200000') %}
    {% endif %}
    {% for row in storage_read.rows %}
      {% if row[0] | int != loop.index %}
        {% do exceptions.raise_compiler_error(
            'Storage Read run_query lost row order at row ' ~ loop.index) %}
      {% endif %}
    {% endfor %}
    {% if storage_read.rows[996][1] is not none
          or storage_read.rows[997][1] != 'row-998'
          or storage_read.rows[199999][1] != 'row-200000' %}
      {% do exceptions.raise_compiler_error('Storage Read run_query lost null or string values') %}
    {% endif %}

    {% set empty_result = run_query(
      "select cast(null as int64) as row_id, cast(null as string) as label "
      ~ "from unnest([1]) where false"
    ) %}
    {% set empty_names = empty_result.columns | map(attribute='name') | list %}
    {% if empty_result.rows | length != 0 or empty_names != ['row_id', 'label'] %}
      {% do exceptions.raise_compiler_error(
          'typed empty run_query lost its schema: ' ~ empty_names | join(', ')) %}
    {% endif %}

    {% do log('adapter surface checks passed for ' ~ relation, info=true) %}
  {% endif %}
{% endmacro %}
