{% macro record_run_metrics(results) %}
    {#- on-run-end: append one row per model run to monitoring.pipeline_metrics, the Delta table the
        Spark jobs record into (common/metrics.py) and the Grafana Pipeline Monitoring dashboard reads.
        Like common.metrics, this never fails the run: it is skipped until register_monitoring has
        exposed the table to Trino or when the table lacks a column. -#}
    {% if not execute %}{{ return('') }}{% endif %}

    {% set metrics = adapter.get_relation(database=target.database, schema='monitoring', identifier='pipeline_metrics') %}
    {% if metrics is none %}
        {{ log('monitoring.pipeline_metrics is not registered in Trino yet -- gold run metrics skipped', info=true) }}
        {{ return('') }}
    {% endif %}

    {% set columns = ['run_id', 'layer', 'table_name', 'row_count', 'duration_sec', 'status', 'error_message', 'recorded_at', 'write_mode'] %}
    {% set existing = adapter.get_columns_in_relation(metrics) | map(attribute='name') | list %}
    {% for column in columns if column not in existing %}
        {{ log('monitoring.pipeline_metrics has no column ' ~ column ~ ' -- gold run metrics skipped', info=true) }}
        {{ return('') }}
    {% endfor %}

    {% set recorded_at = modules.datetime.datetime.now(modules.pytz.utc).strftime('%Y-%m-%d %H:%M:%S') %}
    {% set rows = [] %}
    {% for result in results if result.node.resource_type == 'model' and result.status in ('success', 'error') %}
        {% set row_count = result.adapter_response.get('rows_affected') %}
        {% set error = '' if result.status == 'success' else (result.message or '')[:500] %}
        {% do rows.append("('" ~ run_started_at.isoformat() ~ "', 'gold', '" ~ result.node.name ~ "', "
            ~ (row_count if row_count is not none else -1) ~ ", " ~ (result.execution_time | round(2)) ~ "e0, '"
            ~ ('success' if result.status == 'success' else 'failed') ~ "', '" ~ error | replace("'", "''") ~ "', '"
            ~ recorded_at ~ "', 'overwrite')") %}
    {% endfor %}

    {% if rows %}
        insert into {{ metrics }} ({{ columns | join(', ') }})
        values {{ rows | join(', ') }}
    {% endif %}
{% endmacro %}
