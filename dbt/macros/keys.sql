{% macro hash_key(columns) -%}
    {#- Stable BIGINT surrogate key: xxhash64 of the natural key, so a full rebuild gives every row the
        same key again. -1 stays reserved for the UNKNOWN member of each dimension. -#}
    from_big_endian_64(xxhash64(to_utf8(
        {%- for column in columns %}
        coalesce(cast({{ column }} as varchar), '<null>'){% if not loop.last %} || '|' ||{% endif %}
        {%- endfor %}
    )))
{%- endmacro %}


{% macro date_key(column) -%}
    {#- yyyyMMdd INTEGER key into dim_date (NULL when the timestamp is NULL) -#}
    cast(year({{ column }}) * 10000 + month({{ column }}) * 100 + day({{ column }}) as integer)
{%- endmacro %}


{% macro gold_properties() -%}
    {#- Trino Iceberg table properties of every gold model. Polaris places the table under the
        catalog's base location (s3://lakehouse/gold/...); spec v2 keeps it readable by every engine. -#}
    {{ return({"format_version": "2"}) }}
{%- endmacro %}
