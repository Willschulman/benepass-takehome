{% macro normalize_timestamp(column_name) %}
    cast({{ column_name }} as timestamp)
{% endmacro %}
