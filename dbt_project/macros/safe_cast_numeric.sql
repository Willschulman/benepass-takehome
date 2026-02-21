{% macro safe_cast_numeric(column_name) %}
    try_cast({{ column_name }} as double)
{% endmacro %}
