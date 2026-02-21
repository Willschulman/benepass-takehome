{% macro clean_string(column_name) %}
    trim({{ column_name }})
{% endmacro %}
