{% macro mask_pii(column_name) %}
    {% if var('pii_masking_enabled', true) %}
        '****@' || split_part({{ column_name }}, '@', 2)
    {% else %}
        {{ column_name }}
    {% endif %}
{% endmacro %}
