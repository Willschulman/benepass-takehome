{% macro build_dq_flags(flags) %}
    list_filter(
        list_value(
            {% for flag in flags %}
                case when {{ flag.condition }} then '{{ flag.name }}' end
                {% if not loop.last %},{% endif %}
            {% endfor %}
        ),
        x -> x is not null
    )
{% endmacro %}
