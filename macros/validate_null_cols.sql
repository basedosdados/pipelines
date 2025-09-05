{%- macro validate_null_cols(column) -%}
    case
        when {{ column }} in ('nan', 'NaN', 'Nan', 'None', 'none')
        then null
        else {{ column }}
    end
{%- endmacro %}
