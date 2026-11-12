{% test no_invalid_chars(model, column_name) %}

    {#- Marca como falha linhas com caracteres de encoding inválido (issue 1533):
        o bloco U+FFF0–U+FFFF (inclui o caractere de substituição "�") e caracteres
        de controle U+0000–U+001F. -#}
    select {{ column_name }} as invalid_value
    from {{ model }}
    where
        {{ column_name }} is not null
        and regexp_contains(cast({{ column_name }} as string), r'[\x{fff0}-\x{ffff}]')
{% endtest %}
