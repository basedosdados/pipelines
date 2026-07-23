-- This macro was created to provide a standardized way to deal with cases
-- where there's a mismatch between original data col values and original
-- data dictionary values
{% macro dicionario_not_found(
    id_tabela,
    nome_coluna,
    chave,
    cobertura_temporal="(1)",
    valor="O valor desta chave foi encontrado nos dados originais mas não existe no dicionário informado na fonte original. Por este motivo, não sabemos a tradução do código."
) %}
    {%- set nome_colunas = [nome_coluna] if nome_coluna is string else nome_coluna -%}
    {%- set chaves = [chave] if chave is string else chave -%}

    {%- if nome_colunas | length != chaves | length and nome_colunas | length != 1 and chaves | length != 1 -%}
        {{
            exceptions.raise_compiler_error(
                "nome_coluna and chave must have the same length or one must be a single value"
            )
        }}
    {%- endif -%}

    {%- set max_len = [nome_colunas | length, chaves | length] | max -%}

    {%- for i in range(max_len) -%}
        {%- set col = (
            nome_colunas[i]
            if nome_colunas | length > 1
            else nome_colunas[0]
        ) -%}
        {%- set key = chaves[i] if chaves | length > 1 else chaves[0] -%}
        select
            safe_cast('{{ id_tabela }}' as string) id_tabela,
            safe_cast('{{ col }}' as string) nome_coluna,
            safe_cast('{{ key }}' as string) chave,
            safe_cast('{{ cobertura_temporal }}' as string) cobertura_temporal,
            safe_cast('{{ valor }}' as string) valor
        {%- if not loop.last %}
            union all
        {% endif -%}
    {%- endfor -%}
{% endmacro %}
