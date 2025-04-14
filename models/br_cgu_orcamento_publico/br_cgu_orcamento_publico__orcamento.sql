{{
    config(
        alias="orcamento", schema="br_cgu_orcamento_publico", materialized="table"
    )
}}
select
    safe_cast(ano_exercicio as int64) ano_exercicio,
    safe_cast(codigo_orgao_superior as string) id_orgao_superior,
    safe_cast(nome_orgao_superior as string) nome_orgao_superior,
    safe_cast(codigo_orgao_subordinado as string) id_orgao_subordinado,
    safe_cast(nome_orgao_subordinado as string) nome_orgao_subordinado,
    safe_cast(codigo_unidade_orcamentaria as string) id_unidade_orcamentaria,
    safe_cast(nome_unidade_orcamentaria as string) nome_unidade_orcamentaria,
    safe_cast(codigo_funcao as string) id_funcao,
    safe_cast(nome_funcao as string) nome_funcao,
    safe_cast(codigo_subfuncao as string) id_subfuncao,
    safe_cast(nome_subfuncao as string) nome_subfuncao,
    safe_cast(codigo_programa_orcamentario as string) id_programa_orcamentario,
    safe_cast(nome_programa_orcamentario as string) nome_programa_orcamentario,
    safe_cast(codigo_acao as string) id_acao,
    safe_cast(nome_acao as string) nome_acao,
    safe_cast(codigo_categoria_economica as string) id_categoria_economica,
    safe_cast(nome_categoria_economica as string) nome_categoria_economica,
    safe_cast(codigo_grupo_de_despesa as string) id_grupo_despesa,
    safe_cast(nome_grupo_de_despesa as string) nome_grupo_despesa,
    safe_cast(codigo_elemento_de_despesa as string) id_elemento_despesa,
    safe_cast(nome_elemento_de_despesa as string) nome_elemento_despesa,
    safe_cast(orcamento_inicial as float64) orcamento_inicial,
    safe_cast(orcamento_atualizado as float64) orcamento_atualizado,
    safe_cast(orcamento_empenhado as float64) orcamento_empenhado,
    safe_cast(orcamento_realizado as float64) orcamento_realizado,
    safe_cast(
        porcentagem_realizado_orcamento as float64
    ) porcentagem_realizado_orcamento,
from {{ set_datalake_project("br_cgu_orcamento_publico_staging.orcamento") }}
