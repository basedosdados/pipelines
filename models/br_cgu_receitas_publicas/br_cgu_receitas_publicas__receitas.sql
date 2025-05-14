{{
    config(
        alias="receitas",
        schema="br_cgu_receitas_publicas",
        materialized="table",
    )
}}
select
    safe_cast(ano_exercicio as int64) ano_exercicio,
    safe_cast(parse_date("%d/%m/%Y", data_lancamento) as date) data_lancamento,
    safe_cast(codigo_orgao_superior as string) id_orgao_superior,
    safe_cast(nome_orgao_superior as string) nome_orgao_superior,
    safe_cast(codigo_orgao as string) id_orgao,
    safe_cast(nome_orgao as string) nome_orgao,
    safe_cast(codigo_unidade_gestora as string) codigo_unidade_gestora,
    safe_cast(nome_unidade_gestora as string) nome_unidade_gestora,
    safe_cast(categoria_economica as string) categoria_economica,
    safe_cast(origem_receita as string) origem,
    safe_cast(especie_receita as string) especie,
    safe_cast(detalhamento as string) detalhamento,
    safe_cast(valor_previsto_atualizado as float64) valor_previsto,
    safe_cast(valor_lancado as float64) valor_lancado,
    safe_cast(valor_realizado as float64) valor_realizado,
    safe_cast(percentual_realizado as float64) percentual_realizado,
from {{ set_datalake_project("br_cgu_receitas_publicas_staging.receitas") }} as t
