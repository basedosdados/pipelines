{{ config(alias="dicionario", schema="br_rf_cnpj", materialized="table") }}

with
    tmp_dict as (
        select
            safe_cast(id_tabela as string) id_tabela,
            safe_cast(nome_coluna as string) nome_coluna,
            safe_cast(chave as string) chave,
            safe_cast(cobertura_temporal as string) cobertura_temporal,
            regexp_replace(
                {{ validate_null_cols("valor") }}, r'(^0+)(?:[^0]+|0{1})', ''
            ) as valor,
        from {{ set_datalake_project("br_rf_cnpj_staging.dicionario") }} as t
    )
select
    id_tabela,
    nome_coluna,
    chave,
    cobertura_temporal,
    case when nome_coluna = "id_pais" then valor else initcap(valor) end as valor
from tmp_dict
where valor is not null
union all
{{
    dicionario_not_found(
        id_tabela="empresas",
        nome_coluna="qualificacao_responsavel",
        chave="36",
    )
}}
union all
{{
    dicionario_not_found(
        id_tabela="socios",
        nome_coluna="id_pais",
        chave=["994", "393"],
    )
}}
union all
{{
    dicionario_not_found(
        id_tabela="estabelecimentos",
        nome_coluna="id_pais",
        chave=["8", "9", "393"],
    )
}}
union all
{{
    dicionario_not_found(
        id_tabela="estabelecimentos",
        nome_coluna="motivo_situacao_cadastral",
        chave="32",
    )
}}
union all
{{
    dicionario_not_found(
        id_tabela="estabelecimentos",
        nome_coluna="",
        chave=["6202100", "4761000"],
    )
}}
