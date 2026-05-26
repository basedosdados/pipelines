{{ config(alias="dicionario", schema="br_me_cnpj", materialized="table") }}

with
    tmp_dict as (
        select
            safe_cast(id_tabela as string) id_tabela,
            safe_cast(nome_coluna as string) nome_coluna,
            safe_cast(chave as string) chave,
            safe_cast(cobertura_temporal as string) cobertura_temporal,
            {{ validate_null_cols("valor") }} as valor,
        from {{ set_datalake_project("br_me_cnpj_staging.dicionario") }} as t
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
        id_tabela="empresas", nome_coluna="qualificacao_responsavel", chave="36"
    )
}}
union all
{{
    dicionario_not_found(
        id_tabela="socios",
        nome_coluna="id_pais",
        chave=["994", "15", "393"],
    )
}}
union all
{{
    dicionario_not_found(
        id_tabela="estabelecimentos",
        nome_coluna="id_pais",
        chave=["009", "008", "393"],
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
        nome_coluna="cnae_fiscal_secundaria",
        chave=["6202100", "4761000", "393"],
    )
}}
