{{ config(alias="dicionario", schema="br_ibge_pnadc", materialized="table") }}

{% set v3_em_microdados = [
    "V3001",
    "V3002",
    "V3002A",
    "V3003",
    "V3003A",
    "V3004",
    "V3005",
    "V3005A",
    "V3006",
    "V3006A",
    "V3007",
    "V3008",
    "V3009",
    "V3009A",
    "V3010",
    "V3011",
    "V3011A",
    "V3012",
    "V3013",
    "V3013A",
    "V3013B",
    "V3014",
] %}

{% set colunas_strip_zero = [
    "V2005",
    "V4072",
    "V4074A",
    "V3003",
    "V3003A",
    "V3006",
    "V3009",
    "V3009A",
    "V3013",
] %}

with
    base as (
        select id_tabela, nome_coluna, chave, cobertura_temporal, valor
        from {{ set_datalake_project("br_ibge_pnadc_staging.dicionario") }} as t
    ),
    v3_microdados as (
        select 'microdados' as id_tabela, nome_coluna, chave, cobertura_temporal, valor
        from base
        where
            id_tabela = 'educacao'
            and nome_coluna in ('{{ v3_em_microdados | join("', '") }}')
    ),

    unificado as (
        select *
        from base
        union all
        select *
        from v3_microdados
    )

select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(
        case
            when nome_coluna in ('{{ colunas_strip_zero | join("', '") }}')
            then ltrim(chave, '0')
            else chave
        end as string
    ) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor
from unificado
