{{
    config(
        schema="br_ms_cnes",
        alias="gestao_metas",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2005, "end": 2024, "interval": 1},
        },
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) <= 6)',
        ],
    )
}}
with
    raw_cnes_gestao_metas as (
        -- 1. Retirar linhas com id_estabelecimento_cnes nulo
        select *
        from {{ set_datalake_project("br_ms_cnes_staging.gestao_metas") }}
        where cnes is not null
    ),
    raw_cnes_gestao_metas_without_duplicates as (
        -- 2. distinct nas linhas
        select distinct * from raw_cnes_gestao_metas
    ),
    cnes_add_muni as (
        -- 3. Adicionar id_municipio e sigla_uf
        select *
        from raw_cnes_gestao_metas_without_duplicates
        left join
            (
                select id_municipio, id_municipio_6,
                from `basedosdados.br_bd_diretorios_brasil.municipio`
            ) as mun
            on raw_cnes_gestao_metas_without_duplicates.codufmun = mun.id_municipio_6
    )

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(cnes as string) id_estabelecimento_cnes,
    cast(substr(cmpt_ini, 1, 4) as int64) as ano_competencia_inicial,
    cast(substr(cmpt_ini, 5, 2) as int64) as mes_competencia_inicial,
    cast(substr(cmpt_fim, 1, 4) as int64) as ano_competencia_final,
    cast(substr(cmpt_fim, 5, 2) as int64) as mes_competencia_final,
    ltrim(safe_cast(sgruphab as string), '0') tipo_habilitacao,
    case
        when safe_cast(sgruphab as string) in ("7003", "7004", "7005", "7006")
        then '1'
        else '2'
    end as tipo_gestao_metas,
    safe_cast(portaria as string) portaria,
    cast(
        concat(
            substring(dtportar, -4),
            '-',
            substring(dtportar, -7, 2),
            '-',
            substring(dtportar, 1, 2)
        ) as date
    ) data_portaria,
    cast(substr(maportar, 1, 4) as int64) as ano_portaria,
    cast(substr(maportar, 5, 2) as int64) as mes_portaria,
from cnes_add_muni as t
{% if is_incremental() %}
    where

        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
