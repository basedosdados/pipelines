{{
    config(
        schema="br_ms_cnes",
        alias="habilitacao",
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
    raw_cnes_habilitacaol as (
        -- 1. Retirar linhas com id_estabelecimento_cnes nulo
        select *
        from {{ set_datalake_project("br_ms_cnes_staging.habilitacao") }}
        where cnes is not null
    ),
    raw_cnes_habilitacao_without_duplicates as (
        -- 2. distinct nas linhas
        select distinct * from raw_cnes_habilitacaol
    ),
    cnes_add_muni as (
        -- 3. Adicionar id_municipio e sigla_uf
        select *
        from raw_cnes_habilitacao_without_duplicates
        left join
            (
                select id_municipio, id_municipio_6,
                from `basedosdados.br_bd_diretorios_brasil.municipio`
            ) as mun
            on raw_cnes_habilitacao_without_duplicates.codufmun = mun.id_municipio_6
    )

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(cnes as string) id_estabelecimento_cnes,
    safe_cast(nuleitos as int64) quantidade_leitos,
    safe_cast(substr(cmpt_ini, 1, 4) as int64) as ano_competencia_inicial,
    safe_cast(substr(cmpt_ini, 5, 2) as int64) as mes_competencia_inicial,
    safe_cast(substr(cmpt_fim, 1, 4) as int64) as ano_competencia_final,
    safe_cast(substr(cmpt_fim, 5, 2) as int64) as mes_competencia_final,
    ltrim(safe_cast(sgruphab as string), '0') as tipo_habilitacao,
    case
        when
            safe_cast(sgruphab as string) in (
                "0901",
                "0902",
                "0903",
                "0904",
                "0905",
                "0906",
                "0907",
                "1901",
                "1902",
                "2901",
                "3304"
            )
        then '2'
        else '1'
    end as nivel_habilitacao,
    safe_cast(portaria as string) portaria,
    safe_cast(
        concat(
            substring(dtportar, -4),
            '-',
            substring(dtportar, -7, 2),
            '-',
            substring(dtportar, 1, 2)
        ) as date
    ) data_portaria,

    safe_cast(substr(maportar, 1, 4) as int64) as ano_portaria,
    safe_cast(substr(maportar, 5, 2) as int64) as mes_portaria,
from cnes_add_muni as t
{% if is_incremental() %}
    where
        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
