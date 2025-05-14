{{
    config(
        schema="br_ms_cnes",
        alias="profissional",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2005, "end": 2025, "interval": 1},
        },
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    )
}}
with
    raw_cnes_profissional as (
        -- 1. Retirar linhas com id_estabelecimento_cnes nulo
        select *
        from {{ set_datalake_project("br_ms_cnes_staging.profissional") }}
        where cnes is not null and competen is not null
    ),
    profissional_x_estabelecimento as (
        select *
        from raw_cnes_profissional as pf
        left join
            (
                select
                    id_municipio,
                    cast(ano as string) as ano1,
                    cast(mes as string) as mes1,
                    id_estabelecimento_cnes as iddd
                from `basedosdados.br_ms_cnes.estabelecimento`
            ) as st
            on pf.cnes = st.iddd
            and pf.ano = st.ano1
            and pf.mes = st.mes1
    )
select
    cast(substr(competen, 1, 4) as int64) as ano,
    cast(substr(competen, 5, 2) as int64) as mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(cnes as string) id_estabelecimento_cnes,
    -- replace de valores de linha com 6 zeros para null. 6 zeros é valor do campo
    -- UFMUNRES que indica null
    safe_cast(regexp_replace(ufmunres, '0{6}', '') as string) id_municipio_6_residencia,
    safe_cast(nomeprof as string) nome,
    case
        when vinculac = 'nan' then null else safe_cast(vinculac as string)
    end as tipo_vinculo,
    case
        when registro = 'nan' then null else safe_cast(registro as string)
    end as id_registro_conselho,
    case
        when conselho = 'nan' then null else safe_cast(conselho as string)
    end as tipo_conselho,
    -- replace de valores de linha com 15 zeros para null. 15 zeros é valor do campo
    -- CNS_PROF que indica null
    safe_cast(cns_prof as string) cartao_nacional_saude,
    safe_cast(cbo as string) cbo_2002_original,
    safe_cast(
        case
            when length(cbo) = 6 and regexp_contains(cbo, r'^[0-9]{6}$')
            then cbo
            else null
        end as string
    ) cbo_2002,
    safe_cast(
        case
            when length(cbo) = 5 and regexp_contains(cbo, r'^[0-9]{5}$')
            then cbo
            else null
        end as string
    ) cbo_1994,
    safe_cast(terceiro as int64) indicador_estabelecimento_terceiro,
    safe_cast(vincul_c as int64) indicador_vinculo_contratado_sus,
    safe_cast(vincul_a as int64) indicador_vinculo_autonomo_sus,
    safe_cast(vincul_n as int64) indicador_vinculo_outros,
    safe_cast(prof_sus as int64) indicador_atende_sus,
    safe_cast(profnsus as int64) indicador_atende_nao_sus,
    safe_cast(horaoutr as int64) carga_horaria_outros,
    safe_cast(horahosp as int64) carga_horaria_hospitalar,
    safe_cast(hora_amb as int64) carga_horaria_ambulatorial
from profissional_x_estabelecimento
{% if is_incremental() %}
    where

        safe.date(cast(ano as int64), cast(mes as int64), 1) > (
            select max(safe.date(cast(ano as int64), cast(mes as int64), 1))
            from {{ this }}
        )
{% endif %}
