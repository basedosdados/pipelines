{{
    config(
        schema="br_ms_cnes",
        alias="leito",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2007, "end": 2024, "interval": 1},
        },
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) <= 6)',
        ],
    )
}}


with
    raw_cnes_leito as (
        -- 1. Retirar linhas com id_estabelecimento_cnes nulo
        select *
        from {{ set_datalake_project("br_ms_cnes_staging.leito") }}
        where cnes is not null
    ),
    cnes_leito_without_duplicates as (select distinct * from raw_cnes_leito),
    leito_x_estabelecimento as (
        -- 3. Adicionar id_municipio de 7 dígitos fazendo join com a tabela
        -- estabalecimento
        -- ps: a coluna id_municipio não vem por padrão na tabela leito extraída do
        -- FTP do Datasus
        select *
        from cnes_leito_without_duplicates as lt
        left join
            (
                select
                    id_municipio,
                    cast(ano as string) ano1,
                    cast(mes as string) mes1,
                    id_estabelecimento_cnes as iddd
                from `basedosdados.br_ms_cnes.estabelecimento`
            ) as st
            on lt.cnes = st.iddd
            and lt.ano = st.ano1
            and lt.mes = st.mes1
    )

select
    safe_cast(ano as int64) as ano,
    safe_cast(mes as int64) as mes,
    safe_cast(sigla_uf as string) as sigla_uf,
    safe_cast(id_municipio as string) as id_municipio,
    safe_cast(cnes as string) as id_estabelecimento_cnes,
    ltrim(safe_cast(codleito as string), '0') as tipo_especialidade_leito,
    ltrim(safe_cast(tp_leito as string), '0') as tipo_leito,
    safe_cast(qt_exist as int64) as quantidade_total,
    safe_cast(qt_contr as int64) as quantidade_contratado,
    safe_cast(qt_sus as int64) as quantidade_sus
from leito_x_estabelecimento
{% if is_incremental() %}
    where

        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
