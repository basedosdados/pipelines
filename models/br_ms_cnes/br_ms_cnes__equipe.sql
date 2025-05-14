{{
    config(
        schema="br_ms_cnes",
        alias="equipe",
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
    raw_cnes_equipe as (
        -- 1. Retirar linhas com id_estabelecimento_cnes nulo
        select *
        from {{ set_datalake_project("br_ms_cnes_staging.equipe") }}
        where cnes is not null
    ),
    cnes_add_muni as (
        -- 2. Adicionar id_municipio de 7 dígitos
        select *
        from raw_cnes_equipe
        left join
            (
                select id_municipio, id_municipio_6,
                from `basedosdados.br_bd_diretorios_brasil.municipio`
            ) as mun
            on raw_cnes_equipe.codufmun = mun.id_municipio_6
    )
-- tipo_desativacao_equipe com valor 0 que não é indicado como um valor possível do
-- campo no dicionário do cnes.
-- pode ser NA. Em todos os anos tem valor significativo de zeros
-- tipo_segmento e descricao_segmento vem juntos na tabela e nao esta presente no
-- dicionario original
select
    safe_cast(ano as int64) as ano,
    safe_cast(mes as int64) as mes,
    safe_cast(sigla_uf as string) as sigla_uf,
    safe_cast(id_municipio as string) as id_municipio,
    safe_cast(cnes as string) as id_estabelecimento_cnes,
    safe_cast(id_equipe as string) as id_equipe,
    safe_cast(
        case
            when regexp_replace(tipo_eqp, '^0+', '') = ''
            then '0'
            else regexp_replace(tipo_eqp, '^0+', '')
        end as string
    ) as tipo_equipe,
    safe_cast(nome_eqp as string) as equipe,
    safe_cast(nomearea as string) as area,
    safe_cast(id_segm as string) as id_segmento,
    safe_cast(
        case
            when regexp_replace(tiposegm, '^0+', '') = ''
            then '0'
            else regexp_replace(tiposegm, '^0+', '')
        end as string
    ) as tipo_segmento,
    safe_cast(descsegm as string) as descricao_segmento,
    -- - inserir subsrt para criar ano e mes
    safe_cast(substr(dt_ativa, 1, 4) as int64) as ano_ativacao_equipe,
    safe_cast(substr(dt_ativa, 5, 6) as int64) as mes_ativacao_equipe,
    safe_cast(
        case
            when regexp_replace(motdesat, '^0+', '') = ''
            then '0'
            else regexp_replace(motdesat, '^0+', '')
        end as string
    ) as motivo_desativacao_equipe,
    safe_cast(
        case
            when regexp_replace(tp_desat, '^0+', '') = ''
            then '0'
            else regexp_replace(tp_desat, '^0+', '')
        end as string
    ) as tipo_desativacao_equipe,
    safe_cast(substr(dt_desat, 1, 4) as int64) as ano_desativacao_equipe,
    safe_cast(substr(dt_desat, 5, 6) as int64) as mes_desativacao_equipe,
    safe_cast(quilombo as int64) as indicador_atende_populacao_assistida_quilombolas,
    safe_cast(assentad as int64) as indicador_atende_populacao_assistida_assentados,
    safe_cast(popgeral as int64) as indicador_atende_populacao_assistida_geral,
    safe_cast(escola as int64) as indicador_atende_populacao_assistida_escolares,
    safe_cast(indigena as int64) as indicador_atende_populacao_assistida_indigena,
    safe_cast(pronasci as int64) as indicador_atende_populacao_assistida_pronasci,
from cnes_add_muni
{% if is_incremental() %}

    where
        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
