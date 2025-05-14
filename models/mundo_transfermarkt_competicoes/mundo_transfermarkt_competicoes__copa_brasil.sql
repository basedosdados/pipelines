{{
    config(
        alias="copa_brasil",
        schema="mundo_transfermarkt_competicoes",
        materialized="table",
        partition_by={
            "field": "ano_campeonato",
            "data_type": "int64",
            "range": {"start": 2020, "end": 2022, "interval": 1},
        },
        labels={"tema": "esporte"},
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"), DATE(data), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"), DATE(data), MONTH) <= 6)',
        ],
    )
}}
select
    safe_cast(replace (ano_campeonato, ".0", "") as int64) ano_campeonato,
    safe_cast(data as date) data,
    safe_cast(concat(horario, ":00") as time) horario,
    initcap(fase) fase,
    initcap(tipo_fase) tipo_fase,
    safe_cast(estadio as string) estadio,
    safe_cast(arbitro as string) arbitro,
    safe_cast(replace (publico, ".0", "") as int64) publico,
    safe_cast(replace (publico_max, ".0", "") as int64) publico_max,
    safe_cast(time_man as string) time_mandante,
    safe_cast(time_vis as string) time_visitante,
    safe_cast(tecnico_man as string) tecnico_mandante,
    safe_cast(tecnico_vis as string) tecnico_visitante,
    safe_cast(
        replace (valor_equipe_titular_man, ".0", "") as int64
    ) valor_equipe_titular_mandante,
    safe_cast(
        replace (valor_equipe_titular_vis, ".0", "") as int64
    ) valor_equipe_titular_visitante,
    safe_cast(idade_media_titular_man as float64) idade_media_titular_mandante,
    safe_cast(idade_media_titular_vis as float64) idade_media_titular_visitante,
    safe_cast(replace (gols_man, ".0", "") as int64) gols_mandante,
    safe_cast(replace (gols_vis, ".0", "") as int64) gols_visitante,
    safe_cast(replace (gols_1_tempo_man, ".0", "") as int64) gols_1_tempo_mandante,
    safe_cast(replace (gols_1_tempo_vis, ".0", "") as int64) gols_1_tempo_visitante,
    safe_cast(replace (penalti, ".0", "") as int64) penalti,
    safe_cast(replace (gols_penalti_man, ".0", "") as int64) gols_penalti_mandante,
    safe_cast(replace (gols_penalti_vis, ".0", "") as int64) gols_penalti_visitante,
    safe_cast(replace (escanteios_man, ".0", "") as int64) escanteios_mandante,
    safe_cast(replace (escanteios_vis, ".0", "") as int64) escanteios_visitante,
    safe_cast(replace (faltas_man, ".0", "") as int64) faltas_mandante,
    safe_cast(replace (faltas_vis, ".0", "") as int64) faltas_visitante,
    safe_cast(
        replace (chutes_bola_parada_man, ".0", "") as int64
    ) chutes_bola_parada_mandante,
    safe_cast(
        replace (chutes_bola_parada_vis, ".0", "") as int64
    ) chutes_bola_parada_visitante,
    safe_cast(replace (defesas_man, ".0", "") as int64) defesas_mandante,
    safe_cast(replace (defesas_vis, ".0", "") as int64) defesas_visitante,
    safe_cast(replace (impedimentos_man, ".0", "") as int64) impedimentos_mandante,
    safe_cast(replace (impedimentos_vis, ".0", "") as int64) impedimentos_visitante,
    safe_cast(replace (chutes_man, ".0", "") as int64) chutes_mandante,
    safe_cast(replace (chutes_vis, ".0", "") as int64) chutes_visitante,
    safe_cast(replace (chutes_fora_man, ".0", "") as int64) chutes_fora_mandante,
    safe_cast(replace (chutes_fora_vis, ".0", "") as int64) chutes_fora_visitante
from
    {{ set_datalake_project("mundo_transfermarkt_competicoes_staging.copa_brasil") }}
    as t
