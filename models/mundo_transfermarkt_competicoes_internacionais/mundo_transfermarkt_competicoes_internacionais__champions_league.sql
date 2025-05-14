{{
    config(
        alias="champions_league",
        schema="mundo_transfermarkt_competicoes_internacionais",
        materialized="table",
        partition_by={
            "field": "temporada",
            "data_type": "string",
        },
        labels={"tema": "esporte"},
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"), DATE(data), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"), DATE(data), MONTH) <= 6)',
        ],
    )
}}

select
    safe_cast(temporada as string) temporada,
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
    safe_cast(idade_tecnico_man as int64) idade_tecnico_mandante,
    safe_cast(idade_tecnico_vis as int64) idade_tecnico_visitante,
    safe_cast(data_inicio_tecnico_man as date) data_inicio_tecnico_mandante,
    safe_cast(data_inicio_tecnico_vis as date) data_inicio_tecnico_visitante,
    safe_cast(data_final_tecnico_man as date) data_final_tecnico_mandante,
    safe_cast(data_final_tecnico_vis as date) data_final_tecnico_visitante,
    safe_cast(
        replace (proporcao_sucesso_man, ",", ".") as float64
    ) proporcao_sucesso_mandante,
    safe_cast(
        replace (proporcao_sucesso_vis, ",", ".") as float64
    ) proporcao_sucesso_visitante,
    safe_cast(
        replace (valor_equipe_titular_man, ".0", "") as int64
    ) valor_equipe_titular_mandante,
    safe_cast(
        replace (valor_equipe_titular_vis, ".0", "") as int64
    ) valor_equipe_titular_visitante,
    safe_cast(
        replace (valor_medio_equipe_titular_man, ".0", "") as int64
    ) valor_medio_equipe_titular_mandante,
    safe_cast(
        replace (valor_medio_equipe_titular_vis, ".0", "") as int64
    ) valor_medio_equipe_titular_visitante,
    safe_cast(
        convocacao_selecao_principal_man as int64
    ) convocacao_selecao_principal_mandante,
    safe_cast(
        convocacao_selecao_principal_vis as int64
    ) convocacao_selecao_principal_visitante,
    safe_cast(selecao_juniores_man as int64) selecao_juniores_mandante,
    safe_cast(selecao_juniores_vis as int64) selecao_juniores_visitante,
    safe_cast(estrangeiros_man as int64) estrangeiros_mandante,
    safe_cast(estrangeiros_vis as int64) estrangeiros_visitante,
    safe_cast(replace (socios_man, ".", "") as int64) socios_mandante,
    safe_cast(replace (socios_vis, ".", "") as int64) socios_visitante,
    safe_cast(
        replace (idade_media_titular_man, ",", ".") as float64
    ) idade_media_titular_mandante,
    safe_cast(
        replace (idade_media_titular_vis, ",", ".") as float64
    ) idade_media_titular_visitante,
    safe_cast(replace (gols_man, ".0", "") as int64) gols_mandante,
    safe_cast(replace (gols_vis, ".0", "") as int64) gols_visitante,
    safe_cast(replace (prorrogacao, ".0", "") as int64) prorrogacao,
    safe_cast(replace (penalti, ".0", "") as int64) penalti,
    safe_cast(replace (gols_1_tempo_man, ".0", "") as int64) gols_1_tempo_mandante,
    safe_cast(replace (gols_1_tempo_vis, ".0", "") as int64) gols_1_tempo_visitante,
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
    {{
        set_datalake_project(
            "mundo_transfermarkt_competicoes_internacionais_staging.champions_league"
        )
    }} as t
