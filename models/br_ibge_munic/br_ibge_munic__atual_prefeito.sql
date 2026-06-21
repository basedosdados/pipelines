{{ config(alias="atual_prefeito", schema="br_ibge_munic", materialized="table") }}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(nome as string) nome,
    safe_cast(exercendo_mandato as string) exercendo_mandato,
    safe_cast(sexo as string) sexo,
    safe_cast(idade as int64) idade,
    safe_cast(escolaridade as string) escolaridade,
    safe_cast(cor_raca as string) cor_raca,
    safe_cast(
        cor_raca_respondido_pelo_proprio_prefeito as string
    ) cor_raca_respondido_pelo_proprio_prefeito,
    safe_cast(partido_eleito as string) partido_eleito,
    safe_cast(partido_atual as string) partido_atual
from {{ set_datalake_project("br_ibge_munic_staging.atual_prefeito") }} as t
