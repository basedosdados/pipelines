{{ config(alias="taxa_letalidade", schema="br_rj_isp_estatisticas_seguranca") }}

select
    safe_cast(ano as int64) ano,
    safe_cast(regiao as int64) regiao,
    safe_cast(delito as string) delito,
    safe_cast(contagem_delito as int64) contagem_delito,
    safe_cast(populacao as int64) populacao,
    safe_cast(taxa_cem_mil_habitantes as int64) taxa_cem_mil_habitantes
from
    `basedosdados-staging.br_rj_isp_estatisticas_seguranca_staging.taxa_letalidade `
    as t
