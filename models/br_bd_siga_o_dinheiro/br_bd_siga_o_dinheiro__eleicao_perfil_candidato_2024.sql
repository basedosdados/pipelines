{{
    config(
        alias="eleicao_perfil_candidato_2024",
        schema="br_bd_siga_o_dinheiro",
        materialized="table",
    )
}}

select
    sequencial,
    concat(ano, sequencial) as ano_sequencial_candidato,
    nome_urna,
    sigla_partido,
    initcap(cargo) as cargo,
    concat(municipio.nome, ' - ', municipio.sigla_uf) as municipio,
    candidatos.sigla_uf,
    initcap(genero) as genero,
    case when raca is null then 'Nao Informado' else initcap(raca) end as raca,
    idade,
    case
        when safe_cast(idade as int64) < 29
        then "18-29"
        when safe_cast(idade as int64) < 50
        then "30-49"
        when safe_cast(idade as int64) < 70
        then "50-69"
        when safe_cast(idade as int64) >= 70
        then "70-100"
    end as faixa_etaria
from `basedosdados.br_tse_eleicoes.candidatos` as candidatos
left join
    `basedosdados.br_bd_diretorios_brasil.municipio` as municipio
    on candidatos.id_municipio = municipio.id_municipio
where ano = 2024
