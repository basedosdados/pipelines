select
    sequencial,
    concat(ano, sequencial) as ano_sequencial_candidato,
    nome_urna,
    sigla_partido,
    initcap(cargo) as cargo,
    sigla_uf,
    initcap(genero) as genero,
    initcap(raca) as raca,
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
from `basedosdados.br_tse_eleicoes.candidatos`
where ano = 2022
