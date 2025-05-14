with
    soma_receitas_candidato as (
        select
            sequencial_candidato,
            sum(
                case when origem = 'Recursos De Partido Politico' then valor end
            ) as receita_partido,
            sum(valor) as valor_total
        from
            `basedosdados-perguntas.br_jota.eleicao_prestacao_contas_candidato_origem_2022`
        where sequencial_candidato is not null and receita_despesa = 'Receita'
        group by 1
    )

select
    candidato_info.*,
    rank() over (partition by cargo order by valor_total desc) as rank_cargo,
    rank() over (partition by sigla_partido order by valor_total desc) as rank_partido,
    rank() over (
        partition by sigla_partido, cargo order by valor_total desc
    ) as rank_cargo_partido,
    valores.* except (sequencial_candidato)
from `basedosdados-perguntas.br_jota.eleicao_perfil_candidato_2022` as candidato_info
left join
    soma_receitas_candidato as valores
    on candidato_info.sequencial = valores.sequencial_candidato
