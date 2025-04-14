{{
    config(
        alias="eleicao_prestacao_contas_candidato_2024",
        schema="br_bd_siga_o_dinheiro",
        materialized="table",
    )
}}

with
    soma_receitas_candidato as (
        select
            sequencial_candidato,
            sum(
                case when origem = 'Recursos De Partido Politico' then valor end
            ) as receita_partido,
            sum(
                case when receita_despesa = 'Receita' then valor end
            ) as valor_receita_total,
            sum(
                case when receita_despesa = 'Despesa' then valor end
            ) as valor_despesa_total,
            sum(
                case when categoria = 'Pessoal' then valor end
            ) as valor_despesa_pessoal,
            sum(
                case when categoria = 'Publicidade' then valor end
            ) as valor_despesa_publicidade,
            sum(case when categoria = 'Outros' then valor end) as valor_despesa_outros,
            sum(
                case when categoria = 'Operacoes' then valor end
            ) as valor_despesa_operacoes,
        from
            `basedosdados.br_bd_siga_o_dinheiro.eleicao_prestacao_contas_candidato_origem_2024`
        where sequencial_candidato is not null
        group by 1
    )

select
    candidato_info.*,
    rank() over (partition by cargo order by valor_receita_total desc) as rank_cargo,
    rank() over (
        partition by sigla_partido order by valor_receita_total desc
    ) as rank_partido,
    rank() over (
        partition by cargo, municipio order by valor_receita_total desc
    ) as rank_cargo_municipio,
    rank() over (
        partition by sigla_partido, cargo order by valor_receita_total desc
    ) as rank_cargo_partido,
    valor_despesa_pessoal / valor_despesa_total as proporcao_despesa_pessoal,
    valor_despesa_publicidade / valor_despesa_total as proporcao_despesa_publicidade,
    valor_despesa_outros / valor_despesa_total as proporcao_despesa_outros,
    valor_despesa_operacoes / valor_despesa_total as proporcao_despesa_operacoes,
    valores.* except (sequencial_candidato)
from
    `basedosdados.br_bd_siga_o_dinheiro.eleicao_perfil_candidato_2024` as candidato_info
left join
    soma_receitas_candidato as valores
    on candidato_info.sequencial = valores.sequencial_candidato
