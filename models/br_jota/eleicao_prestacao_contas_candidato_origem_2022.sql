with
    despesa as (
        select
            sequencial_candidato,
            concat(ano, sequencial_candidato) as ano_sequencial_candidato,
            'Despesa' as receita_despesa,
            initcap(origem_despesa) as origem,
            sum(valor_despesa) as valor
        from `basedosdados.br_tse_eleicoes.despesas_candidato`
        where ano = 2022
        group by 1, 2, 3, 4
    ),

    receita as (
        select
            sequencial_candidato,
            concat(ano, sequencial_candidato) as ano_sequencial_candidato,
            'Receita' as receita_despesa,
            initcap(origem_receita) as origem,
            sum(valor_receita) as valor
        from `basedosdados.br_tse_eleicoes.receitas_candidato`
        where ano = 2022
        group by 1, 2, 3, 4
    ),

    receita_despesa as (
        select *
        from despesa
        union all
        select *
        from receita
    )

select t1.*, t2.categoria
from receita_despesa as t1
left join
    `basedosdados-perguntas.br_jota.eleicao_auxiliar_categoria_origem` as t2
    on t1.origem = t2.origem
