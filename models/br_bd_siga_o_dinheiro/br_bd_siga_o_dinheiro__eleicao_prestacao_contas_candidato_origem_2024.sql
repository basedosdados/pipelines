{{
    config(
        alias="eleicao_prestacao_contas_candidato_origem_2024",
        schema="br_bd_siga_o_dinheiro",
        materialized="table",
    )
}}

with
    despesa as (
        select
            case
                when cargo = 'vereador' and valor_despesa > 4773280.39 then 'excluir'
            end as excluir,
            data_despesa as data_conta,
            sequencial_candidato,
            concat(ano, sequencial_candidato) as ano_sequencial_candidato,
            'Despesa' as receita_despesa,
            initcap(origem_despesa) as origem,
            sum(valor_despesa) as valor,
            count(1) as n_linhas
        from `basedosdados.br_tse_eleicoes.despesas_candidato`
        where ano = 2024
        group by 1, 2, 3, 4, 5, 6
        having excluir is null
    ),

    receita as (
        select
            case
                when cargo = 'vereador' and valor_receita > 4773280.39 then 'excluir'
            end as excluir,
            data_receita as data_conta,
            sequencial_candidato,
            concat(ano, sequencial_candidato) as ano_sequencial_candidato,
            'Receita' as receita_despesa,
            initcap(origem_receita) as origem,
            sum(valor_receita) as valor,
            count(1) as n_linhas
        from `basedosdados.br_tse_eleicoes.receitas_candidato`
        where ano = 2024
        group by 1, 2, 3, 4, 5, 6
        having excluir is null
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
    `basedosdados.br_bd_siga_o_dinheiro.eleicao_auxiliar_categoria_origem` as t2
    on t1.origem = regexp_replace(normalize(t2.origem, nfd), r"\pM", '')
where data_conta <= current_date()
order by data_conta
