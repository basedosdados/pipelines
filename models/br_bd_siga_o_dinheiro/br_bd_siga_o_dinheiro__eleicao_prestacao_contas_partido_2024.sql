{{
    config(
        alias="eleicao_prestacao_contas_partido_2024",
        schema="br_bd_siga_o_dinheiro",
        materialized="table",
    )
}}

select
    municipio,
    sigla_partido,
    cargo,
    genero,
    raca,
    faixa_etaria,
    sum(valor_receita_total) as valor_receita_total,
    sum(valor_despesa_total) as valor_despesa_total,
    sum(valor_despesa_pessoal) as valor_despesa_pessoal,
    sum(valor_despesa_publicidade) as valor_despesa_publicidade,
    sum(valor_despesa_outros) as valor_despesa_outros,
    sum(valor_despesa_operacoes) as valor_despesa_operacoes,
from `basedosdados.br_bd_siga_o_dinheiro.eleicao_prestacao_contas_candidato_2024`
group by 1, 2, 3, 4, 5, 6
