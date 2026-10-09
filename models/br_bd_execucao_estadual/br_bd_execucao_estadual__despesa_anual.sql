{{
    config(
        alias="despesa_anual",
        schema="br_bd_execucao_estadual",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2010, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf"],
        labels={"tema": "economia"},
    )
}}

-- Execução orçamentária anual por credor e linha orçamentária, para estados cuja fonte
-- só publica agregados anuais. Uma linha por (exercício, órgão, unidade gestora, fonte
-- de recursos, credor, natureza da despesa), com empenhado, liquidado, pago e pago de
-- restos a pagar.
--
-- Existe porque São Paulo não publica número de documento nem data infra-anual: o SIGEO
-- agrega ao exercício. Forçar SP em `despesa` deixaria toda linha paulista nula
-- exatamente nas colunas que tornam aquela tabela útil, parecendo dado transacional
-- para
-- quem filtrasse por sigla_uf.
--
-- As três tabelas de execução têm grãos distintos e não devem ser unidas sem cuidado:
-- `despesa` é por documento de empenho, `despesa_mensal` é por mês e dotação sem
-- credor,
-- e esta é anual com credor.
select *
from {{ ref("br_bd_execucao_estadual__despesa_anual_sp") }}
