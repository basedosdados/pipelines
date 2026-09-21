{{
    config(
        alias="despesa_dotacao",
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

-- Execução orçamentária da despesa por dotação (linha orçamentária), em posição anual.
--
-- Diferente de `despesa` (que é o documento de empenho, com credor e data) e de
-- `despesa_mensal` (movimento mensal por dotação), esta tabela é a POSIÇÃO acumulada da
-- execução ao fim de cada exercício, por linha orçamentária completa -- não um movimento,
-- mas o total alcançado. Uma linha por (exercício, dotação). Cada arquivo do Rio de
-- Janeiro traz exatamente uma `Posição` (12/AAAA nos exercícios encerrados; o mês mais
-- recente no exercício aberto), preservada em `mes`.
--
-- Existe só para o Rio de Janeiro, único estado que publica a execução nesse grão
-- (dadosabertos.rj.gov.br, pacote tfe-despesa). Sem credor, sem empenho, sem data abaixo
-- do mês: é o retrato da dotação, com dotação inicial e atual, empenhado, liquidado, pago,
-- restos a pagar e o desembolso total. Valores em reais correntes, com vírgula decimal na
-- origem.
-- The CTE is deliberately NOT named `fonte`: rj_despesa has a column called `fonte`,
-- and a CTE of the same name shadows it, so `trim(fonte)` would resolve to the whole
-- row STRUCT and fail (No matching signature for TRIM).
with
    origem as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.rj_despesa") }}
    )
select
    safe_cast(split(posicao, '/')[safe_offset(1)] as int64) as ano,
    safe_cast(split(posicao, '/')[safe_offset(0)] as int64) as mes,
    'RJ' as sigla_uf,
    nullif(trim(ug), '') as id_unidade_gestora,
    nullif(trim(nome_ug), '') as nome_unidade_gestora,
    nullif(trim(codigo_uo), '') as unidade_orcamentaria,
    nullif(trim(nome_uo), '') as nome_unidade_orcamentaria,
    nullif(trim(orgao), '') as orgao,
    nullif(trim(nome_orgao), '') as nome_orgao,
    nullif(trim(poder), '') as poder,
    nullif(trim(nome_poder), '') as nome_poder,
    nullif(trim(tipo_de_administracao), '') as tipo_administracao,
    nullif(trim(funcao), '') as funcao,
    nullif(trim(nome_funcao), '') as nome_funcao,
    nullif(trim(sub_funcao), '') as subfuncao,
    nullif(trim(nome_sub_funcao), '') as nome_subfuncao,
    nullif(trim(programa), '') as programa,
    nullif(trim(nome_programa), '') as nome_programa,
    nullif(trim(projeto_atividade), '') as acao,
    nullif(trim(nome_projeto_atividade), '') as nome_acao,
    nullif(trim(categoria_economica), '') as categoria_economica,
    nullif(trim(nome_categoria_economica), '') as nome_categoria_economica,
    nullif(trim(grupo), '') as grupo_despesa,
    nullif(trim(nome_grupo), '') as nome_grupo_despesa,
    nullif(trim(modalidade_aplicacao), '') as modalidade_aplicacao,
    nullif(trim(nome_modalidade_aplicacao), '') as nome_modalidade_aplicacao,
    nullif(trim(elemento_de_despesa), '') as elemento_despesa,
    nullif(trim(nome_elemento_de_despesa), '') as nome_elemento_despesa,
    nullif(trim(sub_elemento), '') as subelemento_despesa,
    nullif(trim(nome_sub_elemento), '') as nome_subelemento_despesa,
    nullif(trim(fonte), '') as fonte_recurso,
    nullif(trim(nome_fonte), '') as nome_fonte_recurso,
    nullif(trim(gestao), '') as gestao,
    nullif(trim(nome_gestao), '') as nome_gestao,
    -- Reais correntes, vírgula decimal na origem (às vezes sem casas): remove o ponto de
    -- milhar e troca a vírgula por ponto.
    safe_cast(replace(replace(valor_dotacao_inicial, '.', ''), ',', '.') as float64) as valor_dotacao_inicial,
    safe_cast(replace(replace(valor_dotado_atual, '.', ''), ',', '.') as float64) as valor_dotacao_atual,
    safe_cast(replace(replace(valor_despesa_autorizada, '.', ''), ',', '.') as float64) as valor_despesa_autorizada,
    safe_cast(replace(replace(valor_empenhado, '.', ''), ',', '.') as float64) as valor_empenhado,
    safe_cast(replace(replace(valor_liquidado, '.', ''), ',', '.') as float64) as valor_liquidado,
    safe_cast(replace(replace(valor_pago, '.', ''), ',', '.') as float64) as valor_pago,
    safe_cast(replace(replace(valor_rp_a_pagar, '.', ''), ',', '.') as float64) as valor_restos_a_pagar,
    safe_cast(replace(replace(valor_rp_pago, '.', ''), ',', '.') as float64) as valor_restos_a_pagar_pago,
    safe_cast(replace(replace(valor_total_desembolsado, '.', ''), ',', '.') as float64) as valor_total_desembolsado
from origem
