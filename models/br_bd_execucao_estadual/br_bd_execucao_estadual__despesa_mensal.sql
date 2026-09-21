{{
    config(
        alias="despesa_mensal",
        schema="br_bd_execucao_estadual",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf", "mes"],
        labels={"tema": "economia"},
    )
}}

-- Execução orçamentária mensal por linha orçamentária. Uma linha por
-- (mês, dotação, unidade executora), com dotação inicial e atual, empenhado,
-- liquidado e
-- pago.
--
-- Esta tabela existe porque a fonte da Bahia publica os VALORES sem credor e sem
-- número de
-- empenho. Quem tem credor é uma view separada (`empenho_credor`), que por sua vez
-- não tem
-- valores, e a única chave entre as duas é a dotação -- com cerca de seis empenhos por
-- dotação. Atribuir valor a credor por essa chave exigiria uma regra de rateio que a
-- fonte
-- não fornece, e seria invenção. Por isso a Bahia NÃO entra em `despesa`.
--
-- Diferente de `despesa_anual` (São Paulo), que é anual e tem credor mas não tem
-- documento.
-- As três tabelas têm grãos distintos e não devem ser unidas sem cuidado.
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes_exercicio as int64) as mes,
    'BA' as sigla_uf,
    safe_cast(t.cod_orgao_orcamento as string) as orgao,
    safe_cast(t.nom_orgao_orcamento as string) as nome_orgao,
    safe_cast(t.cod_unidade_orcamentaria as string) as unidade_orcamentaria,
    safe_cast(t.nom_unidade_orcamentaria as string) as nome_unidade_orcamentaria,
    safe_cast(t.cod_unidade_gestora as string) as id_unidade_gestora,
    safe_cast(t.nom_unidade_gestora as string) as nome_unidade_gestora,
    safe_cast(t.nom_poder_orcamento as string) as poder,
    safe_cast(t.num_cta_dotacao_orcamentaria_anual as string) as dotacao_orcamentaria,
    safe_cast(t.cod_funcao_programa_governo as string) as funcao,
    safe_cast(t.cod_sub_funcao_programa_governo as string) as subfuncao,
    safe_cast(t.cod_programa_governo as string) as programa,
    safe_cast(t.cod_acao_programa_governo as string) as acao,
    safe_cast(t.cod_categoria_economica_orcamentaria as string) as categoria_economica,
    safe_cast(t.cod_grupo_despesa_orcamento as string) as grupo_despesa,
    safe_cast(t.cod_modalidade_aplicacao_orcamento as string) as modalidade_aplicacao,
    safe_cast(t.cod_elemento_despesa_orcamento as string) as elemento_despesa,
    safe_cast(t.cod_sub_elemento_despesa as string) as subelemento_despesa,
    safe_cast(t.cod_fonte_recurso as string) as fonte_recurso,
    safe_cast(t.cod_destinacao_recurso as string) as destinacao_recurso,
    safe_cast(t.cod_regiao_orcamento as string) as regiao,
    -- BA writes money in the Brazilian format ("2643000,00"). The thousands separator
    -- is
    -- absent in this view and the decimal separator is a comma, so the comma is
    -- replaced
    -- rather than stripped -- stripping it would multiply every value by 100.
    safe_cast(
        replace(t.val_orcado_inicial, ',', '.') as float64
    ) as valor_dotacao_inicial,
    safe_cast(replace(t.val_orcado_atual, ',', '.') as float64) as valor_dotacao_atual,
    safe_cast(replace(t.val_empenhado_total, ',', '.') as float64) as valor_empenhado,
    safe_cast(replace(t.val_liquidado_total, ',', '.') as float64) as valor_liquidado,
    safe_cast(replace(t.val_pago, ',', '.') as float64) as valor_pago,
    safe_cast(
        replace(t.val_descentralizacao_recebida, ',', '.') as float64
    ) as valor_descentralizacao_recebida,
    safe_cast(
        replace(t.val_descentralizacao_concedida, ',', '.') as float64
    ) as valor_descentralizacao_concedida
from {{ set_datalake_project("br_bd_execucao_estadual_staging.ba_despesa") }} as t
