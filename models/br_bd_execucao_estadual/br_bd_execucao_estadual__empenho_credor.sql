{{
    config(
        alias="empenho_credor",
        schema="br_bd_execucao_estadual",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2019, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf"],
        labels={"tema": "economia"},
    )
}}

-- Empenhos e seus credores, SEM valores.
--
-- Esta tabela existe por uma limitação da fonte, não por escolha. A Bahia publica os
-- valores da despesa em uma view (`despesa_mensal`, por mês e dotação, sem credor) e os
-- credores em outra (esta, por empenho, sem valores). A única chave entre as duas é a
-- dotação, e há cerca de seis empenhos por dotação -- atribuir valor a credor por essa
-- chave exigiria um rateio que a fonte não fornece.
--
-- O que esta tabela permite, e é bastante: saber QUEM o estado empenhou, quando, sob
-- qual
-- instrumento orçamentário e em qual processo administrativo. O que ela não permite é
-- dizer QUANTO foi empenhado para cada credor. Não junte com `despesa_mensal` pela
-- dotação esperando obter isso.
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes_pedido as int64) as mes,
    'BA' as sigla_uf,
    safe_cast(t.num_empenho_orcamento as string) as numero_empenho,
    safe_cast(t.num_instrumento_orcamento as string) as instrumento_orcamentario,
    safe_cast(t.cod_tipo_empenho as string) as tipo_empenho,
    safe_cast(t.cod_tipo_despesa as string) as tipo_despesa,
    safe_cast(t.cod_credor_despesa as string) as id_credor,
    safe_cast(t.nom_razao_social as string) as nome_credor,
    safe_cast(t.cnpj_cpf as string) as documento_credor_formatado,
    safe_cast(regexp_replace(t.cnpj_cpf, r'[^0-9]', '') as string) as documento_credor,
    -- BA writes "12/05/2025 00:00:00"; only the date half carries information on the
    -- request, while the authorisation timestamp is genuinely timed.
    safe.parse_date('%d/%m/%Y', left(t.dtc_pedido, 10)) as data_pedido,
    safe.parse_datetime('%d/%m/%Y %H:%M:%S', t.dtc_autorizacao) as data_autorizacao,
    safe_cast(t.num_processo_sist_elet_info as string) as processo_sei,
    safe_cast(t.fk_mes_ano_dotacao as string) as dotacao_orcamentaria
from {{ set_datalake_project("br_bd_execucao_estadual_staging.ba_empenho_sei") }} as t
