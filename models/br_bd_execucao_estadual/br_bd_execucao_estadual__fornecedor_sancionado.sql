{{
    config(
        alias="fornecedor_sancionado",
        schema="br_bd_execucao_estadual",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1990, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf"],
        labels={"tema": "economia"},
    )
}}

-- Fornecedores sancionados pelos governos estaduais: quem foi impedido de contratar ou
-- penalizado, por quê, por qual órgão e em que período. Uma linha por sanção.
--
-- É a margem de fiscalização, que nenhuma outra tabela do conjunto observa. Permite a
-- pergunta direta: ser sancionado de fato afasta a empresa de contratos posteriores?
-- Ligue `documento_fornecedor` a `contrato.documento_contratado`,
-- `licitacao_participante.documento` ou `nota_fiscal.documento_emissor`.
--
-- Reúne dois registros distintos, marcados por `origem`: o CAFIMP, cadastro de
-- fornecedores impedidos, com 2.087 registros desde 2006; e os processos
-- administrativos
-- sancionadores da CGE, com 54. A diferença de tamanho é da fonte. Não leia os 54
-- como o
-- total de sanções do estado, e não some os dois como se fossem um cadastro só: uma
-- empresa pode aparecer nos dois pelo mesmo episódio.
--
-- Existe só para Minas Gerais.
select *
from {{ ref("br_bd_execucao_estadual__fornecedor_sancionado_mg") }}
