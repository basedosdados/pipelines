{{
    config(
        alias="licitacao_participante",
        schema="br_bd_execucao_estadual",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2004, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf"],
        labels={"tema": "economia"},
    )
}}

-- Participantes de licitações estaduais: uma linha por (item, fornecedor), com o valor
-- cotado, o valor homologado e a situação do fornecedor naquele item.
--
-- Só a Bahia publica os perdedores. Minas Gerais divulga apenas o vencedor de cada
-- item,
-- então MG aparece em `licitacao_item` e não aqui. Essa é a diferença que permite, na
-- Bahia, medir concorrência: quantos fornecedores disputaram, quem foi
-- desclassificado e
-- qual a distância entre a proposta vencedora e as demais.
select *
from {{ ref("br_bd_execucao_estadual__licitacao_participante_ba") }}
