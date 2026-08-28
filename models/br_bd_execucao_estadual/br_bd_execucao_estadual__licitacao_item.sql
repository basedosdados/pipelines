{{
    config(
        alias="licitacao_item",
        schema="br_bd_execucao_estadual",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2009, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf"],
        labels={"tema": "economia"},
    )
}}

-- Itens licitados pelos governos estaduais. Uma linha por (processo, item).
--
-- Carrega o preço de referência estimado e o preço homologado, no nível unitário e
-- total,
-- o que torna `valor_unitario` comparável ao campo homônimo de MiDES.
select *
from {{ ref("br_bd_execucao_estadual__licitacao_item_mg") }}
