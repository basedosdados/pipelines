{{
    config(
        alias="contrato_item",
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

-- Itens contratados pelos governos estaduais: o que cada contrato comprou, linha a
-- linha,
-- com produto catalogado, quantidade e preço. Uma linha por (contrato, produto,
-- ocorrência) -- o mesmo produto pode aparecer mais de uma vez no mesmo contrato, com
-- quantidade ou preço diferentes, e essas são linhas legítimas.
--
-- Complementa `contrato` (o instrumento) e `licitacao_item` (o que foi licitado):
-- ligue a
-- `contrato` por `id_contrato_bd` e a `licitacao_item` por `codigo_catalogo`, que é o
-- mesmo catálogo de material/serviço nas duas pontas.
--
-- Existe só para Minas Gerais. Os outros estados publicam itens no processo
-- licitatório,
-- não no contrato.
select *
from {{ ref("br_bd_execucao_estadual__contrato_item_mg") }}
