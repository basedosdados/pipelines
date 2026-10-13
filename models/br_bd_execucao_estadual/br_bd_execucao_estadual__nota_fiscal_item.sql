{{
    config(
        alias="nota_fiscal_item",
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

-- Itens das notas fiscais recebidas pelos governos estaduais: cada produto catalogado
-- entregue, com quantidade e preço unitário efetivamente cobrado. Uma linha por (nota,
-- produto, ocorrência).
--
-- É o grão mais fino do conjunto, e o único lugar em que aparece um preço unitário
-- EFETIVAMENTE PAGO, e não de referência ou homologado. Ligue a `nota_fiscal` por
-- `id_nota_fiscal_bd` e a `licitacao_item` / `contrato_item` por `codigo_catalogo`.
--
-- ATENÇÃO: 0,11% das chaves de nota presentes aqui não têm linha correspondente em
-- `nota_fiscal`, então a ligação não é garantida; filtre pelo join quando precisar de
-- pai
-- garantido. A fonte não traz número de processo, contrato ou empenho -- ver
-- `nota_fiscal`.
--
-- `ano` e `mes` são o mês de referência do arquivo de origem: estas linhas não trazem
-- data alguma.
--
-- Existe só para Minas Gerais.
select *
from {{ ref("br_bd_execucao_estadual__nota_fiscal_item_mg") }}
