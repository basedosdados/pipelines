{{
    config(
        alias="despesa",
        schema="br_bd_execucao_estadual",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2002, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf", "mes"],
        labels={"tema": "economia"},
    )
}}

-- Transaction-level budget execution of Brazilian state governments.
--
-- One row per empenho document x budget line, carrying the three execution phases as
-- values. The sources publish the phases this way -- as columns on one row, not as
-- three
-- separate ledgers -- so the table follows them rather than MiDES's three-table split.
--
-- Each state is a separate ephemeral model under states/, so a state can be rebuilt or
-- reviewed on its own without touching the others. Column names come from the first
-- term
-- of the union and are resolved positionally, so every state model must project the
-- columns in this exact order.
--
-- States whose source only publishes annual aggregates (SP) are NOT here -- they are in
-- `despesa_anual`, because they have no empenho document and no sub-annual date, and
-- padding this table with nulls in those columns would misrepresent them.
select *
from {{ ref("br_bd_execucao_estadual__despesa_mg") }}
union all
select *
from {{ ref("br_bd_execucao_estadual__despesa_pe") }}
union all
select *
from {{ ref("br_bd_execucao_estadual__despesa_pe_legado") }}
