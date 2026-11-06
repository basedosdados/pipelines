-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="liquidacao_nota_fiscal",
        schema="world_wb_mides",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2014, "end": 2031, "interval": 1},
        },
        cluster_by=["id_municipio"],
        labels={"tema": "economia"},
    )
}}
-- THIS TABLE PUBLISHES NO LINK TO ITS LIQUIDACAO, DELIBERATELY.
--
-- It used to join `t.seq_liquidacao` to the state model's `id_liquidacao` with no
-- municipality scoping, on the stated ground that the sequence is unambiguous
-- across municipalities. Measured against the full mirror on 2026-09-28, that is
-- false for this pairing: of 43,353,181 rows, 15,674,114 (36.2%) matched a parent
-- somewhere, but only 43,854 (0.1%) matched one in the SAME municipality. So
-- 99.7% of the foreign keys the table published pointed at another
-- municipality's liquidacao -- populated, plausible and wrong, which no not-null
-- or uniqueness test detects.
--
-- `id_liquidacao` is the right counterpart column (its 36.2% global hit rate
-- equals the built table's non-NULL rate), so this is not a column mix-up: the
-- invoice stream's `seq_liquidacao` and the liquidacao stream's `id_liquidacao`
-- are not the same identifier space within a municipality. Resolving that needs
-- an answer from TCE-MG about the source semantics.
--
-- Until then the column is NULL for every row. A user can see there is no link;
-- they cannot see that a link is wrong. `id_liquidacao` below still carries the
-- raw value, so nothing is lost and the join can be restored once the semantics
-- are known.
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    -- Self-sufficient: it no longer depends on the parent key. Verified UNIQUE
    -- over all 43,353,181 rows, with no NULL in any component (DuckDB on the
    -- mirror, 2026-09-28). Like the other 18 keys carrying a `seq_*`, it
    -- identifies a row rather than surviving a re-extraction.
    safe_cast(
        concat(t.id_municipio, ' ', t.ano, ' ', t.seq_liq_nota_fiscal) as string
    ) as id_liquidacao_nota_fiscal_bd,
    cast(null as string) as id_liquidacao_bd,
    safe_cast(t.seq_liq_nota_fiscal as string) as id_liq_nota_fiscal,
    safe_cast(t.seq_nota_fiscal as string) as id_nota_fiscal,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.seq_empenho as string) as id_empenho,
    safe_cast(t.seq_rsp as string) as id_rsp,
    safe_cast(t.seq_liquidacao as string) as id_liquidacao,
    safe_cast(t.num_doc_emitente as string) as numero_doc_emitente,
    safe_cast(t.nom_emitente as string) as nome_emitente
from
    {{ set_datalake_project("world_wb_mides_staging.raw_liquidacao_nota_fiscal_mg") }}
    as t
