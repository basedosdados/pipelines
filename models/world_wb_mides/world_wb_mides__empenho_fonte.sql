-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="empenho_fonte",
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
with
    -- The parent's `_bd` key is NOT in the staging mirror -- it is built by the
    -- state model. Reading it from `ref()` rather than re-deriving the concat
    -- here means the two can never drift: one definition, one place.
    -- `seq_empenho`/`seq_liquidacao`/`seq_pagamento` are unambiguous across
    -- municipalities within one extraction (0 shared values over 120
    -- municipalities x 2016/2020/2024, measured 2026-09-24), so the join needs
    -- no further scoping.
    p_empenho as (
        select distinct id_empenho_bd, id_empenho
        from {{ ref("world_wb_mides__empenho_mg") }}
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    safe_cast(
        concat(
            p_empenho.id_empenho_bd,
            ' ',
            ifnull(t.dsc_fonte_recurso, ''),
            ' ',
            ifnull(t.dsc_cod_orcamentario, ''),
            ' ',
            ifnull(t.mes, '')
        ) as string
    ) as id_empenho_fonte_bd,
    safe_cast(p_empenho.id_empenho_bd as string) as id_empenho_bd,
    safe_cast(t.seq_empenho_fonte as string) as id_empenho_fonte,
    safe_cast(t.seq_empenho as string) as id_empenho,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.id_unidade_gestora as string) as id_unidade_gestora,
    safe_cast(t.dsc_fonte_recurso as string) as fonte_recurso,
    safe_cast(t.dsc_cod_orcamentario as string) as cod_orcamentario,
    safe_cast(t.valor_empenho as float64) as valor_empenho,
    safe_cast(t.valor_reforco_emp as float64) as valor_reforco_emp,
    safe_cast(t.valor_anulacao_emp as float64) as valor_anulacao_emp,
    safe_cast(t.valor_liquidacao as float64) as valor_liquidacao,
    safe_cast(t.valor_anu_liquidacao as float64) as valor_anu_liquidacao,
    safe_cast(t.valor_pagamento as float64) as valor_pagamento,
    safe_cast(t.valor_anu_pagamento as float64) as valor_anu_pagamento,
    safe_cast(t.valor_outras_baixas as float64) as valor_outras_baixas,
    safe_cast(t.valor_anu_outras_baixas as float64) as valor_anu_outras_baixas
from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_fonte_mg") }} as t
left join p_empenho on t.seq_empenho = p_empenho.id_empenho
