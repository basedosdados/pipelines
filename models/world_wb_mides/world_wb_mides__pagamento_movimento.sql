-- Minas Gerais only. Generated from the pinned source header contract
-- (`code/mg_source_headers.json`) plus the validated key definitions; see
-- `ARCHITECTURE_MG.md`. MG is the only state publishing this stream, so the
-- table's Coverage records sigla_uf = MG rather than implying national scope.
{{
    config(
        alias="pagamento_movimento",
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
    p_pagamento as (
        select distinct id_pagamento_bd, id_pagamento
        from {{ ref("world_wb_mides__pagamento_mg") }}
    )
select
    safe_cast(t.ano as int64) as ano,
    safe_cast(t.mes as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(t.id_municipio as string) as id_municipio,
    -- No stable column makes this key unique: colliding source rows differ
    -- only in a measure, or are identical apart from the portal's own
    -- sequence. `seq_mov_pagamento` is appended so the key identifies a row, at the
    -- cost of churning between extractions -- see
    -- `reference_tce_mg_seq_empenho_unstable`. Decided 2026-09-24.
    safe_cast(
        concat(
            p_pagamento.id_pagamento_bd,
            ' ',
            ifnull(t.dsc_tipo_doc, ''),
            ' ',
            ifnull(t.num_documento, ''),
            ' ',
            ifnull(t.data_emissao, ''),
            ' ',
            ifnull(t.seq_mov_pagamento, '')
        ) as string
    ) as id_pagamento_movimento_bd,
    safe_cast(p_pagamento.id_pagamento_bd as string) as id_pagamento_bd,
    safe_cast(t.seq_mov_pagamento as string) as id_mov_pagamento,
    safe_cast(t.seq_pagamento as string) as id_pagamento,
    safe_cast(t.orgao as string) as orgao,
    safe_cast(t.dsc_tipo_doc as string) as tipo_doc,
    safe_cast(t.dsc_tipo_doc_livre as string) as tipo_doc_livre,
    safe_cast(t.num_documento as string) as numero_documento,
    safe_cast(t.data_emissao as date) as data_emissao,
    safe_cast(t.dsc_inst_financeira as string) as inst_financeira,
    safe_cast(t.dsc_agencia as string) as agencia,
    safe_cast(t.dsc_conta_bancaria as string) as conta_bancaria,
    safe_cast(t.dsc_finalidade_conta as string) as finalidade_conta,
    safe_cast(t.dsc_tipo_aplicacao as string) as tipo_aplicacao,
    safe_cast(t.num_aplicacao as string) as numero_aplicacao,
    safe_cast(t.valor_movimentacao as float64) as valor_movimentacao
from
    {{ set_datalake_project("world_wb_mides_staging.raw_pagamento_movimento_mg") }} as t
left join p_pagamento on t.seq_pagamento = p_pagamento.id_pagamento
