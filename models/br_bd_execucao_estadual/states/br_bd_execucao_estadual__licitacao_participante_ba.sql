{{ config(materialized="ephemeral") }}

-- Bahia tender participants, mapped onto the canonical `licitacao_participante` schema.
--
-- Source: `VW_PROC_AQUISICAO_FORNEC` (SIMPAS/SAEB) -- one row per (item, supplier),
-- with
-- what that supplier quoted and whether it was accepted.
--
-- `DESC_SITUACAO_FORNEC` is the column that makes this table worth having: it
-- distinguishes
-- a supplier that won from one that was "Desclassificado", so the losing bids are
-- visible
-- rather than implied. MiDES has bidder-level data for only four states, and its own
-- documentation of what the field means varies by TCE, so BA's single consistent
-- vocabulary is the cleaner instrument.
--
-- `vencedor` reads BA's own label and derives nothing. The vocabulary is exactly three
-- values -- Vencedor (777,824), Perdedor (676,580), Desclassificado (371,154) -- so the
-- flag is just `situacao = 'Vencedor'`.
--
-- An earlier version inferred it instead, as "not rejected AND has a homologated
-- value".
-- That was wrong: **84% of Perdedor rows also carry a positive val_total_homologado**
-- (569,079 of 676,580), because the column records the item's homologated amount rather
-- than what that particular bidder was awarded. The inferred flag marked 73% of all
-- rows
-- as winners and 1.67 winners per item, where the published label gives 0.96 -- roughly
-- one per item, as it must be. Do not re-derive this.
select
    safe_cast(right(t.processo_de_aquisicao, 4) as int64) as ano,
    'BA' as sigla_uf,
    concat('BA-', t.processo_de_aquisicao) as id_licitacao_bd,
    concat('BA-', t.processo_de_aquisicao, '-', t.num_seq_chave) as id_item_bd,
    safe_cast(t.item as string) as id_item,
    safe_cast(t.nom_fornecedor as string) as razao_social,
    -- BA publishes the document already formatted ("01.806.580/0001-71"). Kept as
    -- published and also stripped, because a join against any other source needs
    -- digits.
    safe_cast(t.cpf_cnpj_formatado as string) as documento_formatado,
    safe_cast(
        regexp_replace(t.cpf_cnpj_formatado, r'[^0-9]', '') as string
    ) as documento,
    safe_cast(t.desc_situacao_fornec as string) as situacao,
    case when t.desc_situacao_fornec = 'Vencedor' then 1 else 0 end as vencedor,
    safe_cast(replace(t.qtd_pedida, ',', '.') as float64) as quantidade,
    safe_cast(replace(t.val_item_cotado, ',', '.') as float64) as valor_unitario_cotado,
    safe_cast(replace(t.val_total_cotado, ',', '.') as float64) as valor_total_cotado,
    safe_cast(
        replace(t.val_item_homologado, ',', '.') as float64
    ) as valor_unitario_homologado,
    safe_cast(
        replace(t.val_total_homologado, ',', '.') as float64
    ) as valor_total_homologado,
    safe_cast(t.categoria as string) as categoria
from
    {{
        set_datalake_project(
            "br_bd_execucao_estadual_staging.ba_licitacao_participante"
        )
    }} as t
-- The export repeats its own header once inside the data; drop that row rather than
-- let it
-- surface as a supplier named after a column.
where t.desc_situacao_fornec != 'DESC_SITUACAO_FORNEC'
