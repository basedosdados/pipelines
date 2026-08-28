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
-- `vencedor` is derived rather than published: BA marks the situação, not a boolean.
-- The
-- rule is deliberately narrow -- a supplier counts as the winner of an item when its
-- situação is not a rejection AND it has a homologated value, since a homologated
-- amount
-- is what an award actually is. Anything else is left 0 rather than guessed.
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
    case
        when
            lower(t.desc_situacao_fornec) not like '%desclassificad%'
            and lower(t.desc_situacao_fornec) not like '%inabilitad%'
            and safe_cast(replace(t.val_total_homologado, ',', '.') as float64) > 0
        then 1
        else 0
    end as vencedor,
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
