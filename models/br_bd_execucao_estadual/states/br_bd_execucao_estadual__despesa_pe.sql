{{ config(materialized="ephemeral") }}

-- Pernambuco execution, mapped onto the canonical `despesa` schema.
--
-- Source: e-Fisco via `todas-despesas-detalhadas` on dados.pe.gov.br, one
-- denormalised CSV
-- per exercise, 2008-2026.
--
-- PE qualifies for `despesa` where BA does not, for one reason: the empenho number, the
-- creditor and the three phase values are all on the SAME row. 255,753 rows over 71,966
-- empenhos in 2016 -- roughly 3.6 budget lines per document, the same shape as MG's
-- fact
-- table.
--
-- What PE lacks is any date below the exercise. There is no date column at all; the
-- empenho number encodes the year ("2018NE000122") and nothing finer. So `mes` and
-- `data`
-- are NULL for PE, and a query filtering `despesa` by month silently excludes it.
-- That is
-- a property of the source, not of this model.
--
-- Column order must match br_bd_execucao_estadual__despesa_mg exactly: the parent union
-- resolves positionally.
with
    fonte as (
        select
            *,
            -- Every classification arrives as "<code> - <label>", but the separator
            -- is not
            -- uniform ("0101  -  RECURSOS ORDINÁRIOS - ADM. DIRETA" uses double spaces)
            -- and the LABEL itself often contains " - " too. So the split is on the
            -- FIRST
            -- occurrence of a dash surrounded by whitespace, never on the last, and
            -- never
            -- with split() -- which would return the label's own fragments as extra
            -- parts.
            regexp_extract(cd_nm_funcao, r'^\s*([^\s-]+)\s*-') as cd_funcao,
            regexp_extract(cd_nm_subfuncao, r'^\s*([^\s-]+)\s*-') as cd_subfuncao,
            regexp_extract(cd_nm_prog, r'^\s*([^\s-]+)\s*-') as cd_programa,
            regexp_extract(cd_nm_acao, r'^\s*([^\s-]+)\s*-') as cd_acao,
            regexp_extract(cd_nm_categoria, r'^\s*([^\s-]+)\s*-') as cd_categoria,
            regexp_extract(cd_nm_grupo, r'^\s*([^\s-]+)\s*-') as cd_grupo,
            regexp_extract(cd_nm_modalidade, r'^\s*([^\s-]+)\s*-') as cd_modalidade,
            regexp_extract(cd_nm_elemento, r'^\s*([^\s-]+)\s*-') as cd_elemento,
            regexp_extract(cd_nm_item_vlrliquidado, r'^\s*([^\s-]+)\s*-') as cd_item,
            regexp_extract(cd_ds_fonte_recurso, r'^\s*([^\s-]+)\s*-') as cd_fonte,
            -- The creditor is "<document> - <name>", where the document is a full
            -- CNPJ or
            -- a partially masked CPF ("***.738.434-**"). The mask contains a dash, so
            -- the
            -- document is taken as everything before the first " - " rather than by
            -- splitting on '-'.
            regexp_extract(credor, r'^(.*?) - ') as doc_credor,
            regexp_extract(credor, r'^.*? - (.*)$') as nome_credor_extraido
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.pe_despesa") }}
    )

select
    safe_cast(ano as int64) as ano,
    cast(null as int64) as mes,
    cast(null as date) as data,
    'PE' as sigla_uf,
    cast(null as string) as orgao,
    cast(null as string) as nome_orgao,
    -- PE publishes the managing unit as a name only, with no code. Several names
    -- contain
    -- " - " themselves ("POLICIA MILITAR - SDS"), so the string is kept whole rather
    -- than
    -- split into a code that does not exist.
    cast(null as string) as id_unidade_gestora,
    safe_cast(trim(unidade_gestora) as string) as nome_unidade_gestora,
    concat('PE', ano, '-', numero_empenho) as id_empenho_bd,
    cast(null as string) as id_empenho,
    safe_cast(numero_empenho as string) as numero_empenho,
    safe_cast(ds_modalidade_empenho as string) as tipo_empenho,
    safe_cast(obs as string) as descricao,
    cast(null as string) as id_licitacao_bd,
    cast(null as string) as id_licitacao,
    -- PE spells the same modality several ways -- "PREGÃO  ELETRÔNICO" (32,240)
    -- alongside
    -- "PREGAO ELETRONICO" (4,157), differing only in accents and spacing. Collapsing
    -- the
    -- whitespace is a faithful canonicalisation; the accents are left alone, because
    -- stripping them would be a recode and the dicionario is the place to relate the
    -- variants. Anyone grouping by this column should expect both spellings.
    safe_cast(
        nullif(trim(regexp_replace(ds_tp_licitacao, r'\s+', ' ')), '') as string
    ) as modalidade_licitacao,
    safe_cast(nullif(doc_credor, '') as string) as documento_credor,
    safe_cast(nullif(nome_credor_extraido, '') as string) as nome_credor,
    cast(null as string) as tipo_documento_credor,
    safe_cast(nullif(cd_funcao, '') as string) as funcao,
    safe_cast(nullif(cd_subfuncao, '') as string) as subfuncao,
    safe_cast(nullif(cd_programa, '') as string) as programa,
    safe_cast(nullif(cd_acao, '') as string) as acao,
    safe_cast(nullif(cd_categoria, '') as string) as categoria_economica,
    safe_cast(nullif(cd_grupo, '') as string) as grupo_despesa,
    safe_cast(nullif(cd_modalidade, '') as string) as modalidade_aplicacao,
    safe_cast(nullif(cd_elemento, '') as string) as elemento_despesa,
    safe_cast(nullif(cd_item, '') as string) as item_despesa,
    safe_cast(nullif(cd_fonte, '') as string) as fonte_recurso,
    safe_cast(ds_tp_desp as string) as tipo_documento,
    -- PE writes money in US format with a leading space (" 43200.0"), NOT the Brazilian
    -- format BA uses. Verified across 255,753 rows of 2016: zero contain a comma.
    -- Applying
    -- BA's comma replacement here would be a no-op today and a silent corruption the
    -- day
    -- PE changes format, so the values are trimmed and cast directly.
    safe_cast(trim(vlrempenhado) as float64) as valor_empenhado,
    safe_cast(trim(vlrliquidado) as float64) as valor_liquidado,
    safe_cast(trim(vlrtotalpago) as float64) as valor_pago
from fonte
