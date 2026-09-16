{{ config(materialized="ephemeral") }}

-- Ceará execution, mapped onto the canonical `despesa` schema.
--
-- Source: Portal da Transparência do Ceará (`cearatransparente.ce.gov.br`), the empenho
-- exports, 2006-2026. See code/download_ce.py and code/clean_ce.py; the source's defects
-- are recorded at length in models/br_bd_execucao_estadual/SOURCE_LESSONS.md (CE section).
--
-- **CE publishes empenho at ONE ROW PER EMPENHO, not per budget line.** Measured on 2025:
-- 196,255 rows over 30,600 distinct `numero`, but `(unidade_gestora, numero)` is unique --
-- the número simply restarts per unidade gestora, the SC pattern. So the empenho key is
-- (exercicio, unidade gestora, numero) and no pivot across budget lines is needed. About
-- 1% of keys repeat across the whole series (27k of 4.93M, mostly same natureza) -- the
-- same near-duplicate publishing MG shows at 9.3%; `despesa` carries no uniqueness test for
-- exactly this reason, so every row is kept.
--
-- **THREE naming eras are stacked in one staging table** (constants.CE_SCHEMAS): legacy
-- 2006-2013 (`num_ano`, `cod_*`, `vlr_*`), mid 2014-2018 (`exercicio`, `valor*`, names AND
-- codes) and the portal eras 2018+ (`unidade_gestora`, `valor_*`, labels only). Each row
-- populates exactly one era's columns, so every field below is a COALESCE across the eras'
-- columns; the value that is present is the row's own. `_final` columns already carry the
-- net of suplementação and anulação, verified: valor_empenhado_final = valor_empenhado +
-- valor_suplementado - valor_anulado to the cent.
--
-- **valor_empenhado and valor_pago are NET** (empenhado + suplementado - anulado; pago -
-- anulado_pago), to match SC's signed-movement convention. Portal uses its own `_final`
-- columns; legacy and mid compute the net from the component columns.
--
-- **valor_liquidado is NULL for CE, and that is measured, not an omission.** CE publishes a
-- liquidação document set (ce_liquidacao), but it does NOT link to the empenho: its
-- `unidade_gestora` code system overlaps the empenho's on only 120 of 982 codes, and the
-- empenho `numero` fans out ~93x on its own, so no (exercicio, gestora, numero) join
-- resolves (23.6% match, and fanned out where it does). Attributing a liquidação value to
-- an empenho would be invention. CE's liquidação lives in the `liquidacao` table instead,
-- where each document stands on its own (with `id_empenho_bd` null, the PB-pagamento
-- precedent). The empenho row itself carries no `valorliquidado` -- the column exists in the
-- mid-era schema but is empty on all 4.93M rows.
--
-- **Decimal format varies by era and sub-source but never carries a thousands separator**
-- (measured: no value contains both '.' and ','), so `replace(x, ',', '.')` parses every
-- era -- dot, comma and integer alike -- without an SP-style thousands strip.
--
-- **The code dimensions (categoria..item, funcao..acao, fonte) are era-limited.** Only the
-- legacy and mid eras publish a natureza CODE (`cod_item_natureza` / `natureza`); the portal
-- eras publish only LABELS (`natureza_da_despesa`, `funcao`, ...), which cannot be split
-- into the 8-digit natureza code, so the code-split columns are null for 2019+. The
-- função/subfunção/programa/ação and fonte columns are populated only by the portal eras,
-- which publish them as labels -- CE issues no separate codes for them -- so those columns
-- carry CE's label where present and are null otherwise. This mirrors the reality that each
-- state's dimension columns are in that state's own scheme and are not cross-state codes.
with
    fonte as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.ce_empenho") }}
    ),
    base as (
        select
            coalesce(
                nullif(trim(exercicio), ''), nullif(trim(num_ano), '')
            ) as exe,
            coalesce(
                nullif(trim(unidade_gestora), ''),
                nullif(trim(unidadegestora), ''),
                nullif(trim(cod_gestora), '')
            ) as ug,
            coalesce(
                nullif(trim(numero), ''), nullif(trim(cod_ne), '')
            ) as num,
            coalesce(
                safe.parse_date('%Y-%m-%d', substr(trim(dth_empenho), 1, 10)),
                safe.parse_date('%Y-%m-%d', substr(trim(dataemissao), 1, 10)),
                safe.parse_date('%d/%m/%Y', trim(data_de_emissao))
            ) as data,
            -- órgão: only the portal eras name it; legacy/mid carry only the gestora.
            coalesce(
                nullif(trim(unidade_orcamentaria), ''),
                nullif(trim(secretaria_orgao), '')
            ) as nome_orgao,
            -- gestora name where the era gives one (portal); legacy/mid have only a code.
            nullif(trim(unidade_executora), '') as nome_ug,
            -- Net empenhado. Portal: valor_empenhado_final. Legacy/mid: empenhado +
            -- suplementado - anulado. Each era's other terms are null, so coalesce picks
            -- the row's own era.
            coalesce(
                safe_cast(replace(valor_empenhado_final, ',', '.') as float64),
                safe_cast(replace(vlr_empenhado, ',', '.') as float64)
                + coalesce(safe_cast(replace(vlr_suplementado, ',', '.') as float64), 0)
                - coalesce(safe_cast(replace(vlr_anulado, ',', '.') as float64), 0),
                safe_cast(replace(valor, ',', '.') as float64)
                + coalesce(safe_cast(replace(valorsuplementado, ',', '.') as float64), 0)
                - coalesce(safe_cast(replace(valoranulado, ',', '.') as float64), 0)
            ) as valor_empenhado,
            -- Net pago. Portal: valor_pago_final (2019 Q4 reduzido has only valor_pago).
            -- Legacy/mid: pago - anulado_pago.
            coalesce(
                safe_cast(replace(valor_pago_final, ',', '.') as float64),
                safe_cast(replace(valor_pago, ',', '.') as float64),
                safe_cast(replace(vlr_pago, ',', '.') as float64)
                - coalesce(safe_cast(replace(vlr_anulado_pago, ',', '.') as float64), 0),
                safe_cast(replace(valorpago, ',', '.') as float64)
                - coalesce(safe_cast(replace(valoranuladopago, ',', '.') as float64), 0)
            ) as valor_pago,
            nullif(trim(cod_tipo_empenho), '') as tipo_empenho_legacy,
            nullif(trim(tipo_da_despesa), '') as tipo_despesa_portal,
            -- movement type, used to keep only original documents (see WHERE):
            nullif(trim(cod_movimento), '') as cod_movimento_legacy,
            nullif(trim(natureza), '') as tipo_mid,
            -- description: legacy dsc_linha, mid especificacaogeral. Portal publishes none.
            coalesce(
                nullif(trim(especificacaogeral), ''), nullif(trim(dsc_linha), '')
            ) as descricao,
            -- tender modality label where present (legacy code, mid label). No tender table.
            coalesce(
                nullif(trim(modalidadelicitacao), ''),
                nullif(trim(cod_licitacao_modalidade), '')
            ) as modalidade_licitacao,
            -- creditor. Legacy/mid publish a document column; the portal eras embed a
            -- partial document prefix inside the name ("54.212.382 FELLIPE ..."), so it is
            -- pulled off the front and the remainder is the name.
            nullif(trim(cpfcnpjcredor), '') as doc_credor_mid,
            nullif(trim(cod_credor), '') as cod_credor_legacy,
            coalesce(
                nullif(trim(razaosocialcredor), ''),
                nullif(trim(dsc_nome_credor), ''),
                nullif(trim(razao_social_do_credor), ''),
                nullif(trim(razao_social_credor), ''),
                nullif(trim(credor), ''),
                nullif(trim(beneficiario), '')
            ) as credor_raw,
            -- natureza CODE: legacy only (8-10 digits, C G MM EE + item). Mid's `natureza`
            -- is the movement type (Ordinária/Anulação/Suplementação), not a code, and its
            -- `classiforcamreduz` is only a reduced code; the portal eras give a label. So
            -- the code split is null outside the legacy era.
            nullif(trim(cod_item_natureza), '') as cod_natureza,
            -- portal label dimensions (null for legacy/mid).
            nullif(trim(funcao), '') as funcao,
            nullif(trim(subfuncao), '') as subfuncao,
            coalesce(
                nullif(trim(programa_de_governo), '')
            ) as programa,
            coalesce(
                nullif(trim(acao_de_governo), '')
            ) as acao,
            -- Only the portal eras publish a fonte, and as a label.
            nullif(trim(fonte_de_recurso), '') as fonte_recurso
        from fonte
        where
            coalesce(nullif(trim(numero), ''), nullif(trim(cod_ne), '')) is not null
            -- Keep only ORIGINAL empenho documents. Legacy (cod_movimento 11, 1,792,704 of
            -- 1,859,177) and mid (natureza Ordinária, 1,122,129 of 1,177,759) each publish
            -- their anulações and suplementações BOTH as separate documents with their own
            -- número AND as columns on the original row. Keeping the documents double-counts
            -- (measured: mid Anulação docs R$4,455.7M = Ordinária valoranulado R$4,451.0M;
            -- Suplementação docs R$165.8M = valorsuplementado R$165.8M, exact) and invents
            -- phantom empenhos, so they are dropped and the net is taken from the original
            -- row's own component columns. The portal eras carry the net in
            -- valor_empenhado_final on one row and publish no such documents.
            and not (
                nullif(trim(num_ano), '') is not null
                and coalesce(nullif(trim(cod_movimento), ''), '11') != '11'
            )
            and not (
                nullif(trim(num_ano), '') is null
                and coalesce(nullif(trim(natureza), ''), '') in ('Anulação', 'Suplementação')
            )
    )
select
    safe_cast(exe as int64) as ano,
    extract(month from data) as mes,
    data as data,
    'CE' as sigla_uf,
    -- órgão: CE names it only from 2018 on, and as a label rather than a code.
    safe_cast(null as string) as orgao,
    nome_orgao as nome_orgao,
    ug as id_unidade_gestora,
    nome_ug as nome_unidade_gestora,
    concat('CE-', exe, '-', ug, '-', num) as id_empenho_bd,
    concat(exe, '|', ug, '|', num) as id_empenho,
    num as numero_empenho,
    coalesce(tipo_despesa_portal, tipo_mid, tipo_empenho_legacy) as tipo_empenho,
    descricao as descricao,
    -- CE publishes no tender table; the empenho carries only a modality label.
    safe_cast(null as string) as id_licitacao_bd,
    safe_cast(null as string) as id_licitacao,
    modalidade_licitacao as modalidade_licitacao,
    -- document: mid CNPJ/CPF column, legacy internal credor code, or the numeric prefix the
    -- portal eras jam onto the front of the name.
    coalesce(
        doc_credor_mid,
        cod_credor_legacy,
        nullif(regexp_extract(credor_raw, r'^([0-9][0-9.\-/]+)\s'), '')
    ) as documento_credor,
    -- name: strip that leading document prefix from the portal-era name.
    coalesce(
        nullif(regexp_replace(credor_raw, r'^[0-9][0-9.\-/]+\s+', ''), ''), credor_raw
    ) as nome_credor,
    -- PJ where the document is a (partial) CNPJ, PF where it is a masked or bare CPF.
    case
        when doc_credor_mid like '%/%' or regexp_contains(credor_raw, r'^[0-9]{2}\.[0-9]{3}\.[0-9]{3}')
        then 'PJ'
        when doc_credor_mid like '%*%' or regexp_contains(doc_credor_mid, r'^[0-9]{11}$')
        then 'PF'
    end as tipo_documento_credor,
    -- The portal eras publish função/subfunção/programa/ação as LABELS; legacy and mid
    -- publish none, so these are null before 2018.
    funcao as funcao,
    subfuncao as subfuncao,
    programa as programa,
    acao as acao,
    -- Natureza code split, only where an 8-digit code exists (legacy/mid). C G MM EE.
    -- Legacy natureza is 8-10 digits (C G MM EE SS [+ item]); split the standard first 8.
    case when regexp_contains(cod_natureza, r'^[0-9]{8,10}$') then substr(cod_natureza, 1, 1) end as categoria_economica,
    case when regexp_contains(cod_natureza, r'^[0-9]{8,10}$') then substr(cod_natureza, 2, 1) end as grupo_despesa,
    case when regexp_contains(cod_natureza, r'^[0-9]{8,10}$') then substr(cod_natureza, 3, 2) end as modalidade_aplicacao,
    case when regexp_contains(cod_natureza, r'^[0-9]{8,10}$') then substr(cod_natureza, 5, 2) end as elemento_despesa,
    case when regexp_contains(cod_natureza, r'^[0-9]{8,10}$') then substr(cod_natureza, 7, 2) end as item_despesa,
    fonte_recurso as fonte_recurso,
    -- CE's document type is the empenho classification, already in tipo_empenho.
    safe_cast(null as string) as tipo_documento,
    valor_empenhado as valor_empenhado,
    -- Measured: CE's liquidação cannot be linked to the empenho (see header); the liquidação
    -- documents live in the `liquidacao` table.
    safe_cast(null as float64) as valor_liquidado,
    valor_pago as valor_pago
from base
