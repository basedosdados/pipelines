{{ config(materialized="ephemeral") }}

-- Rio Grande do Sul execution, PIVOTED onto the canonical `despesa` schema.
--
-- Source: CAGE's `Gasto-RS` via dados.rs.gov.br, monthly ZIPs, 2012-2026, CC0.
--
-- **RS is the only source here published as one row per PHASE.** MG, BA, PE and SP all
-- put `empenhado` / `liquidado` / `pago` side by side on one row, which is why this
-- dataset has one `despesa` table rather than MiDES's three. RS instead emits a
-- movement per phase, so it is pivoted here to match. That direction works and the
-- reverse does not: unpivoting a three-column row invents documents and dates the
-- source never published, while pivoting only aggregates rows that already exist.
--
-- The phases nest cleanly, which is what makes the pivot safe -- every row carries its
-- empenho, and each later phase adds its own document number:
--
-- Empenho      15,021,610 rows   empenho only
-- Liquidação   15,692,456        empenho + liquidacao
-- Pagamento    14,921,596        empenho + liquidacao + pagamento
-- Retenção      4,930,110        empenho + liquidacao + retencao
--
-- Two phases are corrections and one is a different concept:
--
-- * `Prescrição de Empenho` (687 rows, -R$86.4M) and `Prescrição de Liquidação`
-- (626, -R$1.7M) are lapsed commitments, carrying negative values. They are folded
-- into the phase they correct, so the totals are net of write-offs.
-- * **`Retenção` is EXCLUDED** (4,930,110 rows, R$27.5bn). A retenção is tax or
-- charge withheld from within a payment, not additional expenditure; adding it to
-- any of the three columns would double-count money already in `valor_pago`. The
-- canonical schema has no column for it, so it is dropped rather than misfiled.
-- * 270 rows carry no `FaseGasto` at all (-R$33.1M) and cannot be attributed.
--
-- Grain is one row per empenho x budget line, matching the other states. That is
-- almost one row per empenho: of 14,763,432 empenhos, only 222 (0.002%) touch a second
-- budget line and NONE has more than one creditor.
--
-- `ano`, `mes` and `data` come from the empenho's own commitment row, so a 2020
-- commitment paid in 2021 stays in 2020 with its payment beside it -- the same
-- semantics as MG. 173,649 empenhos (1.18%) have no commitment row, because they were
-- committed before 2012 or inside one of the six months missing from RS's catalogue;
-- those fall back to their earliest movement.
with
    fonte as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.rs_despesa") }}
    ),
    movimento as (
        select
            nullif(trim(empenho), '') as empenho,
            trim(fasegasto) as fase,
            safe_cast(ano as int64) as ano_mov,
            safe_cast(mes as int64) as mes_mov,
            safe.parse_date('%d/%m/%Y', substr(trim(data), 1, 10)) as data_mov,
            -- BR decimal comma, no thousands separator ('182,63', '300000,00'), the
            -- same shape as BA and ES. Do NOT copy this to SP, where 76% of values
            -- carry a '.' thousands separator and this expression would null them.
            safe_cast(replace(valor, ',', '.') as float64) as valor,
            trim(cod_orgao) as cod_orgao,
            trim(orgao) as nome_orgao,
            trim(cod_uo) as cod_uo,
            trim(uo) as nome_uo,
            trim(cod_funcao) as cod_funcao,
            trim(cod_subfuncao) as cod_subfuncao,
            trim(cod_programa) as cod_programa,
            trim(cod_acao) as cod_acao,
            trim(cod_categoria) as cod_categoria,
            trim(cod_grupo) as cod_grupo,
            trim(cod_modalidade) as cod_modalidade,
            trim(cod_elemento) as cod_elemento,
            trim(cod_rubrica) as cod_rubrica,
            trim(cod_recurso) as cod_recurso,
            nullif(trim(cnpj), '') as documento_credor,
            nullif(trim(favorecido), '') as nome_credor,
            nullif(trim(procedimentolicitatorio), '') as modalidade_licitacao,
            nullif(trim(informacoes_complementares), '') as descricao
        from fonte
        where nullif(trim(empenho), '') is not null
    ),
    pivotado as (
        select
            empenho,
            cod_orgao,
            cod_uo,
            cod_funcao,
            cod_subfuncao,
            cod_programa,
            cod_acao,
            cod_categoria,
            cod_grupo,
            cod_modalidade,
            cod_elemento,
            cod_rubrica,
            cod_recurso,
            -- ONE row supplies ano, mes and data together. Minimising them separately
            -- invents pairs that never occurred: an empenho with movements in 2020-07
            -- and 2021-03 came out as 2020-03, which silently filled all six months
            -- missing from RS's catalogue and reported a complete calendar.
            --
            -- The ordering prefers the empenho's own commitment row, then the earliest
            -- movement, so a 2020 commitment paid in 2021 keeps its 2020 date with the
            -- payment beside it -- the same semantics as MG.
            array_agg(
                struct(ano_mov as ano, mes_mov as mes, data_mov as data)
                order by
                    if(fase = 'Empenho', 0, 1),
                    -- Sentinels rather than `nulls last`: BigQuery rejects that
                    -- combination inside an aggregate's ORDER BY, and a row whose
                    -- exercise failed to cast must not win the ordering.
                    coalesce(ano_mov, 9999),
                    coalesce(mes_mov, 99),
                    coalesce(data_mov, date '9999-12-31')
                limit 1
            )[safe_offset(0)] as first_mov,
            max(nome_orgao) as nome_orgao,
            max(nome_uo) as nome_uo,
            max(descricao) as descricao,
            max(modalidade_licitacao) as modalidade_licitacao,
            max(documento_credor) as documento_credor,
            max(nome_credor) as nome_credor,
            sum(
                if(fase in ('Empenho', 'Prescrição de Empenho'), valor, 0)
            ) as valor_empenhado,
            sum(
                if(fase in ('Liquidação', 'Prescrição de Liquidação'), valor, 0)
            ) as valor_liquidado,
            sum(if(fase = 'Pagamento', valor, 0)) as valor_pago
        from movimento
        group by
            empenho,
            cod_orgao,
            cod_uo,
            cod_funcao,
            cod_subfuncao,
            cod_programa,
            cod_acao,
            cod_categoria,
            cod_grupo,
            cod_modalidade,
            cod_elemento,
            cod_rubrica,
            cod_recurso
    )

select
    first_mov.ano as ano,
    first_mov.mes as mes,
    first_mov.data as data,
    'RS' as sigla_uf,
    cod_orgao as orgao,
    nome_orgao as nome_orgao,
    cod_uo as id_unidade_gestora,
    nome_uo as nome_unidade_gestora,
    concat('RS-', empenho) as id_empenho_bd,
    empenho as id_empenho,
    empenho as numero_empenho,
    -- RS publishes no empenho type (ordinário / estimativo / global). `TipoGasto` has
    -- two values and describes the spending route, not that classification.
    safe_cast(null as string) as tipo_empenho,
    descricao as descricao,
    -- RS publishes no tender table at all -- a 14-keyword sweep of its 412 CKAN
    -- packages found no licitações -- so there is nothing for a tender id to reach.
    -- Emitting one from `Processo` would leave every key dangling, which reads as a
    -- link; see despesa_es for the same decision made the other way once a tender
    -- table existed.
    safe_cast(null as string) as id_licitacao_bd,
    safe_cast(null as string) as id_licitacao,
    modalidade_licitacao as modalidade_licitacao,
    documento_credor as documento_credor,
    nome_credor as nome_credor,
    -- Derived from the published format, not inferred from content: RS prints natural
    -- persons as `000.000.000-00` (fully zeroed, unlike MG's `***.195.606-**`) and
    -- companies as a full CNPJ, and the two masks are structurally distinct.
    case
        when documento_credor like '%/%'
        then 'PJ'
        when documento_credor is not null
        then 'PF'
    end as tipo_documento_credor,
    cod_funcao as funcao,
    cod_subfuncao as subfuncao,
    cod_programa as programa,
    cod_acao as acao,
    cod_categoria as categoria_economica,
    cod_grupo as grupo_despesa,
    cod_modalidade as modalidade_aplicacao,
    cod_elemento as elemento_despesa,
    cod_rubrica as item_despesa,
    cod_recurso as fonte_recurso,
    -- The document type IS the phase in this source, and the pivot has just consumed
    -- it, so there is nothing left for this column to carry.
    safe_cast(null as string) as tipo_documento,
    valor_empenhado as valor_empenhado,
    valor_liquidado as valor_liquidado,
    valor_pago as valor_pago
from pivotado
