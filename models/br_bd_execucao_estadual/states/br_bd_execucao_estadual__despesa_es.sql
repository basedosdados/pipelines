{{ config(materialized="ephemeral") }}

-- Espírito Santo execution, mapped onto the canonical `despesa` schema.
--
-- Source: SIGEFES via dados.es.gov.br, `portal-da-transparencia-despesas-execucao-
-- orcamentaria-e-financeira`, one CSV per exercise, refreshed daily.
--
-- Shape of the source: a single flat table, 71 columns, one row per accounting
-- document x budget line, carrying all three phase values side by side. Unlike MG
-- there is nothing to join -- every dimension already ships denormalised as a
-- code+label pair on the row -- so this model is a projection, not a star-schema
-- assembly.
--
-- ES is finer-grained than MG on two axes: it carries a full `Data` where MG has only
-- `dt_anomes`, and it carries the tender key on the expense row itself, where MG needs
-- the separate fl_compras_empenho bridge.
--
-- Coverage starts 2009. The package also publishes 2004-2008, but those files are
-- annual aggregates padded into the same 71 columns -- `Favorecido` is the literal
-- 'Informação não disponivel.', `CpfCnpjNis` is '0' and `Data` is pinned to 31/12 --
-- so they are excluded upstream in download_es.py rather than filtered here.
with
    fonte as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.es_despesa") }}
    ),
    -- SIGEFES stamps a process code on nearly every expense row (14,618,313 of
    -- 15,280,454), but a process is not a tender: 948,479 distinct codes appear on
    -- expense rows against only 81,639 tenders in SIGA. The rest are administrative
    -- processes -- diárias, payroll, transfers -- that never went through procurement.
    --
    -- So the tender key is emitted ONLY where the process is actually a known tender.
    -- Populating it from `CodigoProcesso` alone would leave ~94% of the keys dangling,
    -- and a key that matches nothing is worse than a null one: it reads as a link.
    tender as (
        select distinct nullif(trim(numeroprocesso), '') as numero_processo
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.es_licitacao") }}
        where nullif(trim(numeroprocesso), '') is not null
    )

select
    safe_cast(ano as int64) as ano,
    -- Month comes from `Data`, not from a separate column: ES has none. `Data` is
    -- 'dd/mm/yyyy hh:mm:ss', so the date is the first ten characters.
    extract(month from safe.parse_date('%d/%m/%Y', substr(data, 1, 10))) as mes,
    safe.parse_date('%d/%m/%Y', substr(data, 1, 10)) as data,
    'ES' as sigla_uf,
    safe_cast(codigoorgao as string) as orgao,
    safe_cast(trim(orgao) as string) as nome_orgao,
    safe_cast(codigounidadegestora as string) as id_unidade_gestora,
    safe_cast(trim(unidadegestora) as string) as nome_unidade_gestora,
    -- `DocumentoEmpenho` is '2024NE11480' -- already exercise-prefixed and unique
    -- within the state, so the surrogate needs only the UF.
    case
        when nullif(trim(documentoempenho), '') is not null
        then concat('ES-', trim(documentoempenho))
    end as id_empenho_bd,
    safe_cast(nullif(trim(documentoempenho), '') as string) as id_empenho,
    safe_cast(nullif(trim(documentoempenho), '') as string) as numero_empenho,
    -- SIGEFES publishes no empenho type (ordinário / estimativo / global) on the
    -- execution row. Left null rather than inferred from `Embasamento`, which is a
    -- free-text legal basis and not that classification.
    safe_cast(null as string) as tipo_empenho,
    safe_cast(nullif(trim(historicodocumento), '') as string) as descricao,
    -- `CodigoProcesso` ('2024-KZPCV') is the SIGA process key and joins directly to
    -- Licitacoes.NumeroProcesso. Note the source ALSO has a column literally called
    -- `NumeroProcesso`, which is a different, mostly-empty field -- using it here
    -- would null out the tender link on almost every row.
    --
    -- `t.numero_processo` is non-null only when the process is a real tender, so both
    -- columns fall to null on the ~94% of process codes that are administrative.
    case
        when t.numero_processo is not null then concat('ES-', t.numero_processo)
    end as id_licitacao_bd,
    safe_cast(t.numero_processo as string) as id_licitacao,
    safe_cast(nullif(trim(tipolicitacao), '') as string) as modalidade_licitacao,
    -- CPFs arrive partially masked ('###.743.147-##'), CNPJs in full and unformatted.
    -- Kept exactly as published: stripping the punctuation would make the two
    -- indistinguishable, and `TipoFavorecido` is what tells them apart.
    safe_cast(nullif(trim(cpfcnpjnis), '') as string) as documento_credor,
    safe_cast(nullif(trim(favorecido), '') as string) as nome_credor,
    safe_cast(nullif(trim(tipofavorecido), '') as string) as tipo_documento_credor,
    safe_cast(nullif(trim(codigofuncao), '') as string) as funcao,
    safe_cast(nullif(trim(codigosubfuncao), '') as string) as subfuncao,
    safe_cast(nullif(trim(codigoprograma), '') as string) as programa,
    safe_cast(nullif(trim(codigoacao), '') as string) as acao,
    safe_cast(
        nullif(trim(codigocategoriaeconomica), '') as string
    ) as categoria_economica,
    safe_cast(nullif(trim(codigogrupodespesa), '') as string) as grupo_despesa,
    safe_cast(nullif(trim(codigomodalidade), '') as string) as modalidade_aplicacao,
    safe_cast(nullif(trim(codigoelementodespesa), '') as string) as elemento_despesa,
    safe_cast(nullif(trim(codigosubelementodespesa), '') as string) as item_despesa,
    safe_cast(nullif(trim(codigofonte), '') as string) as fonte_recurso,
    -- Derived, not published: `Documento` is '2024OB19537' / '2010NE00518', so the
    -- letters between the exercise and the sequence are the document type (OB =
    -- ordem bancária, NE = nota de empenho, NL = nota de lançamento). Null when the
    -- pattern does not match, never a guess.
    safe_cast(
        nullif(regexp_extract(documento, r'^[0-9]{4}([A-Z]+)'), '') as string
    ) as tipo_documento,
    -- BR decimal comma with four places and NO thousands separator ('88,0000',
    -- '14751756,36'), so a bare comma->dot replacement is correct here. Do NOT copy
    -- this to SP, where 76% of values carry a '.' thousands separator and the same
    -- expression silently nulls three quarters of the column.
    safe_cast(replace(valorempenho, ',', '.') as float64) as valor_empenhado,
    safe_cast(replace(valorliquidado, ',', '.') as float64) as valor_liquidado,
    safe_cast(replace(valorpago, ',', '.') as float64) as valor_pago
from fonte
left join tender as t on nullif(trim(codigoprocesso), '') = t.numero_processo
