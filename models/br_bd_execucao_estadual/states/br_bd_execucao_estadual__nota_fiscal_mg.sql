{{ config(materialized="ephemeral") }}

-- Minas Gerais invoices, mapped onto the canonical `nota_fiscal` schema.
--
-- Source: `portal_notas_fiscais/notas_<mes><aa>.csv` (github.com/transparencia-mg), 57
-- monthly files, 2022-01 to 2026-09, 1,354,341 rows. Nothing to union: the
-- `compras_contratos` dimensional model does not publish invoices at all, and no other
-- state in this dataset publishes them either.
--
-- WHAT THIS ADDS. The dataset previously stopped at authorisation and payment. This is
-- the delivery margin -- what a supplier actually invoiced, when it was received and
-- when
-- it was registered.
--
-- THE HARD LIMITATION, stated up front: THERE IS NO CONTRACT, PROCESS OR EMPENHO KEY.
-- Checked across all 114 resources of the repo -- no `numero_processo`,
-- `numero_contrato`,
-- `numero_empenho` or tender field exists anywhere in it. So `nota_fiscal` does NOT
-- join
-- to `contrato`, `licitacao` or `despesa` on a key, and no such column is invented
-- here.
-- The only available linkage is probabilistic, on
-- (id_unidade_gestora x documento_emissor x period), and for items also the catalogue
-- code. Treat this pair as a delivered-price and delivery-timing panel, not as a keyed
-- contract-to-delivery chain.
--
-- `ano` / `mes` ARE THE REFERENCE MONTH OF THE SOURCE FILE, not the emission date, and
-- this is deliberate. Three reasons:
-- * `data_de_emissao` is not trustworthy as a partition: it ranges 1969-09-22 to
-- 2026-09-22, and 58,534 rows (4.3%) carry an emission year earlier than their file's
-- year. `data_de_registro` is clean (2022-01-03 to 2026-09-22) but exists only here.
-- * `nota_fiscal_item` has NO date column whatsoever. Its period can only come from the
-- filename, which the clean step preserves as `arquivo_origem`.
-- * Deriving both tables' partition the same way keeps them aligned, so the join
-- between them does not cross partitions.
-- The real dates are all carried as columns; use `data_emissao` when the emission
-- date is
-- what matters, and note the 1969 sentinel when aggregating on it.
--
-- GRAIN, and the dedupe. No natural key in this file is unique. Measured over the
-- 1,354,341 rows:
-- numero alone                                      600,310 distinct
-- orgao + numero + serie + sequencial             1,100,137
-- + tipo_de_documento                             1,141,465
-- + fornecedor_cnpj_cpf                           1,344,530
-- + natureza + indicador_de_orcamento             1,352,605
-- The last is the widest key both this file and the item file share, and it leaves
-- 1,736
-- excess rows (0.13%). Only 62 of the 1,354,341 rows are exact duplicates; in the rest
-- `situacao`, `natureza` and `valor_total_com_impostos` differ, i.e. the same invoice
-- was
-- republished in a later month with a corrected status or value.
--
-- Those 1,736 are COLLAPSED here, keeping the most recent registration, so that
-- `id_nota_fiscal_bd` is a real key and `nota_fiscal_item` has an unambiguous parent to
-- point at. This is a deliberate 0.13% reduction of a restatement series, not a silent
-- drop: the count is stated, the rule is `data_de_registro desc, arquivo_origem desc`,
-- and the superseded versions are recoverable from the source files if ever needed.
--
-- Every state model must project the canonical columns in THIS order: the union in the
-- parent resolves positionally, so a reordered or missing column silently shifts values
-- into the wrong field.
with
    base as (
        select
            nullif(trim(orgao_emissor), '') as id_unidade_gestora,
            nullif(trim(tipo_de_documento), '') as tipo_documento,
            nullif(
                regexp_replace(coalesce(fornecedor_cnpj_cpf, ''), r'[^0-9]', ''), ''
            ) as documento_emissor,
            nullif(trim(fornecedor_nome_empresarial), '') as nome_emissor,
            nullif(trim(numero), '') as numero,
            nullif(trim(serie), '') as serie,
            nullif(trim(sequencial), '') as sequencial,
            nullif(trim(situacao), '') as situacao,
            nullif(trim(natureza), '') as natureza,
            nullif(trim(indicador_de_orcamento), '') as indicador_orcamento,
            safe.parse_date(
                '%Y-%m-%d', substr(trim(data_de_emissao), 1, 10)
            ) as data_emissao,
            safe.parse_date(
                '%Y-%m-%d', substr(trim(data_de_recebimento), 1, 10)
            ) as data_recebimento,
            safe.parse_date(
                '%Y-%m-%d', substr(trim(data_de_registro), 1, 10)
            ) as data_registro,
            safe_cast(
                replace(valor_total_com_impostos, ',', '.') as float64
            ) as valor_total,
            arquivo_origem,
            -- The reference period, parsed from the source filename
            -- (`notas_jan22.csv`). Portuguese three-letter month, two-digit year.
            2000 + safe_cast(
                regexp_extract(arquivo_origem, r'([0-9]{2})\.csv$') as int64
            ) as ano,
            case
                regexp_extract(arquivo_origem, r'([a-z]{3})[0-9]{2}\.csv$')
                when 'jan'
                then 1
                when 'fev'
                then 2
                when 'mar'
                then 3
                when 'abr'
                then 4
                when 'mai'
                then 5
                when 'jun'
                then 6
                when 'jul'
                then 7
                when 'ago'
                then 8
                when 'set'
                then 9
                when 'out'
                then 10
                when 'nov'
                then 11
                when 'dez'
                then 12
            end as mes
        from
            {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_nota_fiscal") }}
        where nullif(trim(numero), '') is not null
    ),
    keyed as (
        select
            *,
            concat(
                'MG-',
                coalesce(id_unidade_gestora, ''),
                '-',
                coalesce(tipo_documento, ''),
                '-',
                coalesce(documento_emissor, ''),
                '-',
                coalesce(numero, ''),
                '-',
                coalesce(serie, ''),
                '-',
                coalesce(sequencial, ''),
                '-',
                coalesce(natureza, ''),
                '-',
                coalesce(indicador_orcamento, '')
            ) as id_nota_fiscal_bd
        from base
    ),
    deduped as (
        select *
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_nota_fiscal_bd
                        order by data_registro desc, arquivo_origem desc
                    ) as rn
                from keyed
            )
        where rn = 1
    )
select
    ano,
    mes,
    'MG' as sigla_uf,
    id_nota_fiscal_bd,
    id_unidade_gestora,
    tipo_documento,
    numero,
    serie,
    sequencial,
    documento_emissor,
    nome_emissor,
    situacao,
    natureza,
    indicador_orcamento,
    data_emissao,
    data_recebimento,
    data_registro,
    valor_total
from deduped
