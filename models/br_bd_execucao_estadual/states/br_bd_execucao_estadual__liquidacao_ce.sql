{{ config(materialized="ephemeral") }}

-- Ceará liquidações, at the liquidação-document level. Source: Portal da
-- Transparência do
-- Ceará, the liquidação exports (NLD), 2015-2026, 3,603,925 rows in two naming eras
-- (constants.CE_SCHEMAS['liquidacao']): 2015-2017 and 2018-2026. Each row populates one
-- era's columns, so every field is a COALESCE across them.
--
-- **id_empenho_bd is NULL** -- CE's liquidação gestora code system does not resolve
-- to the
-- empenho (only 120 of 982 gestora codes overlap and the empenho número fans out
-- ~93x, so
-- no key joins reliably), the same limitation recorded in `despesa_ce` and
-- `pagamento_ce`.
-- `numero_empenho` carries the raw reference. valor_liquidado is net of the row's own
-- anulação column; `credor` is a name and the source publishes no creditor document
-- here
-- (only the ordenador's CPF, a different party), so documento_credor is null.
with
    fonte as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.ce_liquidacao") }}
    ),
    base as (
        select
            nullif(trim(exercicio), '') as exe,
            coalesce(
                nullif(trim(unidade_gestora), ''), nullif(trim(unidadegestora), '')
            ) as ug,
            coalesce(
                nullif(trim(numero_do_documento_da_despesa), ''),
                nullif(trim(numerodocdespesa), ''),
                nullif(trim(numero), '')
            ) as numero_liquidacao,
            coalesce(
                nullif(trim(numero_nota_empenho_despesa), ''),
                nullif(trim(numeroned), '')
            ) as numero_empenho,
            coalesce(
                safe.parse_date(
                    '%Y-%m-%d', substr(trim(data_do_documento_da_despesa), 1, 10)
                ),
                safe.parse_date('%Y-%m-%d', substr(trim(datadocdespesa), 1, 10)),
                safe.parse_date('%Y-%m-%d', substr(trim(data_emissao), 1, 10)),
                safe.parse_date('%Y-%m-%d', substr(trim(dataemissao), 1, 10)),
                safe.parse_date('%d/%m/%Y', trim(data_do_documento_da_despesa)),
                safe.parse_date('%d/%m/%Y', trim(data_emissao)),
                safe.parse_date('%d/%m/%Y', trim(dataemissao))
            ) as data,
            coalesce(
                nullif(trim(unidade_executora), ''), nullif(trim(unidadeexecutora), '')
            ) as nome_ug,
            nullif(trim(credor), '') as nome_credor,
            coalesce(
                nullif(trim(tipo_de_documento_da_despesa), ''),
                nullif(trim(tipodocdespesa), '')
            ) as descricao,
            safe_cast(replace(valor, ',', '.') as float64) - coalesce(
                safe_cast(replace(valoranulado, ',', '.') as float64), 0
            ) as valor_liquidado
        from fonte
    )
select
    safe_cast(exe as int64) as ano,
    extract(month from data) as mes,
    data as data,
    'CE' as sigla_uf,
    concat(
        'CE-',
        exe,
        '-',
        ug,
        '-',
        numero_liquidacao,
        '-',
        row_number() over (
            partition by exe, ug, numero_liquidacao
            order by numero_empenho, valor_liquidado, data
        )
    ) as id_liquidacao_bd,
    numero_liquidacao as numero_liquidacao,
    -- Measured: CE's liquidação does not link to the empenho (see header).
    safe_cast(null as string) as id_empenho_bd,
    numero_empenho as numero_empenho,
    nome_ug as nome_unidade_gestora,
    -- CE publishes no creditor document on the liquidação, only the ordenador's CPF.
    safe_cast(null as string) as documento_credor,
    nome_credor as nome_credor,
    descricao as descricao,
    valor_liquidado as valor_liquidado
from base
where numero_liquidacao is not null
