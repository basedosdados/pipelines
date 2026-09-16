{{ config(materialized="ephemeral") }}

-- Paraíba liquidações, at the liquidação-document level. Source: CGE-PB REST API
-- (`/despesas/liquidacoes`), 2015-2026, 3,164,262 rows.
--
-- `tipoLiquidacao` is 11 (liquidação) or 21 (anulação de liquidação); `valorEmpenhado` on
-- this endpoint is the movement value and is ALREADY signed (type 21 rows are negative,
-- summing to -R$17.24bn against type 11's +R$200.06bn), so valor_liquidado is that value
-- as published and the net is a plain SUM. The endpoint publishes no `valorLiquidado`
-- field; this movement value is the amount. Its total (R$182.83bn) does NOT match
-- `despesa.valor_liquidado` for PB (R$120.04bn) -- the empenho endpoint's cumulative
-- `valorLiquidado` and this document ledger disagree in the source, and CGE-PB publishes no
-- data dictionary to adjudicate. The figure is reported as the source gives it.
--
-- **id_empenho_bd resolves cleanly.** pb_liquidacao carries codigoOrgao + numeroEmpenho but
-- not codigoUnidade, which `despesa.id_empenho_bd` needs. But (ano, orgao, numeroempenho) ->
-- unidade is 100% unique in the empenho universe and the liquidação->empenho join is 100%
-- same-year, so the unidade is recovered from pb_empenho and the despesa-matching key
-- `PB-<ano>-<orgao>-<unidade>-<numero>` is rebuilt.
with
    liq as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.pb_liquidacao") }}
    ),
    -- (ano, orgao, numeroempenho) -> unidade, unique 100% (measured), to recover the unidade
    -- the liquidação endpoint drops but despesa.id_empenho_bd requires.
    emp_unidade as (
        select distinct
            trim(ano) as ano,
            trim(codigoorgao) as orgao,
            trim(numeroempenho) as numero,
            trim(codigounidade) as unidade
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.pb_empenho") }}
    )
select
    safe_cast(l.anoexercicio as int64) as ano,
    extract(
        month from safe.parse_date('%Y-%m-%d', substr(trim(l.datamovimento), 1, 10))
    ) as mes,
    safe.parse_date('%Y-%m-%d', substr(trim(l.datamovimento), 1, 10)) as data,
    'PB' as sigla_uf,
    concat(
        'PB-', trim(l.anoexercicio), '-', trim(l.codigoorgao), '-',
        trim(l.numerodocumento), '-',
        row_number() over (
            partition by
                trim(l.anoexercicio), trim(l.codigoorgao), trim(l.numerodocumento)
            order by trim(l.numeroempenho), l.valorempenhado, trim(l.cnpjcpfcredor)
        )
    ) as id_liquidacao_bd,
    nullif(trim(l.numerodocumento), '') as numero_liquidacao,
    case
        when e.unidade is not null
        then concat(
            'PB-', trim(l.anoexercicio), '-', trim(l.codigoorgao), '-', e.unidade, '-',
            trim(l.numeroempenho)
        )
    end as id_empenho_bd,
    nullif(trim(l.numeroempenho), '') as numero_empenho,
    -- PB names the órgão, not the unidade.
    nullif(trim(l.nomeorgao), '') as nome_unidade_gestora,
    nullif(trim(l.cnpjcpfcredor), '') as documento_credor,
    nullif(trim(l.nomecredor), '') as nome_credor,
    safe_cast(null as string) as descricao,
    -- Already signed by tipoLiquidacao (21 = anulação, negative in the source).
    safe_cast(l.valorempenhado as float64) as valor_liquidado
from liq as l
left join emp_unidade as e
    on trim(l.anoexercicio) = e.ano
    and trim(l.codigoorgao) = e.orgao
    and trim(l.numeroempenho) = e.numero
where nullif(trim(l.numerodocumento), '') is not null
