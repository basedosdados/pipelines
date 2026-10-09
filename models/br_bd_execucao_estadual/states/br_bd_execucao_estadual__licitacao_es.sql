{{ config(materialized="ephemeral") }}

-- Espírito Santo tenders, from SIGA via `portal-da-transparencia-compras-publicas`.
--
-- Column order must match the other state models: the parent union resolves
-- positionally.
--
-- SIGA splits a tender across four files, joined here:
-- Licitacoes  the tender itself                      (IdLicitacao, NumeroProcesso)
-- Lotes       its lots, with previsto/licitado value (IdLicitacao)
-- Editais     the published notice, with a real publication date (codigoLicitacao)
-- Compras     lot x item rows, the only place carrying IdOrgao (IdLicitacao)
--
-- ES publishes less tender metadata than BA: no homologation date, no poder, no
-- contracting form, no category, no group, and Editais carries no URL. Those columns
-- are null rather than filled with a plausible substitute.
with
    licitacao as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.es_licitacao") }}
    ),
    -- Lot values roll up to the tender. `ValorLicitado` is 0 on lots that never
    -- closed, so the homologated total is the sum of what was actually awarded.
    lote as (
        select
            trim(idlicitacao) as id_licitacao_siga,
            sum(
                safe_cast(replace(valorprevisto, ',', '.') as float64)
            ) as valor_previsto,
            sum(
                safe_cast(replace(valorlicitado, ',', '.') as float64)
            ) as valor_licitado
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.es_lote") }}
        group by 1
    ),
    -- A tender can have more than one edital (re-publication), so take the earliest:
    -- that is the date the tender became public.
    edital as (
        select
            trim(codigolicitacao) as id_licitacao_siga,
            min(
                safe.parse_date('%d/%m/%Y', substr(datapublicacao, 1, 10))
            ) as data_publicacao
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.es_edital") }}
        where nullif(trim(codigolicitacao), '') is not null
        group by 1
    ),
    -- Only `Compras` carries the órgão code; `Licitacoes` has the name alone. The
    -- pair is constant within a tender (verified on 2023 and 2024), so any_value is
    -- safe rather than arbitrary.
    orgao as (
        select
            trim(idlicitacao) as id_licitacao_siga, any_value(trim(idorgao)) as id_orgao
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.es_compra") }}
        group by 1
    )

select
    safe_cast(l.ano as int64) as ano,
    extract(
        month from safe.parse_date('%d/%m/%Y', substr(l.datacriacao, 1, 10))
    ) as mes,
    'ES' as sigla_uf,
    safe_cast(o.id_orgao as string) as orgao,
    safe_cast(nullif(trim(l.nomeorgao), '') as string) as nome_orgao,
    -- `NumeroProcesso` ('2024-KZPCV') is the key the expense rows carry as
    -- `CodigoProcesso`, so it -- not the SIGA surrogate `IdLicitacao` -- is what makes
    -- despesa joinable to licitacao.
    case
        when nullif(trim(l.numeroprocesso), '') is not null
        then concat('ES-', trim(l.numeroprocesso))
    end as id_licitacao_bd,
    safe_cast(nullif(trim(l.numeroprocesso), '') as string) as id_licitacao,
    safe_cast(nullif(trim(l.idlicitacao), '') as string) as numero_licitacao,
    safe.parse_date('%d/%m/%Y', substr(l.dataabertura, 1, 10)) as data_abertura,
    e.data_publicacao as data_publicacao,
    -- SIGA publishes no homologation date on the tender.
    safe_cast(null as date) as data_homologacao,
    -- Column order below follows licitacao_mg / licitacao_ba, which are byte-identical
    -- to each other and are the first terms of the union. It deliberately does NOT
    -- follow the order of the `columns:` list in schema.yml, which drifted out of sync
    -- (it has `poder` at position 11 where both published models have
    -- `descricao_objeto`). schema.yml's order is documentation and does not affect the
    -- built table; the union resolves positionally against the models.
    safe_cast(nullif(trim(l.objeto), '') as string) as descricao_objeto,
    safe_cast(nullif(trim(l.modalidade), '') as string) as modalidade,
    safe_cast(nullif(trim(l.tipolicitacao), '') as string) as tipo,
    -- The extended form is the human label ('Valor Global'); `CriterioClassificacao`
    -- is its single-letter code.
    safe_cast(
        nullif(trim(l.criterioclassificacaoextenso), '') as string
    ) as criterio_julgamento,
    safe_cast(nullif(trim(l.situacao), '') as string) as situacao,
    safe_cast(null as string) as poder,
    safe_cast(null as string) as forma_contratacao,
    safe_cast(null as string) as categoria,
    safe_cast(nullif(trim(l.registropreco), '') as string) as registro_preco,
    safe_cast(null as string) as grupo,
    lo.valor_previsto as valor_referencia,
    lo.valor_licitado as valor_homologado,
    -- Editais publishes a notice number and its dates, but no document URL.
    safe_cast(null as string) as url_edital,
    safe_cast(null as string) as processo_sei
from licitacao as l
left join lote as lo on trim(l.idlicitacao) = lo.id_licitacao_siga
left join edital as e on trim(l.idlicitacao) = e.id_licitacao_siga
left join orgao as o on trim(l.idlicitacao) = o.id_licitacao_siga
