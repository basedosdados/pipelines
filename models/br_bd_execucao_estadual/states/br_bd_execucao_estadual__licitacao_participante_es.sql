{{ config(materialized="ephemeral") }}

-- Espírito Santo bidders, from SIGA's `ItensLotesDisputas` -- including losing bids.
--
-- Column order must match the other state models: the parent union resolves
-- positionally.
--
-- **The bid key is (CodigoLote, CodigoLoteItem), never CodigoLoteItem alone.** ES
-- mixes two grains in this one file: item-level bids carry `CodigoLoteItem`
-- ('ITEM-OUTRAS-8438119'), while pregão lots are bid as a whole and leave it EMPTY.
-- The split is large and moves year to year -- in 2023, 41,641 of 54,880 rows (75.9%)
-- are lot-level; in 2024 almost none are.
--
-- Keying on the item code alone therefore collapses every lot-level bid in 2023 into a
-- single empty-string group holding 2,331 tenders, 9,313 lots and 8,871 winner rows.
-- On (CodigoLote, CodigoLoteItem) the file is clean: 15,254 groups, at most ONE winner
-- each -- consistent with BA's published 0.96 winners per item, and unlike the 1.67
-- that BA's *inferred* flag produced before it was caught.
--
-- `Vencedor` is published, so it is read and never derived. Same for `Inabilitado` and
-- `Desclassificado`, which is why `situacao` is a translation of the three published
-- flags rather than a judgement about who won.
with
    fonte as (
        select *
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.es_licitacao_participante"
                )
            }}
    ),
    -- The bid rows name the tender by SIGA surrogate; the canonical id is the process
    -- number, which only Licitacoes carries.
    licitacao as (
        select
            trim(idlicitacao) as id_licitacao_siga,
            nullif(trim(numeroprocesso), '') as numero_processo
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.es_licitacao") }}
    )

select
    safe_cast(f.ano as int64) as ano,
    'ES' as sigla_uf,
    case
        when l.numero_processo is not null then concat('ES-', l.numero_processo)
    end as id_licitacao_bd,
    -- Lot and item together. On a lot-level bid the item half is absent, and the
    -- surrogate says so explicitly rather than silently colliding with every other
    -- lot-level bid in the state.
    case
        when nullif(trim(f.codigolote), '') is not null
        then
            concat(
                'ES-',
                trim(f.codigolote),
                '-',
                coalesce(nullif(trim(f.codigoloteitem), ''), 'LOTE')
            )
    end as id_item_bd,
    safe_cast(nullif(trim(f.codigoloteitem), '') as string) as id_item,
    safe_cast(nullif(trim(f.razaosocial), '') as string) as razao_social,
    -- Published formatted ('33.609.783/0001-64'); the bare form is the digits only.
    safe_cast(nullif(trim(f.cnpjcpffornecedor), '') as string) as documento_formatado,
    safe_cast(
        nullif(regexp_replace(f.cnpjcpffornecedor, r'[^0-9]', ''), '') as string
    ) as documento,
    -- A translation of the three published flags, in the order that decides the
    -- outcome: a disqualified or unqualified bid cannot also be the winner.
    case
        when lower(trim(f.desclassificado)) = 'true'
        then 'Desclassificado'
        when lower(trim(f.inabilitado)) = 'true'
        then 'Inabilitado'
        when lower(trim(f.vencedor)) = 'true'
        then 'Vencedor'
        else 'Perdedor'
    end as situacao,
    safe_cast(lower(trim(f.vencedor)) = 'true' as bool) as vencedor,
    -- SIGA records no bid quantity, and no separate cotado/homologado split: the one
    -- published figure is `MelhorLance`, the bidder's best offer on the lot or item.
    safe_cast(null as float64) as quantidade,
    safe_cast(replace(f.melhorlance, ',', '.') as float64) as valor_unitario_cotado,
    safe_cast(null as float64) as valor_total_cotado,
    safe_cast(null as float64) as valor_unitario_homologado,
    -- `ValorLicitacao` is the tender total repeated on every bid row, not this
    -- bidder's award, so it is NOT mapped to a homologated value here.
    safe_cast(null as float64) as valor_total_homologado,
    safe_cast(nullif(trim(f.modalidade), '') as string) as categoria
from fonte as f
left join licitacao as l on trim(f.idlicitacao) = l.id_licitacao_siga
