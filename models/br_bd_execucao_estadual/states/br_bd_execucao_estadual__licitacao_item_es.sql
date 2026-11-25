{{ config(materialized="ephemeral") }}

-- Espírito Santo tender items, from SIGA's `ItensLotes`.
--
-- Column order must match the other state models: the parent union resolves
-- positionally.
--
-- `ItensLotes` carries no tender id -- only `CodigoLote` -- so the item reaches its
-- tender through Lotes. That chain resolves for 100% of item rows in both 2023 and
-- 2024 (28,206/28,206 and 20,615/20,615), so an inner join would be safe; it is left
-- outer anyway, because a future exercise breaking the chain should surface as null
-- ids rather than as rows that quietly disappear.
--
-- The winner comes from the bid file, matched on (CodigoLote, CodigoLoteItem). That
-- pair is the real bid key -- see licitacao_participante_es for why the item code
-- alone produces 8,871 winners for a single "item" in 2023.
with
    -- One row per (lot, item), NOT one per source row.
    --
    -- ES publishes the same lot-item once per expenditure-element classification, so
    -- `ItensLotes` holds 437,994 rows over 410,873 distinct pairs. Of the 26,988
    -- duplicated groups, 98.2% differ ONLY in `ElementoDespesa` -- description, unit
    -- and exercise are identical in every group, quantity differs in 7 and value in 15.
    -- Since the element label is deliberately not published here (see item_despesa
    -- below), collapsing loses nothing and is what makes `id_item_bd` a key.
    --
    -- `max` rather than `any_value` so a rebuild is deterministic, and rather than
    -- `sum` because these are one item published twice, not two items -- summing would
    -- double-count 26,988 items' quantities and values.
    item as (
        select
            trim(codigolote) as codigo_lote,
            trim(codigoloteitem) as codigo_item,
            max(ano) as ano,
            max(descricaoitemcompra) as descricao_item,
            max(unidade) as unidade,
            max(safe_cast(replace(quantidade, ',', '.') as float64)) as quantidade,
            max(
                safe_cast(replace(valorprevistototal, ',', '.') as float64)
            ) as valor_previsto
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.es_licitacao_item"
                )
            }}
        group by 1, 2
    ),
    -- Collapsed to one row per lot code, because `es_lote` is not unique on it: 17 of
    -- 243,931 rows repeat a `CodigoLote` under a second `IdLote` surrogate. Joined raw,
    -- those 17 lots fan every item they carry into two rows and break `id_item_bd`'s
    -- uniqueness -- 52 duplicated item groups, all of them manufactured by the join
    -- rather than present in the source.
    --
    -- The tender is withheld where the duplicates disagree about it, rather than
    -- picked arbitrarily; on the rows observed they agree, so this collapses cleanly.
    lote as (
        select
            trim(codigolote) as codigo_lote,
            case
                when count(distinct trim(idlicitacao)) = 1
                then any_value(trim(idlicitacao))
            end as id_licitacao_siga
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.es_lote") }}
        group by 1
    ),
    licitacao as (
        select
            trim(idlicitacao) as id_licitacao_siga,
            nullif(trim(numeroprocesso), '') as numero_processo
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.es_licitacao") }}
    ),
    -- Almost every (lot, item) has exactly one winner, but not all: across the full
    -- 2009-2026 history 37 of 229,310 groups have more than one (36 with two, one with
    -- three; 75 rows). None of them spans an exercise or a tender, so the key is right
    -- and the source genuinely publishes them.
    --
    -- 17 of the 37 name the SAME supplier on every row, which is a duplicate
    -- publication and safe to collapse. The other 20 name different suppliers, and
    -- there is no published basis for choosing between them, so the winner is WITHHELD
    -- rather than picked arbitrarily -- the same rule despesa_mg applies to its
    -- ambiguous procurement bridge. `any_value` over those rows would attribute the
    -- whole item to one of several suppliers and read as fact downstream.
    --
    -- `melhor_lance` follows the identity: it is only meaningful once the supplier is
    -- unambiguous, so it is withheld with them rather than averaged across bidders.
    vencedor as (
        select
            trim(codigolote) as codigo_lote,
            trim(codigoloteitem) as codigo_item,
            case
                when count(distinct trim(cnpjcpffornecedor)) = 1
                then any_value(trim(razaosocial))
            end as razao_social,
            case
                when count(distinct trim(cnpjcpffornecedor)) = 1
                then any_value(trim(cnpjcpffornecedor))
            end as documento,
            case
                when count(distinct trim(cnpjcpffornecedor)) = 1
                then any_value(safe_cast(replace(melhorlance, ',', '.') as float64))
            end as melhor_lance
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.es_licitacao_participante"
                )
            }}
        where lower(trim(vencedor)) = 'true'
        group by 1, 2
    )

select
    safe_cast(i.ano as int64) as ano,
    'ES' as sigla_uf,
    case
        when l.numero_processo is not null then concat('ES-', l.numero_processo)
    end as id_licitacao_bd,
    safe_cast(l.numero_processo as string) as id_licitacao,
    -- Same surrogate shape as licitacao_participante_es, so the two join.
    case
        when nullif(i.codigo_lote, '') is not null
        then
            concat(
                'ES-', i.codigo_lote, '-', coalesce(nullif(i.codigo_item, ''), 'LOTE')
            )
    end as id_item_bd,
    safe_cast(nullif(i.codigo_item, '') as string) as id_item,
    safe_cast(nullif(i.descricao_item, '') as string) as descricao,
    -- SIGA has no material catalogue code, group or class on the item row; the only
    -- classification published is the expenditure element, as a label.
    safe_cast(null as string) as codigo_catalogo,
    safe_cast(null as string) as grupo_material_servico,
    safe_cast(null as string) as classe_material_servico,
    -- NOT mapped from `ItensLotes.ElementoDespesa`. This column is documented as
    -- carrying codes that join to `despesa.item_despesa`, and what ES publishes on the
    -- item row is a LABEL ('MATERIAL PARA FESTIVIDADES E HOMENAGENS') for a different
    -- level of the classification: the elemento, where despesa.item_despesa is the
    -- subelemento. Putting it here would join against nothing while looking populated.
    --
    -- The code does exist in ES, on `Compras.CodigoElementoDespesa` ('333903941'), but
    -- that is the full natureza da despesa rather than the subelemento, and the join
    -- from ItensLotes to Compras has not been verified. Left null until it is.
    safe_cast(null as string) as item_despesa,
    safe_cast(nullif(i.unidade, '') as string) as unidade_medida,
    safe_cast(null as date) as data_homologacao,
    i.quantidade as quantidade,
    -- `ValorPrevistoTotal` is the estimate for the whole item line. The unit estimate
    -- is derived only where the quantity is a usable divisor; a zero or missing
    -- quantity yields null rather than an infinity.
    case
        when i.quantidade > 0 then i.valor_previsto / i.quantidade
    end as valor_unitario_referencia,
    i.valor_previsto as valor_referencia,
    v.melhor_lance as valor_unitario,
    -- No awarded total is published per item; only the winning bid figure above.
    safe_cast(null as float64) as valor_total,
    safe_cast(null as float64) as valor_atualizado,
    safe_cast(nullif(v.documento, '') as string) as documento_vencedor,
    safe_cast(nullif(v.razao_social, '') as string) as nome_vencedor,
    safe_cast(null as string) as tipo_documento_vencedor
from item as i
left join lote as lo on i.codigo_lote = lo.codigo_lote
left join licitacao as l on lo.id_licitacao_siga = l.id_licitacao_siga
left join
    vencedor as v on i.codigo_lote = v.codigo_lote and i.codigo_item = v.codigo_item
