{{ config(materialized="ephemeral") }}

-- Minas Gerais contract items, mapped onto the canonical `contrato_item` schema.
--
-- Source: `portal_contratos/itens<ano>.csv` (github.com/transparencia-mg), 2022-2026,
-- 84,145 rows over the 25,468 contracts in `portal_contratos/contratos<ano>.csv`. The
-- `compras_contratos` dimensional model publishes items for PROCESSES (`ft_compras`,
-- which feeds `licitacao_item`) but not for CONTRACTS, so this flat export is the only
-- source for the contract-item grain and there is nothing to union.
--
-- GRAIN, and why `numero_item_pedido` is not the key. The source's item number is a
-- pedido/lot line, not a per-product identifier: 8,990 (contrato, numero_item_pedido)
-- pairs carry more than one row -- 31,985 rows in total -- and within them
-- `codigo_item_material_servico_numerico` differs in 6,981 cases, quantity in 1,188,
-- and
-- unit price in 8,883. Those are distinct products billed under one lot line, not
-- duplicates. So the key is (contract, catalogued product, occurrence), exactly the
-- construction `licitacao_item_mg.sql` arrived at for the same problem on the process
-- side. Numbering within (contract, product) rather than within the contract keeps the
-- id stable when the source republishes: adding a new product does not renumber the
-- others.
--
-- `id_contrato_bd` must agree with whatever `contrato_mg` minted for the same contract,
-- or the two tables will not join. `contrato_mg` uses `MG-<id_contrato>` for contracts
-- the CKAN dimension knows and `MG-P-<numero_contrato>` for the 8,427 it does not, so
-- the
-- same coalesce is repeated here. The lookup is unambiguous even though `nr_contrato`
-- repeats 8,146 times in `mg_dm_contrato`: none of those reused numbers appears in the
-- portal export (measured), and `min(id_contrato)` is a deterministic guard if that
-- ever
-- changes.
--
-- The source carries no CNPJ on the item rows -- only
-- `nome_empresarial_nome_fornecedor`
-- -- so `documento_contratado` comes from `mg_contrato`, which does carry it unmasked.
--
-- Values are comma-decimal with no thousands separator (`0,0`, `44754,88`).
--
-- WATCH OUT, unexplained source arithmetic: `valor_total_*` is NOT
-- quantity x unit price. For contract 9319194 item 1 the three rows read
-- (qtd 2, unit 2355.52, total 44754.88), (qtd 3, unit 396.88, total 8334.48),
-- (qtd 1, unit 1008.00, total 6048.00) -- implied multipliers 19, 21 and 6. So
-- `quantidade` is not the quantity the total was computed from; most likely it is a
-- per-delivery figure against a total covering the contract term, but MG documents
-- neither. All three columns are passed through as published rather than recomputed,
-- and
-- anyone deriving a unit price should divide the total by the quantity they can
-- justify,
-- not assume this one.
--
-- There is no homologated UNIT price here, unlike `portal_licitacoes_mg/item`, so
-- `valor_unitario` is NULL and only the reference unit price is published.
--
-- Every state model must project the canonical columns in THIS order: the union in the
-- parent resolves positionally, so a reordered or missing column silently shifts values
-- into the wrong field. Columns the source does not publish are explicit typed NULLs.
with
    contrato_key as (
        select
            trim(nr_contrato) as nr_contrato,
            min(safe_cast(id_contrato as string)) as id_contrato
        from
            {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_contrato") }}
        where nullif(trim(nr_contrato), '') is not null
        group by 1
    ),
    contratado as (
        select nr_contrato, documento_contratado
        from
            (
                select
                    nullif(trim(numero_contrato), '') as nr_contrato,
                    nullif(
                        regexp_replace(
                            coalesce(cnpj_cpf_fornecedor_formatado, ''), r'[^0-9]', ''
                        ),
                        ''
                    ) as documento_contratado,
                    row_number() over (
                        partition by nullif(trim(numero_contrato), '')
                        order by safe_cast(ano_assinatura_contrato as int64) desc
                    ) as rn
                from
                    {{
                        set_datalake_project(
                            "br_bd_execucao_estadual_staging.mg_contrato"
                        )
                    }}
                where nullif(trim(numero_contrato), '') is not null
            )
        where rn = 1
    ),
    base as (
        select
            safe_cast(i.ano_assinatura_contrato as int64) as ano,
            nullif(trim(i.numero_contrato), '') as numero_contrato,
            nullif(trim(i.numero_processo_formatado), '') as numero_processo,
            nullif(trim(i.numero_item_pedido), '') as numero_item,
            nullif(
                trim(i.codigo_item_material_servico_numerico), ''
            ) as codigo_catalogo,
            nullif(trim(i.item_material_servico), '') as descricao,
            nullif(trim(i.codigo_elemento_item_despesa), '') as item_despesa,
            nullif(trim(i.nome_elemento_item_despesa), '') as nome_item_despesa,
            nullif(trim(i.nome_empresarial_nome_fornecedor), '') as nome_contratado,
            safe_cast(
                replace(i.quantidade_item_pedido, ',', '.') as float64
            ) as quantidade,
            safe_cast(
                replace(i.valor_unitario_referencia_item_processo, ',', '.') as float64
            ) as valor_unitario_referencia,
            safe_cast(
                replace(i.valor_total_referencia_item_processo, ',', '.') as float64
            ) as valor_total_referencia,
            safe_cast(
                replace(i.valor_total_homologado, ',', '.') as float64
            ) as valor_total_homologado
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.mg_contrato_item"
                )
            }} as i
        where nullif(trim(i.numero_contrato), '') is not null
    )
select
    b.ano,
    'MG' as sigla_uf,
    coalesce(
        concat('MG-', k.id_contrato), concat('MG-P-', b.numero_contrato)
    ) as id_contrato_bd,
    concat(
        'MG-',
        b.numero_contrato,
        '-',
        coalesce(b.codigo_catalogo, 'SEMCAT'),
        '-',
        row_number() over (
            partition by b.numero_contrato, b.codigo_catalogo
            order by
                b.item_despesa, b.valor_total_homologado, b.quantidade, b.numero_item
        )
    ) as id_item_bd,
    b.numero_contrato,
    b.numero_processo,
    b.numero_item,
    b.codigo_catalogo,
    b.descricao,
    b.item_despesa,
    b.nome_item_despesa,
    ct.documento_contratado,
    b.nome_contratado,
    b.quantidade,
    cast(null as float64) as valor_unitario,
    b.valor_unitario_referencia,
    b.valor_total_referencia,
    b.valor_total_homologado
from base as b
left join contrato_key as k on b.numero_contrato = k.nr_contrato
left join contratado as ct on b.numero_contrato = ct.nr_contrato
