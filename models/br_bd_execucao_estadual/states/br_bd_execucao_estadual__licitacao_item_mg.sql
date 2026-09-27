{{ config(materialized="ephemeral") }}

-- Minas Gerais tender items, mapped onto the canonical `licitacao_item` schema.
--
-- Source: `ft_compras` in the SIAD/MG `compras_contratos` model -- 2,138,523 rows from
-- 2010. One row per purchase of a catalogued product within a process; the same product
-- can appear more than once in one process. NOT one row per (process, item number) --
-- see the note on `id_item` below, which is what that misreading cost.
--
-- The row carries both the *reference* price the state estimated and the *homologated*
-- price it actually awarded, at unit and total level, plus the winning supplier. That
-- makes `valor_unitario` here directly comparable to MiDES's
-- `licitacao_item.valor_unitario`
-- -- the field the thin-markets work keys its good definition on, and the one that is
-- 100%
-- NULL in MiDES's PR data. See the .claude memory note on MiDES procurement linkage.
--
-- Only the winner appears: MG's open model publishes `id_contratado` on the awarded
-- item
-- and does not publish losing bids. So this feeds `licitacao_item`, and the winner
-- side of
-- `licitacao_participante`, but MG cannot populate a full bidder list the way BA can.
select
    safe_cast(f.ano_particao as int64) as ano,
    'MG' as sigla_uf,
    concat('MG-', f.id_processo) as id_licitacao_bd,
    safe_cast(p.cd_processo_formatado as string) as id_licitacao,
    -- CUIDADO: `ft_compras.id_item` NÃO é o número do item dentro do processo. É chave
    -- estrangeira para `dm_item` -- o ITEM DE DESPESA (MEDICAMENTOS, MATERIAL DE
    -- ESCRITÓRIO), a mesma classificação que `despesa.item_despesa` carrega. São 188
    -- valores distintos em 2,1 milhões de linhas, e todos os 188 casam com `dm_item`.
    -- Lido como número do item, produzia um `id_item_bd` com 4,28 linhas por chave.
    --
    -- O item de fato é o produto catalogado, `id_item_matserv` (102.653 distintos). Mas
    -- nem (processo, produto) é único: 11,7% das linhas são compras repetidas do mesmo
    -- produto no mesmo processo, com fornecedor, lote, quantidade ou preço diferentes
    -- --
    -- linhas legítimas, não duplicatas. Por isso a ocorrência entra na chave. Numerar
    -- dentro de (processo, produto), e não dentro do processo, mantém o id estável
    -- quando a fonte republica: incluir um produto novo não renumera os demais.
    concat(
        'MG-',
        f.id_processo,
        '-',
        f.id_item_matserv,
        '-',
        row_number() over (
            partition by f.id_processo, f.id_item_matserv
            order by f.id_contratado, f.vr_homologado, f.qt_item_pedido
        )
    ) as id_item_bd,
    -- MG não publica número de item dentro do processo; a Bahia publica (`num_item`).
    cast(null as string) as id_item,
    safe_cast(coalesce(im.nome, ms.nome) as string) as descricao,
    safe_cast(im.cd_item_matserv as string) as codigo_catalogo,
    safe_cast(gm.nome as string) as grupo_material_servico,
    safe_cast(cm.nome as string) as classe_material_servico,
    -- O item de despesa que `id_item` de fato aponta, no mesmo espaço de códigos de
    -- `despesa.item_despesa` e já coberto pelo `dicionario`.
    safe_cast(it.cd_item as string) as item_despesa,
    safe_cast(um.nome as string) as unidade_medida,
    safe_cast(f.dt_item_homologa as date) as data_homologacao,
    safe_cast(f.qt_item_pedido as float64) as quantidade,
    safe_cast(f.vr_un_referencia as float64) as valor_unitario_referencia,
    safe_cast(f.vr_referencia as float64) as valor_referencia,
    safe_cast(f.vr_un_homologado as float64) as valor_unitario,
    safe_cast(f.vr_homologado as float64) as valor_total,
    safe_cast(f.vr_atualizado as float64) as valor_atualizado,
    safe_cast(ct.nr_documento_anonimizado as string) as documento_vencedor,
    safe_cast(ct.nome_anonimizado as string) as nome_vencedor,
    safe_cast(ct.tp_documento as string) as tipo_documento_vencedor
from {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_ft_compras") }} as f
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_processo") }} as p
    on f.id_processo = p.id_processo
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_contratado") }} as ct
    on f.id_contratado = ct.id_contratado
-- Two catalogues coexist: `dm_material_servico` is the coarse item, `dm_item_matserv`
-- the
-- fully specified one ("CALCADO DE SEGURANCA - MODELO: BOTA; TAMANHO: 37; ..."). The
-- specified name is preferred and the coarse one is the fallback, because the detailed
-- description is what any product classification has to work from.
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_item_matserv") }}
    as im
    on f.id_item_matserv = im.id_item_matserv
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_material_servico") }}
    as ms
    on f.id_material_servico = ms.id_material_servico
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_grupo_matserv") }}
    as gm
    on f.id_grupo_matserv = gm.id_grupo_matserv
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_classe_matserv") }}
    as cm
    on f.id_classe_matserv = cm.id_classe_matserv
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_unidade_medida") }}
    as um
    on f.id_unidade_medida = um.id_unidade_medida
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_item") }} as it
    on f.id_item = it.id_item
