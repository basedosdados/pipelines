{{ config(materialized="ephemeral") }}

-- Bahia tender items, mapped onto the canonical `licitacao_item` schema.
--
-- Source: `VW_PROC_AQUISICAO_ITEM` (SIMPAS/SAEB), 940,953 items.
--
-- Unlike MG, BA publishes the item independently of who won it: the winning supplier
-- and
-- the losing bidders both live in VW_PROC_AQUISICAO_FORNEC, which becomes
-- `licitacao_participante`. So the winner columns here are left NULL and the answer
-- is a
-- join, not a denormalised field -- BA is the one source where "who else bid" is a real
-- question with a real answer.
--
-- Column order must match br_bd_execucao_estadual__licitacao_item_mg exactly: the
-- parent
-- union resolves positionally.
select
    -- The item view carries no exercise of its own; the process id begins with the
    -- órgão
    -- and ends with the four-digit year (11640PE0432024 -> 2024), which is the only
    -- year
    -- available without joining back to the tender.
    safe_cast(right(t.processo_de_aquisicao, 4) as int64) as ano,
    'BA' as sigla_uf,
    concat('BA-', t.processo_de_aquisicao) as id_licitacao_bd,
    safe_cast(t.num_identificacao_formatado as string) as id_licitacao,
    concat('BA-', t.processo_de_aquisicao, '-', t.num_seq_chave) as id_item_bd,
    safe_cast(t.num_item as string) as id_item,
    -- "Nome do Item" is the short label ("Porta"); the full specification is the
    -- separate
    -- "Nome do Item Completo" ("Porta de madeira, semi-oca, lisa, para interior, ...").
    -- The specification is what any product classification has to work from.
    safe_cast(
        coalesce(nullif(t.nome_do_item_completo, ''), t.nome_do_item) as string
    ) as descricao,
    safe_cast(t.num_item_catalogo_formatado as string) as codigo_catalogo,
    safe_cast(t.desc_tip_item_mat as string) as grupo_material_servico,
    safe_cast(t.tipo_item as string) as classe_material_servico,
    -- A Bahia não classifica o item licitado por item de despesa; Minas classifica.
    cast(null as string) as item_despesa,
    cast(null as string) as unidade_medida,
    cast(null as date) as data_homologacao,
    safe_cast(replace(t.quantidade, ',', '.') as float64) as quantidade,
    safe_cast(
        replace(t.val_item_estimado, ',', '.') as float64
    ) as valor_unitario_referencia,
    safe_cast(
        replace(t.val_item_total_estimado, ',', '.') as float64
    ) as valor_referencia,
    safe_cast(replace(t.val_item_homologado, ',', '.') as float64) as valor_unitario,
    safe_cast(replace(t.val_item_total_homologado, ',', '.') as float64) as valor_total,
    cast(null as float64) as valor_atualizado,
    -- The winner is not on the item row in BA. See licitacao_participante.
    cast(null as string) as documento_vencedor,
    cast(null as string) as nome_vencedor,
    cast(null as string) as tipo_documento_vencedor
from
    {{ set_datalake_project("br_bd_execucao_estadual_staging.ba_licitacao_item") }} as t
-- Esta exportação também repete o próprio cabeçalho uma vez dentro dos dados, como a de
-- fornecedores. A linha vinha com `ano` nulo (o id do processo termina em "Aquisição",
-- não em quatro dígitos) e um item descrito como "Nome do Item Completo".
where t.processo_de_aquisicao != 'Processo de Aquisição'
