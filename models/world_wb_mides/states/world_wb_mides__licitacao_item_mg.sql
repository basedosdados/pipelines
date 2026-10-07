-- Minas Gerais (MG) contribution to world_wb_mides.licitacao_item.
--
-- Ported from `code/licitacao_item.ipynb`. An item row is itemLicitacao joined
-- to its price quotation (cotacaoLicitacao) and, where the item was awarded, to
-- its winner and awarded price (homologLicitacao).
--
-- The notebook reads `id_unidade_gestora` from an auxiliary Drive file
-- (`auxiliary_files/ug_id_mg.csv`), because itemLicitacao does not carry the
-- unit. That file is not in this repo and is not needed: the item's licitacao
-- does carry the unit, so it is taken from there by join.
--
-- `credLicitacao` is the notebook's third price source. It is EMPTY in every
-- exercise measured (0 rows, 2014-2026), so that arm is omitted rather than
-- joined to nothing.
--
-- `quantidade_proposta`, `valor_proposta` and `valor_total` have no MG source
-- and stay NULL, as they already are for MG in the published table.
with
    unidade_xwalk as (
        select distinct id_municipio, id_unidade_gestora, cod_unidade
        from
            (
                select id_municipio, id_unidade_gestora, cod_unidade
                from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_mg") }}
                union distinct
                select id_municipio, id_unidade_gestora, cod_unidade
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_contrato_mg") }}
                union distinct
                select id_municipio, id_unidade_gestora, cod_unidade
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_despesa_dotacao_mg"
                        )
                    }}
            )
    ),
    chave_licitacao as (
        select distinct
            t.seq_licitacao,
            t.id_municipio,
            t.ano,
            t.orgao,
            t.id_unidade_gestora,
            concat(
                t.orgao,
                ' ',
                ifnull(x.cod_unidade, concat('u:', t.id_unidade_gestora)),
                ' ',
                ifnull(t.num_processo, ''),
                ' ',
                ifnull(t.num_ano_processo, ''),
                ' ',
                ifnull(t.data_abert_proc_adm, ''),
                ' ',
                t.id_municipio,
                ' ',
                t.ano
            ) as id_licitacao_bd
        from {{ set_datalake_project("world_wb_mides_staging.raw_licitacao_mg") }} as t
        left join
            unidade_xwalk as x
            on t.id_municipio = x.id_municipio
            and t.id_unidade_gestora = x.id_unidade_gestora
    ),
    cotacao as (
        -- one quoted price per item
        select
            id_municipio,
            ano,
            orgao,
            seq_item_licitacao,
            avg(safe_cast(valor_cot_preco_unit as float64)) as valor_unitario_cotacao,
            sum(safe_cast(num_quant_item_cotado as float64)) as quantidade_cotada
        from
            {{
                set_datalake_project(
                    "world_wb_mides_staging.raw_licitacao_cotacao_mg"
                )
            }}
        group by 1, 2, 3, 4
    ),
    homologado as (
        -- the awarded price, quantity and winner for the item
        select
            id_municipio,
            ano,
            orgao,
            seq_item_licitacao,
            any_value(num_doc_vencedor) as documento,
            any_value(nom_vencedor) as nome_vencedor,
            sum(safe_cast(num_quant_item as float64)) as quantidade,
            avg(safe_cast(valor_unitario as float64)) as valor_unitario
        from
            {{
                set_datalake_project(
                    "world_wb_mides_staging.raw_licitacao_homologacao_mg"
                )
            }}
        group by 1, 2, 3, 4
    )
select
    safe_cast(i.ano as int64) as ano,
    'MG' as sigla_uf,
    safe_cast(i.id_municipio as string) as id_municipio,
    safe_cast(i.orgao as string) as orgao,
    safe_cast(k.id_unidade_gestora as string) as id_unidade_gestora,
    safe_cast(k.id_licitacao_bd as string) as id_licitacao_bd,
    safe_cast(i.seq_licitacao as string) as id_licitacao,
    safe_cast(null as string) as id_dispensa,
    -- the item's own stable key: its licitacao plus its lot/item number
    safe_cast(
        concat(
            k.id_licitacao_bd, ' ', ifnull(i.num_lote, ''), ' ', ifnull(i.num_item, '')
        ) as string
    ) as id_item_bd,
    safe_cast(i.seq_item_licitacao as string) as id_item,
    safe_cast(i.dsc_item as string) as descricao,
    safe_cast(i.num_item as int64) as numero,
    safe_cast(i.num_lote as int64) as numero_lote,
    safe_cast(i.dsc_unid_medida as string) as unidade_medida,
    c.quantidade_cotada,
    c.valor_unitario_cotacao,
    h.quantidade,
    h.valor_unitario,
    safe_cast(null as float64) as valor_total,
    safe_cast(null as int64) as quantidade_proposta,
    safe_cast(null as float64) as valor_proposta,
    safe_cast(null as float64) as valor_vencedor,
    safe_cast(h.nome_vencedor as string) as nome_vencedor,
    safe_cast(h.documento as string) as documento
from {{ set_datalake_project("world_wb_mides_staging.raw_licitacao_item_mg") }} as i
left join
    chave_licitacao as k
    on i.id_municipio = k.id_municipio
    and i.ano = k.ano
    and i.orgao = k.orgao
    and i.seq_licitacao = k.seq_licitacao
left join
    cotacao as c
    on i.id_municipio = c.id_municipio
    and i.ano = c.ano
    and i.orgao = c.orgao
    and i.seq_item_licitacao = c.seq_item_licitacao
left join
    homologado as h
    on i.id_municipio = h.id_municipio
    and i.ano = h.ano
    and i.orgao = h.orgao
    and i.seq_item_licitacao = h.seq_item_licitacao
