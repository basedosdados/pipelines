{{ config(materialized="ephemeral") }}

-- Minas Gerais execution, mapped onto the canonical `despesa` schema.
--
-- Source: SIAFI/MG via dados.mg.gov.br, dimensional model, 2002-2026, daily D+1.
--
-- Shape of the source: `ft_despesa` is the fact table, one row per empenho x
-- budget-line x
-- accounting document, carrying all three phase values (vr_empenhado / vr_liquidado /
-- vr_pago) side by side. Everything else is a surrogate-key lookup. MG versions some
-- dimensions by exercise (funcao, subfuncao, programa, acao, unidade_orc all carry
-- ano_exercicio) because the codes are re-issued between PPAs, so those join on the
-- surrogate id alone -- the id already encodes the vintage, and joining on
-- (id, ano) would drop restos a pagar rows whose ano_particao is an earlier exercise.
with
    fato as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_ft_despesa") }}
    ),
    empenho as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_empenho") }}
    ),
    favorecido as (
        select *
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.mg_dm_favorecido"
                )
            }}
    ),
    -- The procurement bridge is genuinely many-to-many, so it must be collapsed to
    -- one row
    -- per empenho before joining or it fans the fact table out and inflates every
    -- total.
    --
    -- Where an empenho maps to more than one process the link is ambiguous, and the
    -- id is
    -- WITHHELD rather than a winner picked arbitrarily: `any_value` would attribute the
    -- whole commitment to one of several tenders and read as fact downstream. The full
    -- many-to-many relation is preserved losslessly in `relacionamentos`; this column
    -- is
    -- only the unambiguous convenience key.
    bridge as (
        select
            id_empenho,
            case
                when count(distinct id_processo) = 1 then any_value(id_processo)
            end as id_processo
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.mg_fl_compras_empenho"
                )
            }}
        where id_processo is not null
        group by id_empenho
    ),
    processo as (
        select *
        from
            {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_processo") }}
    )

select
    safe_cast(f.ano as int64) as ano,
    safe_cast(substr(f.dt_anomes, 5, 2) as int64) as mes,
    safe_cast(e.dt_empenho as date) as data,
    'MG' as sigla_uf,
    -- MG has no separate orgao code on the fact row; the budget unit's administration
    -- grouping is the closest stable equivalent, and the unidade gestora carries the
    -- operational detail.
    safe_cast(uo.cd_unidade_orc as string) as orgao,
    safe_cast(uo.nome as string) as nome_orgao,
    -- `unidade_executora` is "1320076 - C - SUBPAS/SAPS(ATEN.PRIM": the separator
    -- recurs
    -- inside the name, so the code is the first split part but the name must be
    -- everything
    -- after the FIRST separator, not the second split part. strpos returns 0 when the
    -- separator is absent, which would make substr slice from the wrong offset and
    -- emit a
    -- mangled name, so that case is explicitly null.
    safe_cast(
        split(e.unidade_executora, ' - ')[safe_offset(0)] as string
    ) as id_unidade_gestora,
    case
        when strpos(e.unidade_executora, ' - ') > 0
        then
            safe_cast(
                trim(
                    substr(e.unidade_executora, strpos(e.unidade_executora, ' - ') + 3)
                ) as string
            )
    end as nome_unidade_gestora,
    concat('MG', f.ano, '-', f.id_empenho) as id_empenho_bd,
    safe_cast(f.id_empenho as string) as id_empenho,
    safe_cast(e.nr_empenho as string) as numero_empenho,
    safe_cast(e.tipo_empenho as string) as tipo_empenho,
    safe_cast(e.uni_prog_gasto as string) as descricao,
    -- Only populated where MG's own compras<->empenho bridge resolves; null otherwise,
    -- never guessed from the dotacao string.
    case
        when b.id_processo is not null then concat('MG-', b.id_processo)
    end as id_licitacao_bd,
    safe_cast(p.cd_processo_formatado as string) as id_licitacao,
    safe_cast(p.procedimento as string) as modalidade_licitacao,
    safe_cast(fav.nr_documento_anonimizado as string) as documento_credor,
    safe_cast(fav.nome_anonimizado as string) as nome_credor,
    safe_cast(fav.tp_documento as string) as tipo_documento_credor,
    safe_cast(fu.cd_funcao as string) as funcao,
    safe_cast(sf.cd_subfuncao as string) as subfuncao,
    safe_cast(pr.cd_programa as string) as programa,
    safe_cast(ac.cd_acao as string) as acao,
    safe_cast(ce.cd_categ_econ as string) as categoria_economica,
    safe_cast(gr.cd_grupo as string) as grupo_despesa,
    safe_cast(ma.cd_modalidade_aplic as string) as modalidade_aplicacao,
    safe_cast(el.cd_elemento as string) as elemento_despesa,
    safe_cast(it.cd_item as string) as item_despesa,
    safe_cast(fo.cd_fonte as string) as fonte_recurso,
    safe_cast(td.nome as string) as tipo_documento,
    safe_cast(f.vr_empenhado as float64) as valor_empenhado,
    safe_cast(f.vr_liquidado as float64) as valor_liquidado,
    safe_cast(f.vr_pago as float64) as valor_pago
from fato as f
left join empenho as e on f.id_empenho = e.id_empenho
left join favorecido as fav on f.id_favorecido = fav.id_favorecido
left join bridge as b on f.id_empenho = b.id_empenho
left join processo as p on b.id_processo = p.id_processo
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_funcao") }} as fu
    on f.id_funcao = fu.id_funcao
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_subfuncao") }} as sf
    on f.id_subfuncao = sf.id_subfuncao
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_programa") }} as pr
    on f.id_programa = pr.id_programa
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_acao") }} as ac
    on f.id_acao = ac.id_acao
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_categoria") }} as ce
    on f.id_categ_econ = ce.id_categ_econ
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_grupo") }} as gr
    on f.id_grupo = gr.id_grupo
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_modalidade_aplic") }}
    as ma
    on f.id_modalidade_aplic = ma.id_modalidade_aplic
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_elemento") }} as el
    on f.id_elemento = el.id_elemento
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_item") }} as it
    on f.id_item = it.id_item
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_fonte") }} as fo
    on f.id_fonte = fo.id_fonte
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_unidade_orc") }}
    as uo
    on f.id_unidade_orc = uo.id_unidade_orc
left join
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_tipo_documento") }}
    as td
    on f.id_tipo_documento = td.id_tipo_documento
