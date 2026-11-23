{{ config(materialized="ephemeral") }}

-- Minas Gerais annual procurement plan, item level, mapped onto the canonical
-- `plano_contratacao_item` schema.
--
-- Source: `portal_plano_anual_contratacao` (github.com/transparencia-mg), 35 files,
-- 1.4 GB, 1,623,629 rows, plan years 2023-2026. Nothing to union: no other state
-- publishes an ex-ante plan, and the `compras_contratos` dimensional model does not
-- either.
--
-- WHAT THIS ADDS, and why it is the most unusual table here. Everything else in this
-- dataset is ex-post -- what was tendered, contracted, invoiced, paid. This is ex-ante:
-- what the state SAID it would buy, item by item, with expected quantity, expected unit
-- price, expected delivery year and municipality, before buying it. It makes
-- planned-versus-executed measurable, and the initial plan works as a pre-registration
-- baseline against which later discretion can be measured. Join to `contrato_item`,
-- `licitacao_item` or `nota_fiscal_item` on `codigo_catalogo`, and to the process it
-- became via `numero_processo`.
--
-- THE FILENAME NUMBER IS A FILE PART, NOT A REVISION. `pac_2026_revisao7.csv` is the
-- seventh ~50,000-row page of an export, not a seventh revision. Pages are capped at
-- 50,000 rows (49,999 / 50,000 / 50,001 observed), and a single vintage spans as many
-- pages as it needs.
--
-- THE VINTAGE IS A COLUMN, AND THE SOURCE'S LABELS CONTRADICT EACH OTHER. `base`
-- carries
-- 'inicial', 'revisão' or '2ª revisão'; `dado` carries 'INICIAL' or '1 REVISAO' and
-- exists only in four 2026 files. For 2023-2025 the two agree and each year has exactly
-- two clean vintages. 2026 does not: 98,544 rows are labelled `base = 'revisão'` and
-- `dado = 'INICIAL'` at once, and 109,511 more have no `base` at all. There are eleven
-- distinct snapshots in total, five of them in 2026.
--
-- So NO normalised revision ordinal is published here. Constructing one would mean
-- choosing which of two contradictory source labels to believe, silently, on 200,000
-- rows. Instead `versao` and `versao_detalhe` carry `base` and `dado` verbatim, exactly
-- as published, and the table description says the 2026 labels conflict. A user
-- comparing initial against revised for 2023-2025 can rely on `versao`; for 2026 they
-- have to look at both columns and decide. That is the true state of the source.
--
-- GRAIN, verified: (ano_base, plano, item, base, dado) is UNIQUE -- 1,623,629 distinct
-- over 1,623,629 rows -- and every one of the eleven snapshots is internally unique on
-- (plano, item). The surrogate key is built from exactly those five parts.
--
-- MUNICIPALITY. The source publishes a SIX-digit IBGE code (`310620`), the standard
-- code
-- without its check digit, which is the established `id_municipio_6` shape in this
-- repository. It is joined to the production municipality directory to publish a proper
-- seven-digit `id_municipio`; the 6-digit prefix is 1:1 with the 7-digit code across
-- all
-- 5,571 municipalities, so the derivation is exact rather than approximate. The source
-- also uses `14` / 'MINAS GERAIS' as a state-wide sentinel on 156,256 rows; that does
-- not
-- match a municipality and correctly yields a NULL `id_municipio`, with
-- `nome_municipio`
-- still carrying the published label. This is the first municipality column in a
-- dataset
-- that is otherwise state-level throughout.
--
-- IMPLAUSIBLE PLANNED VALUES, passed through deliberately. `valor_total_previsto` sums
-- to R$90.4 TRILLION across the table, which is driven entirely by 42 rows above R$1bn,
-- the largest at R$31.98tn on a single planned item. Excluding those 42 the total is
-- R$169.1bn, which is the plausible figure for four years of state procurement
-- planning;
-- the median planned item is R$552.50 and the 99th percentile R$915,308.16.
--
-- The outliers are not nulled. Staging mirrors the source, and silently editing a
-- published value is worse than a documented outlier -- the same decision, for the same
-- reason, as the R$81.76tn `vr_referencia` row documented in `licitacao_mg.sql`. Anyone
-- aggregating `valor_total_previsto` or `valor_unitario_previsto` must apply an upper
-- bound; R$1bn removes all 42.
--
-- `item_despesa` IS NOT THE SAME CODE SPACE AS `dicionario`. The flat export's
-- `codigo_elemento_item_despesa` (168-181 four-digit codes such as 3010 = MATERIAL
-- MEDICO E HOSPITALAR) does not overlap the MG `item_despesa` keys the dicionario
-- carries from `mg_dm_item` -- 0 of 181 match. Do not join them. The label travels with
-- the row in `nome_item_despesa`, so these tables are self-describing and need no
-- dictionary entry for it. `fonte_recurso`, by contrast, IS the dicionario's code space
-- (56 of 57 codes match), which is why it carries the canonical name.
--
-- Values are comma-decimal with no thousands separator. Empty dates are published as
-- ' - ', which `safe.parse_date` nulls.
--
-- Every state model must project the canonical columns in THIS order: the union in the
-- parent resolves positionally, so a reordered or missing column silently shifts values
-- into the wrong field.
with
    base_rows as (
        select
            safe_cast(ano_base_planejamento_de_solicitacoes as int64) as ano,
            nullif(trim(`base`), '') as versao,
            nullif(trim(`dado`), '') as versao_detalhe,
            nullif(
                trim(numero_formatado_do_planejamento_de_solicitacoes), ''
            ) as numero_plano,
            nullif(trim(no_do_item_de_planejamento_de_solicitacoes), '') as numero_item,
            nullif(trim(situacao), '') as situacao,
            nullif(trim(item_de_planejamento_ativo), '') as item_ativo,
            nullif(trim(centralizado), '') as centralizado,
            nullif(
                trim(codigo_do_orgao_do_planejamento_de_solicitacoes), ''
            ) as id_orgao,
            nullif(
                trim(nome_do_orgao_do_planejamento_de_solicitacoes), ''
            ) as nome_orgao,
            nullif(
                trim(codigo_da_unidade_do_planejamento_de_solicitacoes), ''
            ) as id_unidade,
            nullif(
                trim(nome_da_unidade_do_planejamento_de_solicitacoes), ''
            ) as nome_unidade,
            nullif(trim(codigo_do_item_de_material_ou_servico), '') as codigo_catalogo,
            nullif(trim(desc_do_item_de_material_ou_servico), '') as descricao,
            nullif(trim(codigo_do_material_ou_servico), '') as codigo_material_servico,
            nullif(trim(nome_do_material_ou_servico), '') as nome_material_servico,
            nullif(trim(codigo_do_grupo_material_ou_servico), '') as codigo_grupo,
            nullif(trim(nome_do_grupo_material_ou_servico), '') as nome_grupo,
            nullif(trim(codigo_da_classe_material_ou_servico), '') as codigo_classe,
            nullif(trim(nome_da_classe_material_ou_servico), '') as nome_classe,
            nullif(trim(elemento_item_de_despesa), '') as item_despesa,
            nullif(trim(unidade_de_aquisicao), '') as unidade_aquisicao,
            nullif(
                trim(descricao_da_unidade_de_aquisicao), ''
            ) as descricao_unidade_aquisicao,
            safe_cast(replace(quantidade, ',', '.') as float64) as quantidade,
            safe_cast(
                replace(valor_unitario_previsto_r, ',', '.') as float64
            ) as valor_unitario_previsto,
            safe_cast(
                replace(valor_total_previsto_r, ',', '.') as float64
            ) as valor_total_previsto,
            safe_cast(ano_de_inicio_execucao_entrega as int64) as ano_inicio_execucao,
            nullif(trim(municipio), '') as id_municipio_6,
            nullif(trim(nome_do_municipio), '') as nome_municipio,
            nullif(trim(unidade_orcamentaria), '') as unidade_orcamentaria,
            nullif(trim(projeto_atividade), '') as projeto_atividade,
            nullif(trim(fonte), '') as fonte_recurso,
            nullif(trim(procedencia), '') as procedencia,
            nullif(
                trim(numero_formatado_do_planejamento_de_processo), ''
            ) as numero_processo,
            nullif(trim(procedimento_de_contratacao), '') as procedimento_contratacao,
            nullif(trim(objeto), '') as objeto,
            safe.parse_date(
                '%Y-%m-%d',
                substr(trim(data_de_criacao_planejamento_de_solicitacoes), 1, 10)
            ) as data_criacao_plano,
            safe.parse_date(
                '%Y-%m-%d',
                substr(
                    trim(data_aprovacao_reprovacao_do_planejamento_de_solicitacoes),
                    1,
                    10
                )
            ) as data_aprovacao_plano,
            safe.parse_date(
                '%Y-%m-%d',
                substr(
                    trim(data_de_criacao_do_planejamento_de_processo_de_compra), 1, 10
                )
            ) as data_criacao_processo,
            safe.parse_date(
                '%Y-%m-%d', substr(trim(data_conclusao_do_processo_de_compra), 1, 10)
            ) as data_conclusao_processo,
            nullif(trim(justificativa_desativacao), '') as justificativa_desativacao
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.mg_plano_contratacao_item"
                )
            }}
    )
select
    b.ano,
    'MG' as sigla_uf,
    concat(
        'MG-',
        coalesce(safe_cast(b.ano as string), ''),
        '-',
        coalesce(b.numero_plano, ''),
        '-',
        coalesce(b.numero_item, ''),
        '-',
        coalesce(b.versao, 'SEMBASE'),
        '-',
        coalesce(b.versao_detalhe, 'SEMDADO')
    ) as id_plano_item_bd,
    b.versao,
    b.versao_detalhe,
    b.numero_plano,
    b.numero_item,
    b.situacao,
    b.item_ativo,
    b.centralizado,
    b.id_orgao,
    b.nome_orgao,
    b.id_unidade,
    b.nome_unidade,
    b.codigo_catalogo,
    b.descricao,
    b.codigo_material_servico,
    b.nome_material_servico,
    b.codigo_grupo,
    b.nome_grupo,
    b.codigo_classe,
    b.nome_classe,
    b.item_despesa,
    b.unidade_aquisicao,
    b.descricao_unidade_aquisicao,
    b.quantidade,
    b.valor_unitario_previsto,
    b.valor_total_previsto,
    b.ano_inicio_execucao,
    -- Seven-digit IBGE code, derived from the source's six-digit code through the
    -- production directory. NULL for the state-wide `14` sentinel, by design.
    m.id_municipio,
    b.nome_municipio,
    b.unidade_orcamentaria,
    b.projeto_atividade,
    b.fonte_recurso,
    b.procedencia,
    b.numero_processo,
    b.procedimento_contratacao,
    b.objeto,
    b.data_criacao_plano,
    b.data_aprovacao_plano,
    b.data_criacao_processo,
    b.data_conclusao_processo,
    b.justificativa_desativacao
from base_rows as b
left join
    `basedosdados.br_bd_diretorios_brasil.municipio` as m
    on b.id_municipio_6 = m.id_municipio_6
