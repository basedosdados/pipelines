-- Minas Gerais (MG) contribution to world_wb_mides.licitacao.
--
-- Ported from `code/licitacao.ipynb`, the original ingest that produced MG
-- 2014-2021. That notebook is authoritative for every mapping below; this model
-- reproduces it in SQL and extends the coverage to 2026. Two deliberate
-- departures, both recorded here:
--
-- 1. `orgao` is `cod_orgao`, not the notebook's `seq_orgao`. Sequence ids are
-- reassigned by TCE-MG between extractions, so seq values from this harvest
-- would not join to the 2014-2021 rows anyway. Matches empenho/liquidacao/
-- pagamento.
-- 2. `id_licitacao_bd` is the stable key, not the notebook's
-- `id_licitacao + id_unidade_gestora + 'MG'`, which is built from sequence
-- ids and churns. Validated to equal grain against `seq_licitacao` over 150
-- municipalities x 2016/2021/2024: 28,578 of 28,578.
--
-- Both mean MG's historical rows change. That is inherent in a rebuild and was
-- chosen deliberately; see ARCHITECTURE_MG.md.
--
-- The table carries BOTH competitive procurement (licitacao) and direct
-- contracting (dispensa), distinguished by `id_dispensa`, exactly as the
-- notebook built it -- 204,399 of MG's 643,442 published rows are dispensas.
-- `regadesao` is NOT included: the notebook opens it and never merges it.
--
-- Columns never populated for MG in the published table -- data_homologacao,
-- situacao, estagio, contratacao, carona, covid_19 -- stay NULL here, which is
-- continuity, not a gap.
with
    unidade_xwalk as (
        -- seq_unidade -> cod_unidade; the procurement streams publish only the
        -- sequence form. 9,319 pairs, 0 conflicts, 98.3% coverage.
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
    -- valor_orcamento: sum of the budget lines attached to the process.
    -- notebook: mg2 = recLicitacao grouped by municipio, orgao, ano, mes, licitacao.
    orcamento_licitacao as (
        select
            id_municipio,
            orgao,
            ano,
            mes,
            seq_licitacao,
            sum(safe_cast(valor_recurso as float64)) as valor_orcamento
        from
            {{
                set_datalake_project(
                    "world_wb_mides_staging.raw_licitacao_dotacao_mg"
                )
            }}
        group by 1, 2, 3, 4, 5
    ),
    orcamento_dispensa as (
        select
            id_municipio,
            orgao,
            ano,
            mes,
            seq_dispensa,
            sum(safe_cast(valor_recurso as float64)) as valor_orcamento
        from
            {{ set_datalake_project("world_wb_mides_staging.raw_dispensa_dotacao_mg") }}
        group by 1, 2, 3, 4, 5
    ),
    -- valor: quantity * unit price of the homologated items, summed per process.
    -- notebook: mg3 = homologLicitacao, mg6 = fornDispensa. Note the grouping key
    -- has NO `mes`, unlike the orcamento CTEs above -- that asymmetry is the
    -- notebook's and is preserved.
    valor_licitacao as (
        select
            id_municipio,
            orgao,
            ano,
            seq_licitacao,
            sum(
                safe_cast(num_quant_item as float64)
                * safe_cast(valor_unitario as float64)
            ) as valor
        from
            {{
                set_datalake_project(
                    "world_wb_mides_staging.raw_licitacao_homologacao_mg"
                )
            }}
        group by 1, 2, 3, 4
    ),
    valor_dispensa as (
        select
            id_municipio,
            orgao,
            ano,
            seq_dispensa,
            sum(
                safe_cast(num_quant_item as float64) * safe_cast(valor_item as float64)
            ) as valor
        from
            {{
                set_datalake_project(
                    "world_wb_mides_staging.raw_dispensa_fornecedor_mg"
                )
            }}
        group by 1, 2, 3, 4
    ),
    competitiva as (
        select
            safe_cast(t.ano as int64) as ano,
            safe_cast(t.mes as int64) as mes,
            'MG' as sigla_uf,
            safe_cast(t.id_municipio as string) as id_municipio,
            safe_cast(t.orgao as string) as orgao,
            safe_cast(t.id_unidade_gestora as string) as id_unidade_gestora,
            safe_cast(
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
                ) as string
            ) as id_licitacao_bd,
            safe_cast(t.seq_licitacao as string) as id_licitacao,
            safe_cast(null as string) as id_dispensa,
            safe_cast(t.num_ano_processo as int64) as ano_processo,
            safe_cast(t.data_abert_proc_adm as date) as data_abertura,
            safe_cast(t.data_pub_edital as date) as data_edital,
            safe_cast(null as date) as data_homologacao,
            safe_cast(null as date) as data_publicacao_dispensa,
            safe_cast(t.dsc_objeto_licitacao as string) as descricao_objeto,
            -- The notebook maps the full label to a code. The numeric prefix is
            -- the code in every case EXCEPT '4 - CONCURSO' -> 11, so the mapping
            -- is spelled out rather than parsed off the front of the string.
            case
                trim(t.dsc_nat_objeto)
                when '1 - OBRAS E SERVIÇOS DE ENGENHARIA'
                then '1'
                when '2 - COMPRAS E OUTROS SERVIÇOS'
                then '2'
                when '3 - LOCAÇÃO DE IMÓVEIS'
                then '3'
                when '3 - LOCAÇÃO DE IMOVÉIS'
                then '3'
                when '4 - CONCESSÃO'
                then '4'
                when '5 - PERMISSÃO'
                then '5'
                when '6 - ALIENAÇÃO DE BENS'
                then '6'
                when '7 - COMPRAS PARA OBRAS E/OU SERVIÇOS DE ENGENHARIA'
                then '7'
                else null
            end as natureza_objeto,
            case
                trim(t.dsc_modalidade)
                when '1 - CONVITE'
                then '1'
                when '2 - TOMADA DE PREÇOS'
                then '2'
                when '3 - CONCORRÊNCIA'
                then '3'
                when '4 - CONCURSO'
                then '11'
                when '5 - PREGÃO PRESENCIAL'
                then '5'
                when '6 - PREGÃO ELETRÔNICO'
                then '6'
                when '7 - LEILÃO'
                then '7'
                else null
            end as modalidade,
            case
                trim(t.dsc_nat_processo)
                when '1 - NORMAL'
                then '1'
                when '2 - REGISTRO DE PREÇOS'
                then '2'
                when '3 - CREDENCIAMENTO/ CHAMADA PÚBLICA'
                then '3'
                when '3 - CREDENCIAMENTO/CHAMADA PÚBLICA'
                then '3'
                else null
            end as natureza_processo,
            case
                trim(t.dsc_tipo_licitacao)
                when '1 - MENOR PREÇO'
                then '1'
                when '2 - MELHOR TÉCNICA'
                then '2'
                when '3 - TÉCNICA E PREÇO'
                then '3'
                when '4 - MAIOR LANCE OU OFERTA'
                then '4'
                else null
            end as tipo,
            safe_cast(t.dsc_forma_pagamento as string) as forma_pagamento,
            o.valor_orcamento,
            v.valor,
            -- notebook: the smaller of the two when both exist, else whichever does
            case
                when o.valor_orcamento is not null and v.valor is not null
                then least(o.valor_orcamento, v.valor)
                else coalesce(v.valor, o.valor_orcamento)
            end as valor_corrigido,
            safe_cast(null as string) as situacao,
            safe_cast(null as string) as estagio,
            case
                trim(t.dsc_ind_pref_micro)
                when 'SIM'
                then '1'
                when 'NÃO'
                then '0'
                else null
            end as preferencia_micro_pequena,
            case
                trim(t.dsc_ind_exclusiva_micro)
                when 'SIM'
                then '1'
                when 'NÃO'
                then '0'
                else null
            end as exclusiva_micro_pequena,
            safe_cast(null as string) as contratacao,
            safe_cast(t.num_convidados as int64) as quantidade_convidados,
            case
                trim(t.dsc_tipo_cadastro)
                when '1 - CADASTRO INICIAL'
                then '1'
                when '2 - RETIFICAÇÃO'
                then '2'
                else null
            end as tipo_cadastro,
            safe_cast(null as string) as carona,
            safe_cast(null as string) as covid_19
        from {{ set_datalake_project("world_wb_mides_staging.raw_licitacao_mg") }} as t
        left join
            unidade_xwalk as x
            on t.id_municipio = x.id_municipio
            and t.id_unidade_gestora = x.id_unidade_gestora
        left join
            orcamento_licitacao as o
            on t.id_municipio = o.id_municipio
            and t.orgao = o.orgao
            and t.ano = o.ano
            and t.mes = o.mes
            and t.seq_licitacao = o.seq_licitacao
        left join
            valor_licitacao as v
            on t.id_municipio = v.id_municipio
            and t.orgao = v.orgao
            and t.ano = v.ano
            and t.seq_licitacao = v.seq_licitacao
    ),
    nao_competitiva as (
        select
            safe_cast(t.ano as int64) as ano,
            safe_cast(t.mes as int64) as mes,
            'MG' as sigla_uf,
            safe_cast(t.id_municipio as string) as id_municipio,
            safe_cast(t.orgao as string) as orgao,
            safe_cast(t.id_unidade_gestora as string) as id_unidade_gestora,
            safe_cast(
                concat(
                    t.orgao,
                    ' ',
                    ifnull(x.cod_unidade, concat('u:', t.id_unidade_gestora)),
                    ' ',
                    ifnull(t.num_processo, ''),
                    ' ',
                    ifnull(t.num_ano_processo, ''),
                    ' ',
                    ifnull(t.data_abertura, ''),
                    ' ',
                    ifnull(t.dsc_tipo_processo, ''),
                    ' ',
                    t.id_municipio,
                    ' ',
                    t.ano
                ) as string
            ) as id_licitacao_bd,
            safe_cast(null as string) as id_licitacao,
            safe_cast(t.seq_dispensa as string) as id_dispensa,
            safe_cast(t.num_ano_processo as int64) as ano_processo,
            safe_cast(t.data_abertura as date) as data_abertura,
            safe_cast(null as date) as data_edital,
            safe_cast(null as date) as data_homologacao,
            safe_cast(t.data_pub_termo as date) as data_publicacao_dispensa,
            safe_cast(t.dsc_objeto as string) as descricao_objeto,
            case
                trim(t.dsc_nat_objeto)
                when '1 - OBRAS E SERVIÇOS DE ENGENHARIA'
                then '1'
                when '2 - COMPRAS E OUTROS SERVIÇOS'
                then '2'
                when '3 - LOCAÇÃO DE IMÓVEIS'
                then '3'
                when '3 - LOCAÇÃO DE IMOVÉIS'
                then '3'
                when '4 - CONCESSÃO'
                then '4'
                when '5 - PERMISSÃO'
                then '5'
                when '6 - ALIENAÇÃO DE BENS'
                then '6'
                when '7 - COMPRAS PARA OBRAS E/OU SERVIÇOS DE ENGENHARIA'
                then '7'
                else null
            end as natureza_objeto,
            -- For dispensas the notebook maps dsc_tipo_processo into `modalidade`,
            -- and the codes are NOT the numeric prefix: dispensa -> 8,
            -- inexigibilidade -> 10.
            case
                trim(t.dsc_tipo_processo)
                when '1 - DISPENSA'
                then '8'
                when '2 - INEXIGIBILIDADE'
                then '10'
                when '3 - INEXIGIBILIDADE POR CREDENCIAMENTO/CHAMADA PÚBLICA'
                then '10'
                when '4 - DISPENSA POR CHAMADA PÚBLICA'
                then '8'
                else null
            end as modalidade,
            safe_cast(null as string) as natureza_processo,
            safe_cast(null as string) as tipo,
            safe_cast(null as string) as forma_pagamento,
            o.valor_orcamento,
            v.valor,
            case
                when o.valor_orcamento is not null and v.valor is not null
                then least(o.valor_orcamento, v.valor)
                else coalesce(v.valor, o.valor_orcamento)
            end as valor_corrigido,
            safe_cast(null as string) as situacao,
            safe_cast(null as string) as estagio,
            safe_cast(null as string) as preferencia_micro_pequena,
            safe_cast(null as string) as exclusiva_micro_pequena,
            safe_cast(null as string) as contratacao,
            safe_cast(null as int64) as quantidade_convidados,
            case
                trim(t.dsc_tipo_cadastro)
                when '1 - CADASTRO INICIAL'
                then '1'
                when '2 - RETIFICAÇÃO'
                then '2'
                else null
            end as tipo_cadastro,
            safe_cast(null as string) as carona,
            safe_cast(null as string) as covid_19
        from {{ set_datalake_project("world_wb_mides_staging.raw_dispensa_mg") }} as t
        left join
            unidade_xwalk as x
            on t.id_municipio = x.id_municipio
            and t.id_unidade_gestora = x.id_unidade_gestora
        left join
            orcamento_dispensa as o
            on t.id_municipio = o.id_municipio
            and t.orgao = o.orgao
            and t.ano = o.ano
            and t.mes = o.mes
            and t.seq_dispensa = o.seq_dispensa
        left join
            valor_dispensa as v
            on t.id_municipio = v.id_municipio
            and t.orgao = v.orgao
            and t.ano = v.ano
            and t.seq_dispensa = v.seq_dispensa
    )
select *
from competitiva
union all
select *
from nao_competitiva
