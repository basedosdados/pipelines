-- Tocantins (TO) contribution to world_wb_mides.empenho.
--
-- Source: world_wb_mides_staging.raw_empenho_to, 2013-2022, all 139 TO
-- municipalities. TCE-TO publishes a *movement ledger*: one row per movement on
-- an empenho, signed by `sinal` ('+' issue, '-' anulacao). The MiDES empenho
-- schema wants one row per empenho line with the movements folded into
-- valor_inicial / valor_anulacao / valor_final, so the movements are aggregated
-- to the line key (municipio, orgao, nr_empenho, rubrica).
--
-- The line key is the right grain, not (municipio, nr_empenho): of the 238,192
-- empenho numbers carrying more than one '+' row, 228,729 (96%) differ in
-- `rubrica`, i.e. they are separate budget lines of one empenho rather than
-- reforcos. Aggregating on the empenho number alone would merge distinct lines
-- and attribute each line's anulacao to all of them.
--
-- `indicador_restos_pagar` is null by necessity: nr_empenho always carries the
-- same year as `exercicio` in every one of the 5,418,514 source rows, so a
-- prior-year empenho is not identifiable here.
with
    movimento_to as (
        select
            safe_cast(exercicio as int64) as ano,
            parse_date('%d/%m/%Y', trim(data)) as data,
            safe_cast(municipio as string) as id_municipio,
            safe_cast(trim(orgao) as string) as orgao,
            safe_cast(
                trim(split(unidade_gestora, ' - ')[safe_offset(0)]) as string
            ) as id_unidade_gestora,
            safe_cast(nr_empenho as string) as numero,
            safe_cast(trim(rubrica) as string) as rubrica,
            safe_cast(lower(historico) as string) as descricao,
            safe_cast(safe_cast(funcao as int64) as string) as funcao,
            safe_cast(safe_cast(subfuncao as int64) as string) as subfuncao,
            safe_cast(safe_cast(programa as int64) as string) as programa,
            safe_cast(safe_cast(proj_atividade as int64) as string) as acao,
            -- TCE-TO's own modalidade codes do NOT match the MiDES dictionary
            -- (TO 1 = Dispensa, MiDES 1 = Convite), so they are remapped by
            -- description onto the codes in world_wb_mides.dicionario. TO's
            -- "Registro de Preco" is a procurement procedure, not a modality,
            -- and has no MiDES equivalent: it becomes 98 (Processo licitatorio),
            -- the generic "a tender happened" code, rather than 99 (Outros/Nao
            -- aplicavel), which would assert no tender took place. Where TO
            -- qualifies a pregao as "- Registro de Preco", the pregao modality
            -- is kept and the procedure dropped.
            case
                trim(modalidade_licitacao)
                when '1'
                then '8'  -- Dispensa
                when '2'
                then '10'  -- Inexigibilidade
                when '3'
                then '1'  -- Convite
                when '4'
                then '2'  -- Tomada de Precos
                when '5'
                then '3'  -- Concorrencia
                when '6'
                then '98'  -- Registro de Preco -> Processo licitatorio
                when '7'
                then '5'  -- Pregao Presencial
                when '8'
                then '6'  -- Pregao Eletronico
                when '9'
                then '5'  -- Pregao Presencial - Registro de Preco
                when '10'
                then '6'  -- Pregao Eletronico - Registro de Preco
                when '11'
                then '12'  -- RDC
                when '12'
                then '31'  -- Chamamento/Credenciamento -> Chamada Publica
                when '99'
                then '99'  -- Nao aplicado
            end as modalidade_licitacao,
            trim(sinal) as sinal,
            safe_cast(valor as float64) as valor
        from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_to") }}
    ),
    linha_to as (
        select
            ano,
            id_municipio,
            orgao,
            numero,
            rubrica,
            min(data) as data,
            any_value(id_unidade_gestora) as id_unidade_gestora,
            any_value(descricao) as descricao,
            any_value(funcao) as funcao,
            any_value(subfuncao) as subfuncao,
            any_value(programa) as programa,
            any_value(acao) as acao,
            any_value(modalidade_licitacao) as modalidade_licitacao,
            round(sum(if(sinal = '+', valor, 0)), 2) as valor_inicial,
            round(sum(if(sinal = '-', valor, 0)), 2) as valor_anulacao
        from movimento_to
        group by ano, id_municipio, orgao, numero, rubrica
    ),
    empenhado_to as (
        select
            ano,
            extract(month from data) as mes,
            data,
            'TO' as sigla_uf,
            id_municipio,
            orgao,
            id_unidade_gestora,
            safe_cast(null as string) as id_licitacao_bd,
            safe_cast(null as string) as id_licitacao,
            modalidade_licitacao,
            safe_cast(
                concat(
                    numero,
                    ' ',
                    orgao,
                    ' ',
                    id_municipio,
                    ' ',
                    right(cast(ano as string), 2)
                ) as string
            ) as id_empenho_bd,
            safe_cast(null as string) as id_empenho,
            numero,
            descricao,
            safe_cast(null as string) as modalidade,
            funcao,
            subfuncao,
            programa,
            acao,
            safe_cast(substr(rubrica, 1, 8) as string) as elemento_despesa,
            valor_inicial,
            valor_anulacao
        from linha_to
    ),
    frequencia_to as (
        select id_empenho_bd, count(id_empenho_bd) as frequencia_id
        from empenhado_to
        group by 1
    ),
    empenho_to as (
        select
            e.ano,
            e.mes,
            e.data,
            e.sigla_uf,
            e.id_municipio,
            e.orgao,
            e.id_unidade_gestora,
            e.id_licitacao_bd,
            e.id_licitacao,
            e.modalidade_licitacao,
            -- house convention: an empenho spanning several budget lines has an
            -- ambiguous key, so it is withheld rather than published wrong.
            (
                case
                    when f.frequencia_id > 1
                    then safe_cast(null as string)
                    else e.id_empenho_bd
                end
            ) as id_empenho_bd,
            e.id_empenho,
            e.numero,
            e.descricao,
            e.modalidade,
            e.funcao,
            e.subfuncao,
            e.programa,
            e.acao,
            e.elemento_despesa,
            round(e.valor_inicial, 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_reforco,
            round(e.valor_anulacao, 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(e.valor_inicial - e.valor_anulacao, 2) as valor_final
        from empenhado_to e
        left join frequencia_to f on e.id_empenho_bd = f.id_empenho_bd
    )
select *
from empenho_to
