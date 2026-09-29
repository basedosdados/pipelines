-- Ceará (CE) contribution to world_wb_mides.empenho.
-- Split out of the monolithic empenho model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    empenhado_ce as (
        select
            (safe_cast(extract(year from date(data_emissao_empenho)) as int64)) as ano,
            (safe_cast(extract(month from date(data_emissao_empenho)) as int64)) as mes,
            safe_cast(
                extract(date from timestamp(data_emissao_empenho)) as date
            ) as data,
            'CE' as sigla_uf,
            safe_cast(geoibgeid as string) as id_municipio,
            safe_cast(codigo_orgao as string) as orgao,
            safe_cast(trim(codigo_unidade) as string) as id_unidade_gestora,
            safe_cast(null as string) as id_licitacao_bd,
            safe_cast(numero_licitacao as string) as id_licitacao,
            case
                when tipo_processo_licitatorio = 'N'
                then '98'
                when tipo_processo_licitatorio = 'R'
                then '2'
                when tipo_processo_licitatorio = 'D'
                then '8'
                when tipo_processo_licitatorio = 'I'
                then '10'
                when tipo_processo_licitatorio = 'R'
                then '29'
            end as modalidade_licitacao,
            safe_cast(
                concat(
                    numero_empenho,
                    ' ',
                    trim(codigo_orgao),
                    ' ',
                    trim(codigo_unidade),
                    ' ',
                    geoibgeid,
                    ' ',
                    (substring(data_emissao_empenho, 6, 2)),
                    ' ',
                    (substring(data_emissao_empenho, 3, 2))
                ) as string
            ) as id_empenho_bd,
            safe_cast(null as string) as id_empenho,
            safe_cast(numero_empenho as string) as numero,
            safe_cast(lower(descricao_empenho) as string) as descricao,
            safe_cast(modalidade_empenho as string) as modalidade,
            safe_cast(safe_cast(codigo_funcao as int64) as string) as funcao,
            safe_cast(safe_cast(codigo_subfuncao as int64) as string) as subfuncao,
            safe_cast(safe_cast(codigo_programa as int64) as string) as programa,
            safe_cast(safe_cast(codigo_projeto_atividade as int64) as string) as acao,
            safe_cast(
                safe_cast(codigo_elemento_despesa as int64) as string
            ) as modalidade_despesa,
            round(safe_cast(valor_empenhado as float64), 2) as valor_inicial,
        from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_ce") }} e

    ),
    anulacao_ce as (
        select
            safe_cast(
                concat(
                    numero_empenho,
                    ' ',
                    trim(codigo_orgao),
                    ' ',
                    trim(codigo_unidade),
                    ' ',
                    geoibgeid,
                    ' ',
                    (substring(data_emissao_empenho, 6, 2)),
                    ' ',
                    (substring(data_emissao_empenho, 3, 2))
                ) as string
            ) as id_empenho_bd,
            round(sum(safe_cast(valor_anulacao as float64)), 2) as valor_anulacao
        from {{ set_datalake_project("world_wb_mides_staging.raw_anulacao_ce") }}
        group by 1

    ),
    frequencia_ce as (
        select id_empenho_bd, count(id_empenho_bd) as frequencia_id
        from empenhado_ce
        group by 1
        order by 2 desc

    ),
    empenho_ce as (
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
            (
                case
                    when frequencia_id > 1
                    then (safe_cast(null as string))
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
            e.modalidade_despesa,
            round(e.valor_inicial, 2),
            round(safe_cast(0 as float64), 2) as valor_reforco,
            round(a.valor_anulacao, 2),
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(
                ifnull(e.valor_inicial, 0) - ifnull(a.valor_anulacao, 0), 2
            ) as valor_final
        from empenhado_ce e
        left join frequencia_ce f on e.id_empenho_bd = f.id_empenho_bd
        full outer join anulacao_ce a on a.id_empenho_bd = e.id_empenho_bd

    )
select *
from empenho_ce
