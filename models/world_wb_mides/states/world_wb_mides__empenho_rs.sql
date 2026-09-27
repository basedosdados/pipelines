-- Rio Grande do Sul (RS) contribution to world_wb_mides.empenho.
-- Split out of the monolithic empenho model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    empenhado_rs as (
        select
            min(ano_recebimento) as ano_recebimento,
            safe_cast(ano_empenho as int64) as ano,
            safe_cast(extract(month from date(dt_operacao)) as int64) as mes,
            safe_cast(
                concat(
                    substring(dt_operacao, 1, 4),
                    '-',
                    substring(dt_operacao, 6, 2),
                    '-',
                    substring(dt_operacao, 9, 2)
                ) as date
            ) as data,
            'RS' as sigla_uf,
            safe_cast(a.id_municipio as string) as id_municipio,
            safe_cast(c.cd_orgao as string) as orgao,
            safe_cast(cd_orgao_orcamentario as string) as id_unidade_gestora,
            safe_cast(null as string) as id_licitacao_bd,
            safe_cast(null as string) as id_licitacao,
            safe_cast(null as string) as modalidade_licitacao,
            safe_cast(
                concat(
                    nr_empenho,
                    ' ',
                    c.cd_orgao,
                    ' ',
                    m.id_municipio,
                    ' ',
                    (right(ano_empenho, 2))
                ) as string
            ) as id_empenho_bd,
            safe_cast(null as string) as id_empenho,
            safe_cast(nr_empenho as string) as numero,
            safe_cast(lower(historico) as string) as descricao,
            safe_cast(null as string) as modalidade,
            safe_cast(safe_cast(cd_funcao as int64) as string) as funcao,
            safe_cast(safe_cast(cd_subfuncao as int64) as string) as subfuncao,
            safe_cast(safe_cast(cd_programa as int64) as string) as programa,
            safe_cast(safe_cast(cd_projeto as int64) as string) as acao,
            safe_cast(replace(cd_elemento, '.', '') as string) as elemento_despesa,
            safe_cast(vl_empenho as float64) as valor_inicial
        from {{ set_datalake_project("world_wb_mides_staging.raw_despesa_rs") }} as c
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_orgao_rs") }} as a
            on c.cd_orgao = a.cd_orgao
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` m
            on m.id_municipio = a.id_municipio
        where tipo_operacao = 'E' and (safe_cast(vl_empenho as float64) >= 0)
        group by
            2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22

    ),
    frequencia_rs as (
        select id_empenho_bd, count(id_empenho_bd) as frequencia_id
        from empenhado_rs
        group by 1

    ),
    anulacao_rs as (
        select
            safe_cast(
                concat(
                    nr_empenho,
                    ' ',
                    c.cd_orgao,
                    ' ',
                    m.id_municipio,
                    ' ',
                    (right(ano_empenho, 2))
                ) as string
            ) as id_empenho_bd,
            -1 * sum(safe_cast(vl_empenho as float64)) as valor_anulacao
        from {{ set_datalake_project("world_wb_mides_staging.raw_despesa_rs") }} as c
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_orgao_rs") }} as a
            on c.cd_orgao = a.cd_orgao
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` m
            on m.id_municipio = a.id_municipio
        where tipo_operacao = 'E' and (safe_cast(vl_empenho as float64) < 0)
        group by 1

    ),
    empenho_anulacao as (
        select
            e.*, f.frequencia_id, a.valor_anulacao / f.frequencia_id as valor_anulacao
        from empenhado_rs e
        left join anulacao_rs a on e.id_empenho_bd = a.id_empenho_bd
        left join frequencia_rs f on e.id_empenho_bd = f.id_empenho_bd

    ),
    dorgao as (
        select
            id_empenho_bd,
            case when (count(distinct orgao)) > 1 then 1 else 0 end as dorgao
        from empenho_anulacao
        group by 1

    ),
    dugest as (
        select
            id_empenho_bd,
            case
                when (count(distinct id_unidade_gestora)) > 1 then 1 else 0
            end as dugest
        from empenho_anulacao
        group by 1

    ),
    ddesc as (
        select
            id_empenho_bd,
            case when (count(distinct descricao)) > 1 then 1 else 0 end as ddesc
        from empenho_anulacao
        group by 1

    ),
    dfun as (
        select
            id_empenho_bd,
            case when (count(distinct funcao)) > 1 then 1 else 0 end as dfun
        from empenho_anulacao
        group by 1

    ),
    dsubf as (
        select
            id_empenho_bd,
            case when (count(distinct subfuncao)) > 1 then 1 else 0 end as dsubf
        from empenho_anulacao
        group by 1

    ),
    dprog as (
        select
            id_empenho_bd,
            case when (count(distinct programa)) > 1 then 1 else 0 end as dprog
        from empenho_anulacao
        group by 1

    ),
    dacao as (
        select
            id_empenho_bd,
            case when (count(distinct acao)) > 1 then 1 else 0 end as dacao
        from empenho_anulacao
        group by 1

    ),
    delem as (
        select
            id_empenho_bd,
            case when (count(distinct elemento_despesa)) > 1 then 1 else 0 end as delem
        from empenho_anulacao
        group by 1

    ),
    dummies as (
        select o.id_empenho_bd, dorgao, dugest, ddesc, dfun, dsubf, dprog, dacao, delem
        from dorgao o
        left join dugest g on o.id_empenho_bd = g.id_empenho_bd
        left join ddesc d on o.id_empenho_bd = d.id_empenho_bd
        left join dfun f on o.id_empenho_bd = f.id_empenho_bd
        left join dsubf s on o.id_empenho_bd = s.id_empenho_bd
        left join dprog p on o.id_empenho_bd = p.id_empenho_bd
        left join dacao a on o.id_empenho_bd = a.id_empenho_bd
        left join delem e on o.id_empenho_bd = e.id_empenho_bd

    ),
    empenho_rs as (
        select
            min(e.ano) as ano,
            min(e.mes) as mes,
            min(e.data) as data,
            e.sigla_uf,
            e.id_municipio,
            e.orgao,
            e.id_unidade_gestora,
            e.id_licitacao_bd,
            e.id_licitacao,
            e.modalidade_licitacao,
            (
                case
                    when
                        (
                            dorgao = 1
                            or dugest = 1
                            or dfun = 1
                            or dsubf = 1
                            or dprog = 1
                            or dacao = 1
                            or delem = 1
                        )
                    then (safe_cast(null as string))
                    else e.id_empenho_bd
                end
            ) as id_empenho_bd,
            e.id_empenho,
            e.numero,
            (
                case
                    when
                        (
                            ddesc = 1
                            and (
                                dorgao = 0
                                or dugest = 0
                                or dfun = 0
                                or dsubf = 0
                                or dprog = 0
                                or dacao = 0
                                or delem = 0
                            )
                        )
                    then (safe_cast(null as string))
                    when
                        (
                            ddesc = 1
                            and (
                                dorgao = 1
                                or dugest = 1
                                or dfun = 1
                                or dsubf = 1
                                or dprog = 1
                                or dacao = 1
                                or delem = 1
                            )
                        )
                    then (safe_cast(e.descricao as string))
                    else e.descricao
                end
            ) as descricao,
            e.modalidade,
            e.funcao,
            e.subfuncao,
            e.programa,
            e.acao,
            e.elemento_despesa,
            round(sum(e.valor_inicial), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_reforco,
            round(sum(e.valor_anulacao), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(
                sum(e.valor_inicial) - ifnull(sum(e.valor_anulacao), 0), 2
            ) as valor_final
        from empenho_anulacao e
        left join dummies d on e.id_empenho_bd = d.id_empenho_bd
        group by 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20

    )
select *
from empenho_rs
