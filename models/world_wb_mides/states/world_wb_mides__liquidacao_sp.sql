-- São Paulo (SP) contribution to world_wb_mides.liquidacao.
-- Split out of the monolithic liquidacao model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    liquidado_sp as (
        select
            safe_cast(ano_exercicio as int64) as ano,
            safe_cast(mes_referencia as int64) as mes,
            safe_cast(
                concat(
                    substring(dt_emissao_despesa, -4),
                    '-',
                    substring(dt_emissao_despesa, -7, 2),
                    '-',
                    substring(dt_emissao_despesa, 1, 2)
                ) as date
            ) as data,
            'SP' as sigla_uf,
            safe_cast(id_municipio as string) as id_municipio,
            safe_cast(codigo_orgao as string) as orgao,
            safe_cast(null as string) as id_unidade_gestora,
            safe_cast(
                concat(
                    left(nr_empenho, length(nr_empenho) - 5),
                    ' ',
                    codigo_orgao,
                    ' ',
                    id_municipio,
                    ' ',
                    (right(ano_exercicio, 2))
                ) as string
            ) as id_empenho_bd,
            safe_cast(null as string) as id_empenho,
            safe_cast(nr_empenho as string) as numero_empenho,
            safe_cast(
                concat(
                    left(nr_empenho, length(nr_empenho) - 5),
                    ' ',
                    regexp_replace(identificador_despesa, '[^0-9]', ''),
                    ' ',
                    codigo_orgao,
                    ' ',
                    id_municipio,
                    ' ',
                    (right(ano_exercicio, 2))
                ) as string
            ) as id_liquidacao_bd,
            safe_cast(null as string) as id_liquidacao,
            safe_cast(null as string) as numero,
            safe_cast(null as string) as nome_responsavel,
            safe_cast(null as string) as documento_responsavel,
            safe_cast(null as bool) as indicador_restos_pagar,
            safe_cast(nr_empenho as string) as numero,
            case
                when ds_modalidade_lic = 'CONVITE'
                then '1'
                when ds_modalidade_lic = 'TOMADA DE PREÇOS'
                then '2'
                when ds_modalidade_lic = 'CONCORRÊNCIA'
                then '3'
                when ds_modalidade_lic = 'PREGÃO'
                then '4'
                when ds_modalidade_lic = 'Leilão'
                then '7'
                when ds_modalidade_lic = 'DISPENSA DE LICITAÇÃO'
                then '8'
                when ds_modalidade_lic = 'BEC-BOLSA ELETRÔNICA DE COMPRAS'
                then '9'
                when ds_modalidade_lic = 'INEXIGÍVEL'
                then '10'
                when ds_modalidade_lic = 'CONCURSO'
                then '11'
                when ds_modalidade_lic = 'RDC'
                then '12'
                when ds_modalidade_lic = 'OUTROS/NÃO APLICÁVEL'
                then '99'
            end as modalidade_licitacao,
            safe_cast(lower(historico_despesa) as string) as descricao,
            safe_cast(null as string) as modalidade,
            safe_cast(funcao as string) as funcao,
            safe_cast(subfuncao as string) as subfuncao,
            safe_cast(cd_programa as string) as programa,
            safe_cast(cd_acao as string) as acao,
            safe_cast((left(ds_elemento, 8)) as string) as elemento_despesa,
            safe_cast(replace(vl_despesa, ',', '.') as float64) as valor_inicial
        from {{ set_datalake_project("world_wb_mides_staging.raw_despesa_sp") }} e
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_municipio_sp") }} m
            on m.ds_orgao = e.ds_orgao
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_funcao") }}
            on ds_funcao_governo = upper(nome_funcao)
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_subfuncao") }}
            on ds_subfuncao_governo = upper(nome_subfuncao)
        where tp_despesa = 'Valor Liquidado'

    ),
    dorgao as (
        select
            id_empenho_bd,
            case when (count(distinct orgao)) > 1 then 1 else 0 end as dorgao
        from liquidado_sp
        group by 1

    ),
    ddesc as (
        select
            id_empenho_bd,
            case
                when (count(distinct ifnull(descricao, ''))) > 1 then 1 else 0
            end as ddesc
        from liquidado_sp
        group by 1

    ),
    dmod as (
        select
            id_empenho_bd,
            case
                when (count(distinct modalidade_licitacao)) > 1 then 1 else 0
            end as dmod
        from liquidado_sp
        group by 1

    ),
    dfun as (
        select
            id_empenho_bd,
            case when (count(distinct funcao)) > 1 then 1 else 0 end as dfun
        from liquidado_sp
        group by 1

    ),
    dsubf as (
        select
            id_empenho_bd,
            case when (count(distinct subfuncao)) > 1 then 1 else 0 end as dsubf
        from liquidado_sp
        group by 1

    ),
    dprog as (
        select
            id_empenho_bd,
            case when (count(distinct programa)) > 1 then 1 else 0 end as dprog
        from liquidado_sp
        group by 1

    ),
    dacao as (
        select
            id_empenho_bd,
            case when (count(distinct acao)) > 1 then 1 else 0 end as dacao
        from liquidado_sp
        group by 1

    ),
    delem as (
        select
            id_empenho_bd,
            case when (count(distinct elemento_despesa)) > 1 then 1 else 0 end as delem
        from liquidado_sp
        group by 1

    ),
    dummies as (
        select o.id_empenho_bd, dorgao, dmod, ddesc, dfun, dsubf, dprog, dacao, delem
        from dorgao o
        full outer join dmod m on o.id_empenho_bd = m.id_empenho_bd
        full outer join ddesc d on o.id_empenho_bd = d.id_empenho_bd
        full outer join dfun f on o.id_empenho_bd = f.id_empenho_bd
        full outer join dsubf s on o.id_empenho_bd = s.id_empenho_bd
        full outer join dprog p on o.id_empenho_bd = p.id_empenho_bd
        full outer join dacao a on o.id_empenho_bd = a.id_empenho_bd
        full outer join delem e on o.id_empenho_bd = e.id_empenho_bd

    ),
    liquidacao_sp as (
        select
            min(ano) as ano,
            min(mes) as mes,
            min(data) as data,
            sigla_uf,
            id_municipio,
            orgao,
            id_unidade_gestora,
            (
                case
                    when
                        (
                            dorgao = 1
                            or dmod = 1
                            or dfun = 1
                            or dsubf = 1
                            or dprog = 1
                            or dacao = 1
                            or delem = 1
                        )
                    then (safe_cast(null as string))
                    else l.id_empenho_bd
                end
            ) as id_empenho_bd,
            id_empenho,
            numero_empenho,
            id_liquidacao_bd,
            id_liquidacao,
            safe_cast(null as string) as numero,
            nome_responsavel,
            documento_responsavel,
            indicador_restos_pagar,
            round(sum(valor_inicial), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(sum(valor_inicial), 2) as valor_final
        from liquidado_sp l
        left join dummies d on d.id_empenho_bd = l.id_empenho_bd
        group by 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16

    ),
    liquidacao_municipio_sp as (
        select
            (safe_cast(exercicio as int64)) as ano,
            (safe_cast(extract(month from date(data_empenho)) as int64)) as mes,
            safe_cast(data_empenho as date) as data,
            'SP' as sigla_uf,
            '3550308' as id_municipio,
            safe_cast(codigo_orgao as string) as orgao,
            safe_cast(codigo_unidade as string) as id_unidade_gestora,
            safe_cast(
                concat(
                    nr_empenho,
                    ' ',
                    trim(codigo_orgao),
                    ' ',
                    trim(codigo_unidade),
                    ' ',
                    '3550308',
                    ' ',
                    (right(exercicio, 2))
                ) as string
            ) as id_empenho_bd,
            safe_cast(id_empenho as string) as id_empenho,
            safe_cast(nr_empenho as string) as numero_empenho,
            safe_cast(null as string) as id_liquidacao_bd,
            safe_cast(null as string) as id_liquidacao,
            safe_cast(null as string) as numero,
            safe_cast(null as string) as nome_responsavel,
            safe_cast(null as string) as documento_responsavel,
            safe_cast(null as bool) as indicador_restos_pagar,
            round(safe_cast(liquidado as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(safe_cast(liquidado as float64), 2) as valor_final
        from
            {{
                set_datalake_project(
                    "world_wb_mides_staging.raw_despesa_sp_municipio"
                )
            }}

    )
select *
from liquidacao_sp
union all
(select * from liquidacao_municipio_sp)
