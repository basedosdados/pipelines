-- Rio de Janeiro (RJ) contribution to world_wb_mides.liquidacao.
-- Split out of the monolithic liquidacao model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    liquidado_municipio_rj_v1 as (
        select
            safe_cast(exercicio_empenho as int64) as ano,
            safe_cast(null as int64) as mes,
            safe_cast(null as date) as data,
            'RJ' as sigla_uf,
            '3304557' as id_municipio,
            safe_cast(orgao_programa_trabalho as string) as orgao,
            safe_cast(unidade_programa_trabalho as string) as id_unidade_gestora,
            safe_cast(
                concat(
                    nr_empenho,
                    ' ',
                    trim(orgao_programa_trabalho),
                    ' ',
                    trim(unidade_programa_trabalho),
                    ' ',
                    '3304557',
                    ' ',
                    (right(exercicio_empenho, 2))
                ) as string
            ) as id_empenho_bd,
            safe_cast(null as string) as id_empenho,
            safe_cast(nr_empenho as string) as numero_empenho,
            safe_cast(null as string) as id_liquidacao_bd,
            safe_cast(null as string) as id_liquidacao,
            safe_cast(null as string) as numero,
            safe_cast(null as string) as nome_responsavel,
            safe_cast(null as string) as documento_responsavel,
            safe_cast(null as bool) as indicador_restos_pagar,
            round(safe_cast(0 as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(safe_cast(valor_liquidado as float64), 2) as valor_final
        from
            {{
                set_datalake_project(
                    "world_wb_mides_staging.raw_despesa_rj_municipio"
                )
            }}
        where (safe_cast(exercicio_empenho as int64)) < 2017

    ),
    frequencia_rj_v1 as (
        select id_empenho_bd, count(id_empenho_bd) as frequencia_id
        from liquidado_municipio_rj_v1
        group by 1
        order by 2 desc

    ),
    liquidacao_municipio_rj_v1 as (
        select
            l.ano,
            l.mes,
            l.data,
            l.sigla_uf,
            l.id_municipio,
            l.orgao,
            l.id_unidade_gestora,
            (
                case
                    when frequencia_id > 1
                    then (safe_cast(null as string))
                    else l.id_empenho_bd
                end
            ) as id_empenho_bd,
            l.id_empenho,
            l.numero_empenho,
            l.id_liquidacao_bd,
            l.id_liquidacao,
            l.numero,
            l.nome_responsavel,
            l.documento_responsavel,
            l.indicador_restos_pagar,
            l.valor_inicial,
            l.valor_anulacao,
            l.valor_ajuste,
            l.valor_final
        from liquidado_municipio_rj_v1 l
        left join frequencia_rj_v1 f on l.id_empenho_bd = f.id_empenho_bd

    ),
    liquidado_municipio_rj_v2 as (
        select
            (safe_cast(exercicio as int64)) as ano,
            (safe_cast(extract(month from date(data)) as int64)) as mes,
            safe_cast(data as date) as data,
            'RJ' as sigla_uf,
            '3304557' as id_municipio,
            safe_cast(ug as string) as orgao,
            safe_cast(uo as string) as id_unidade_gestora,
            safe_cast(
                concat(
                    left(empenhoexercicio, length(empenhoexercicio) - 5),
                    ' ',
                    trim(uo),
                    ' ',
                    trim(ug),
                    ' ',
                    '3304557',
                    ' ',
                    (right(exercicio, 2))
                ) as string
            ) as id_empenho_bd,
            safe_cast(null as string) as id_empenho,
            safe_cast(empenhoexercicio as string) as numero_empenho,
            safe_cast(
                concat(
                    liquidacao,
                    ' ',
                    left(empenhoexercicio, length(empenhoexercicio) - 5),
                    ' ',
                    trim(uo),
                    ' ',
                    trim(ug),
                    ' ',
                    '3304557',
                    ' ',
                    (right(exercicio, 2))
                ) as string
            ) as id_liquidacao_bd,
            safe_cast(null as string) as id_liquidacao,
            safe_cast(liquidacao as string) as numero,
            safe_cast(null as string) as nome_responsavel,
            safe_cast(null as string) as documento_responsavel,
            safe_cast(null as bool) as indicador_restos_pagar,
            round(safe_cast(valor as float64), 2) as valor_inicial
        from
            {{
                set_datalake_project(
                    "world_wb_mides_staging.raw_despesa_ato_rj_municipio"
                )
            }}
        where tipoato = 'LIQUIDACAO'

    ),
    anulacao_municipio_rj_v2 as (
        select
            safe_cast(tipoato as string) as tipoato,
            safe_cast(
                concat(
                    left(empenhoexercicio, length(empenhoexercicio) - 5),
                    ' ',
                    trim(uo),
                    ' ',
                    trim(ug),
                    ' ',
                    '3304557',
                    ' ',
                    (right(exercicio, 2))
                ) as string
            ) as id_empenho_bd,
            sum(safe_cast(valor as float64)) as valor_anulacao,
        from
            {{
                set_datalake_project(
                    "world_wb_mides_staging.raw_despesa_ato_rj_municipio"
                )
            }}
        where
            tipoato in (
                'CANCELAMENTO LIQUIDACAO',
                'Cancelamento de liquidação de RPN',
                'CANCELAMENTO DE RPN'
            )
        group by 1, 2

    ),
    frequencia_rj_v2 as (
        select id_empenho_bd, count(1) as frequencia
        from anulacao_municipio_rj_v2
        group by 1

    ),
    liquidacao_municipio_rj_v2 as (
        select
            l.ano,
            l.mes,
            l.data,
            l.sigla_uf,
            l.id_municipio,
            l.orgao,
            l.id_unidade_gestora,
            l.id_empenho_bd,
            l.id_empenho,
            l.numero_empenho,
            l.id_liquidacao_bd,
            l.id_liquidacao,
            l.numero,
            l.nome_responsavel,
            l.documento_responsavel,
            case
                when tipoato = 'Cancelamento de liquidação de RPN'
                then true
                when tipoato = 'CANCELAMENTO DE RPN'
                then true
                else false
            end as indicador_restos_pagar,
            round(safe_cast(l.valor_inicial as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(safe_cast(l.valor_inicial as float64), 2) as valor_final
        from liquidado_municipio_rj_v2 l
        left join anulacao_municipio_rj_v2 a on l.id_empenho_bd = a.id_empenho_bd
        left join frequencia_rj_v2 f on l.id_empenho_bd = f.id_empenho_bd

    ),
    liquidacao_rj as (
        select
            (safe_cast(ano as int64)) as ano,
            (safe_cast(extract(month from date(data)) as int64)) as mes,
            safe_cast(data as date) as data,
            'RJ' as sigla_uf,
            safe_cast(id_municipio as string) as id_municipio,
            safe_cast(id_orgao as string) as orgao,
            safe_cast(unidade_administrativa as string) as id_unidade_gestora,
            safe_cast(
                concat(
                    numero_empenho,
                    ' ',
                    id_orgao,
                    ' ',
                    id_municipio,
                    ' ',
                    (right(ano, 2))
                ) as string
            ) as id_empenho_bd,
            safe_cast(null as string) as id_empenho,
            safe_cast(numero_empenho as string) as numero_empenho,
            safe_cast(null as string) as id_liquidacao_bd,
            safe_cast(null as string) as id_liquidacao,
            safe_cast(null as string) as numero,
            safe_cast(null as string) as nome_responsavel,
            safe_cast(null as string) as documento_responsavel,
            safe_cast(null as bool) as indicador_restos_pagar,
            round(safe_cast(valor as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(safe_cast(valor as float64), 2) as valor_final
        from {{ set_datalake_project("world_wb_mides_staging.raw_liquidacao_rj") }}
        where numero_empenho is not null

    )
select *
from liquidacao_municipio_rj_v1
union all
(select * from liquidacao_municipio_rj_v2)
union all
(select * from liquidacao_rj)
