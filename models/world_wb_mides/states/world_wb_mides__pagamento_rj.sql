-- Rio de Janeiro (RJ) contribution to world_wb_mides.pagamento.
-- Split out of the monolithic pagamento model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    pago_municipio_rj_v1 as (
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
            safe_cast(null as string) as numero_liquidacao,
            safe_cast(null as string) as id_pagamento_bd,
            safe_cast(null as string) as id_pagamento,
            safe_cast(null as string) as numero,
            safe_cast(favorecido as string) as nome_credor,
            safe_cast(codigo_favorecido as string) as documento_credor,
            safe_cast(null as bool) as indicador_restos_pagar,
            safe_cast(fonte_recursos as string) as fonte,
            round(safe_cast(0 as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(safe_cast(valor_pago as float64), 2) as valor_final,
            round(safe_cast(valor_pago as float64), 2) as valor_liquido_recebido
        from
            {{
                set_datalake_project(
                    "world_wb_mides_staging.raw_despesa_rj_municipio"
                )
            }}

    ),
    frequencia_rj_v1 as (
        select id_empenho_bd, count(id_empenho_bd) as frequencia_id
        from pago_municipio_rj_v1
        group by 1
        order by 2 desc

    ),
    pagamento_municipio_rj_v1 as (
        select
            p.ano,
            p.mes,
            p.data,
            p.sigla_uf,
            p.id_municipio,
            p.orgao,
            p.id_unidade_gestora,
            (
                case
                    when frequencia_id > 1
                    then (safe_cast(null as string))
                    else p.id_empenho_bd
                end
            ) as id_empenho_bd,
            p.id_empenho,
            p.numero_empenho,
            p.id_liquidacao_bd,
            p.id_liquidacao,
            p.numero_liquidacao,
            p.id_pagamento_bd,
            p.id_pagamento,
            p.numero,
            p.nome_credor,
            p.documento_credor,
            p.indicador_restos_pagar,
            p.fonte,
            p.valor_inicial,
            p.valor_anulacao,
            p.valor_ajuste,
            p.valor_final,
            p.valor_liquido_recebido
        from pago_municipio_rj_v1 p
        left join frequencia_rj_v1 f on p.id_empenho_bd = f.id_empenho_bd

    ),
    pago_municipio_rj_v2 as (
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
            safe_cast(liquidacao as string) as numero_liquidacao,
            safe_cast(
                concat(
                    pagamento,
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
            ) as id_pagamento_bd,
            safe_cast(null as string) as id_pagamento,
            safe_cast(pagamento as string) as numero,
            safe_cast(nomecredor as string) as nome_credor,
            safe_cast(credor as string) as documento_credor,
            safe_cast(null as bool) as indicador_restos_pagar,
            safe_cast(fonterecursos as string) as fonte,
            round(safe_cast(valor as float64), 2) as valor_inicial,
        from
            {{
                set_datalake_project(
                    "world_wb_mides_staging.raw_despesa_ato_rj_municipio"
                )
            }}
        where tipoato = 'PAGAMENTO'

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
                'CANCEL.PAGAMENTO RET.DOTAÇÃO',
                'CANCEL.PAGAMENTO RET.EMPENHO',
                'CANCELAMENTO DE PAGAMENTO DE RPP',
                'CANCELAMENTO DE PAGAMENTO DE RPN',
                'Cancelamento de RPP'
            )
        group by 1, 2

    ),
    frequencia_rj_v2 as (
        select id_empenho_bd, count(1) as frequencia
        from anulacao_municipio_rj_v2
        group by 1

    ),
    pagamento_municipio_rj_v2 as (
        select
            p.ano,
            p.mes,
            p.data,
            p.sigla_uf,
            p.id_municipio,
            p.orgao,
            p.id_unidade_gestora,
            p.id_empenho_bd,
            p.id_empenho,
            p.numero_empenho,
            p.id_liquidacao_bd,
            p.id_liquidacao,
            p.numero_liquidacao,
            p.id_empenho_bd,
            p.id_empenho,
            p.numero,
            p.nome_credor,
            p.documento_credor,
            case
                when tipoato = 'CANCELAMENTO DE PAGAMENTO DE RPP'
                then true
                when tipoato = 'CANCELAMENTO DE PAGAMENTO DE RPN'
                then true
                when tipoato = 'Cancelamento de RPP'
                then true
                else false
            end as indicador_restos_pagar,
            p.fonte,
            round(safe_cast(p.valor_inicial as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(safe_cast(p.valor_inicial as float64), 2) as valor_final,
            round(safe_cast(p.valor_inicial as float64), 2) as valor_liquido_recebido
        from pago_municipio_rj_v2 p
        left join anulacao_municipio_rj_v2 a on p.id_empenho_bd = a.id_empenho_bd
        left join frequencia_rj_v2 f on p.id_empenho_bd = f.id_empenho_bd

    ),
    pagamento_rj as (
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
            safe_cast(null as string) as numero_liquidacao,
            safe_cast(null as string) as id_pagamento_bd,
            safe_cast(null as string) as id_pagamento,
            safe_cast(null as string) as numero,
            safe_cast(credor as string) as nome_credor,
            safe_cast(null as string) as documento_credor,
            safe_cast(null as bool) as indicador_restos_pagar,
            safe_cast(fonte as string) as fonte,
            round(safe_cast(valor as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(safe_cast(valor as float64), 2) as valor_final,
            round(safe_cast(valor as float64), 2) as valor_liquido_recebido,
        from {{ set_datalake_project("world_wb_mides_staging.raw_pagamento_rj") }}
        where numero_empenho is not null

    )
select *
from pagamento_municipio_rj_v1
union all
(select * from pagamento_municipio_rj_v2)
union all
(select * from pagamento_rj)
