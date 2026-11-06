-- Paraíba (PB) contribution to world_wb_mides.pagamento.
-- Split out of the monolithic pagamento model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    pago_pb as (
        select
            safe_cast(p.dt_ano as int64) as ano,
            safe_cast(substring(trim(dt_pagamento), -7, 2) as int64) as mes,
            safe_cast(
                concat(
                    substring(trim(dt_pagamento), -4),
                    '-',
                    substring(trim(dt_pagamento), -7, 2),
                    '-',
                    substring(trim(dt_pagamento), 1, 2)
                ) as date
            ) as data,
            m.sigla_uf,
            safe_cast(m.id_municipio as string) as id_municipio,
            safe_cast(null as string) as orgao,
            safe_cast(p.cd_ugestora as string) as id_unidade_gestora,
            safe_cast(
                concat(
                    e.nu_empenho,
                    ' ',
                    e.cd_ugestora,
                    ' ',
                    m.id_municipio,
                    ' ',
                    (right(e.dt_ano, 2))
                ) as string
            ) as id_empenho_bd,
            safe_cast(null as string) as id_empenho,
            safe_cast(p.nu_empenho as string) as numero_empenho,
            safe_cast(null as string) as id_liquidacao_bd,
            safe_cast(null as string) as id_liquidacao,
            safe_cast(null as string) as numero_liquidacao,
            safe_cast(
                concat(
                    p.nu_empenho,
                    ' ',
                    (safe_cast(nu_parcela as int64)),
                    ' ',
                    p.cd_ugestora,
                    ' ',
                    id_municipio,
                    ' ',
                    (right(p.dt_ano, 2))
                ) as string
            ) as id_pagamento_bd,
            safe_cast(null as string) as id_pagamento,
            safe_cast(nu_parcela as string) as numero,
            safe_cast(no_credor as string) as nome_credor,
            safe_cast(
                replace (replace (cd_credor, '.', ''), '-', '') as string
            ) as documento_credor,
            safe_cast(null as bool) as indicador_restos_pagar,
            safe_cast(tp_fonterecursos as string) as fonte,
            round(safe_cast(vl_pagamento as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(vl_retencao as float64), 2) as valor_ajuste,
            round(safe_cast(vl_pagamento as float64), 2) as valor_final,
            round(
                safe_cast(vl_pagamento as float64) - safe_cast(vl_retencao as float64),
                2
            ) as valor_liquido_recebido,
        from {{ set_datalake_project("world_wb_mides_staging.raw_pagamento_pb") }} p
        left join
            {{ set_datalake_project("world_wb_mides_staging.raw_empenho_pb") }} e
            on p.nu_empenho = e.nu_empenho
            and p.cd_ugestora = e.cd_ugestora
            and p.de_uorcamentaria = e.de_uorcamentaria
            and p.dt_ano = e.dt_ano
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_municipio_pb") }} m
            on safe_cast(e.cd_ugestora as string)
            = safe_cast(m.id_unidade_gestora as string)

    ),
    frequencia_pb as (
        select id_pagamento_bd, count(id_pagamento_bd) frequencia_id
        from pago_pb
        group by 1

    ),
    pagamento_pb as (
        select
            ano,
            mes,
            data,
            sigla_uf,
            id_municipio,
            orgao,
            id_unidade_gestora,
            case
                when (frequencia_id > 1)
                then (safe_cast(null as string))
                else p.id_empenho_bd
            end as id_empenho_bd,
            id_empenho,
            numero_empenho,
            id_liquidacao_bd,
            id_liquidacao,
            numero_liquidacao,
            case
                when (frequencia_id > 1)
                then (safe_cast(null as string))
                else p.id_pagamento_bd
            end as id_pagamento_bd,
            id_pagamento,
            numero,
            nome_credor,
            documento_credor,
            indicador_restos_pagar,
            fonte,
            valor_inicial,
            valor_anulacao,
            valor_ajuste,
            valor_final,
            valor_liquido_recebido
        from pago_pb p
        left join frequencia_pb f on p.id_pagamento_bd = f.id_pagamento_bd

    )
select *
from pagamento_pb
