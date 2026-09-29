-- Ceará (CE) contribution to world_wb_mides.pagamento.
-- Split out of the monolithic pagamento model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    empenho_ce as (
        select
            safe_cast(
                concat(
                    numero_empenho,
                    ' ',
                    trim(codigo_orgao),
                    ' ',
                    trim(codigo_unidade),
                    ' ',
                    m.geoibgeid,
                    ' ',
                    (substring(data_emissao_empenho, 6, 2)),
                    ' ',
                    (substring(data_emissao_empenho, 3, 2))
                ) as string
            ) as id_empenho_bd,
            safe_cast(nome_negociante as string) as nome_credor,
            safe_cast(
                replace (
                    replace (numero_documento_negociante, '.', ''), '-', ''
                ) as string
            ) as documento_credor,
            safe_cast(safe_cast(codigo_fonte_ as int64) as string) as fonte,
        from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_ce") }} e
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_municipio_ce") }} m
            on e.codigo_municipio = m.codigo_municipio

    ),
    pago_ce as (
        select
            (safe_cast(extract(year from date(data_nota_pagamento)) as int64)) as ano,
            (safe_cast(extract(month from date(data_nota_pagamento)) as int64)) as mes,
            safe_cast(
                extract(date from timestamp(data_nota_pagamento)) as date
            ) as data,
            'CE' as sigla_uf,
            safe_cast(m.geoibgeid as string) as id_municipio,
            safe_cast(p.codigo_orgao as string) orgao,
            safe_cast(p.codigo_unidade as string) as id_unidade_gestora,
            safe_cast(
                concat(
                    p.numero_empenho,
                    ' ',
                    trim(p.codigo_orgao),
                    ' ',
                    trim(p.codigo_unidade),
                    ' ',
                    m.geoibgeid,
                    ' ',
                    (substring(p.data_emissao_empenho, 6, 2)),
                    ' ',
                    (substring(p.data_emissao_empenho, 3, 2))
                ) as string
            ) as id_empenho_bd,
            safe_cast(null as string) as id_empenho,
            safe_cast(p.numero_empenho as string) as numero_empenho,
            safe_cast(null as string) as id_liquidacao_bd,
            safe_cast(null as string) as id_liquidacao,
            safe_cast(null as string) as numero_liquidacao,
            safe_cast(
                concat(
                    p.numero_empenho,
                    ' ',
                    safe_cast(safe_cast(numero_nota_pagamento as int64) as string),
                    ' ',
                    trim(p.codigo_orgao),
                    ' ',
                    trim(p.codigo_unidade),
                    ' ',
                    m.geoibgeid,
                    ' ',
                    (substring(p.data_emissao_empenho, 6, 2)),
                    ' ',
                    (substring(p.data_emissao_empenho, 3, 2))
                ) as string
            ) as id_pagamento_bd,
            safe_cast(null as string) as id_pagamento,
            safe_cast(numero_nota_pagamento as string) as numero,
            safe_cast(null as bool) as indicador_restos_pagar,
            round(safe_cast(valor_nota_pagamento as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(safe_cast(valor_nota_pagamento as float64), 2) as valor_final,
            round(safe_cast(0 as float64), 2) as valor_liquido_recebido,
        from {{ set_datalake_project("world_wb_mides_staging.raw_pagamento_ce") }} p
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_municipio_ce") }} m
            on p.codigo_municipio = m.codigo_municipio

    ),
    frequencia_ce as (
        select id_pagamento_bd, count(id_pagamento_bd) as frequencia_id
        from pago_ce
        group by 1

    ),
    pagamento_ce as (
        select
            ano,
            mes,
            data,
            sigla_uf,
            id_municipio,
            orgao,
            id_unidade_gestora,
            p.id_empenho_bd,
            p.id_empenho,
            p.numero_empenho,
            id_liquidacao_bd,
            id_liquidacao,
            numero_liquidacao,
            (
                case
                    when (frequencia_id > 1)
                    then (safe_cast(null as string))
                    else p.id_pagamento_bd
                end
            ) as id_pagamento_bd,
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
        from pago_ce p
        left join frequencia_ce f on p.id_pagamento_bd = f.id_pagamento_bd
        left join empenho_ce e on p.id_empenho_bd = e.id_empenho_bd

    )
select *
from pagamento_ce
