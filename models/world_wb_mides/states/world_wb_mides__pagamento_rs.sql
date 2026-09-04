-- Rio Grande do Sul (RS) contribution to world_wb_mides.pagamento.
-- Split out of the monolithic pagamento model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    pago_rs as (
        select
            min(ano_recebimento) as ano_recebimento,
            safe_cast(ano_operacao as int64) as ano,
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
            m.sigla_uf as sigla_uf,
            safe_cast(a.id_municipio as string) as id_municipio,
            safe_cast(c.cd_orgao as string) as orgao,
            safe_cast(cd_orgao_orcamentario as string) as id_unidade_gestora,
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
            safe_cast(nr_empenho as string) as numero_empenho,
            safe_cast(
                concat(
                    nr_empenho,
                    ' ',
                    nr_liquidacao,
                    ' ',
                    c.cd_orgao,
                    ' ',
                    m.id_municipio,
                    ' ',
                    (right(ano_empenho, 2))
                ) as string
            ) as id_liquidacao_bd,
            safe_cast(null as string) as id_liquidacao,
            safe_cast(nr_liquidacao as string) as numero_liquidacao,
            safe_cast(
                concat(
                    nr_empenho,
                    ' ',
                    nr_liquidacao,
                    ' ',
                    nr_pagamento,
                    ' ',
                    c.cd_orgao,
                    ' ',
                    m.id_municipio,
                    ' ',
                    (right(ano_empenho, 2))
                ) as string
            ) as id_pagamento_bd,
            safe_cast(null as string) as id_pagamento,
            safe_cast(nr_pagamento as string) as numero,
            safe_cast(nm_credor as string) as nome_credor,
            safe_cast(cnpj_cpf as string) as documento_credor,
            safe_cast(null as bool) as indicador_restos_pagar,
            safe_cast(null as string) as fonte,
            safe_cast(vl_pagamento as float64) as valor_inicial
        from {{ set_datalake_project("world_wb_mides_staging.raw_despesa_rs") }} as c
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_orgao_rs") }} as a
            on c.cd_orgao = a.cd_orgao
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` m
            on m.id_municipio = a.id_municipio
        where tipo_operacao = 'P' and (safe_cast(vl_pagamento as float64) >= 0)
        group by
            2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22

    ),
    estorno_rs as (
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
            -1 * sum(safe_cast(vl_pagamento as float64)) as valor_anulacao
        from {{ set_datalake_project("world_wb_mides_staging.raw_despesa_rs") }} as c
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_orgao_rs") }} as a
            on c.cd_orgao = a.cd_orgao
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` m
            on m.id_municipio = a.id_municipio
        where tipo_operacao = 'P' and (safe_cast(vl_pagamento as float64) < 0)
        group by 1

    ),
    frequencia_rs as (
        select id_empenho_bd, count(id_empenho_bd) as frequencia_id
        from pago_rs
        group by 1

    ),
    pagamento1_rs as (
        select
            ano,
            mes,
            data,
            sigla_uf,
            id_municipio,
            orgao,
            id_unidade_gestora,
            p.id_empenho_bd,
            id_empenho,
            numero_empenho,
            p.id_liquidacao_bd,
            id_liquidacao,
            numero_liquidacao,
            id_pagamento_bd,
            id_pagamento,
            numero,
            nome_credor,
            ifnull(documento_credor, '99999999999') as documento_credor,
            indicador_restos_pagar,
            fonte,
            round(sum(valor_inicial), 2) as valor_inicial,
            round(sum(valor_anulacao / frequencia_id), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(
                sum(valor_inicial - ifnull((valor_anulacao / frequencia_id), 0)), 2
            ) as valor_final,
            round(safe_cast(0 as float64), 2) as valor_liquido_recebido
        from pago_rs p
        left join estorno_rs e on p.id_empenho_bd = e.id_empenho_bd
        left join frequencia_rs f on p.id_empenho_bd = f.id_empenho_bd
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20

    ),
    ddata_rs as (
        select
            id_pagamento_bd,
            case when (count(distinct data)) > 1 then 1 else 0 end as ddata
        from pagamento1_rs
        group by 1

    ),
    dorgao_rs as (
        select
            id_pagamento_bd,
            case when (count(distinct orgao)) > 1 then 1 else 0 end as dorgao
        from pagamento1_rs
        group by 1

    ),
    dugest_rs as (
        select
            id_pagamento_bd,
            case
                when (count(distinct id_unidade_gestora)) > 1 then 1 else 0
            end as dugest
        from pagamento1_rs
        group by 1

    ),
    credor_rs as (
        select
            id_pagamento_bd,
            case when (count(distinct nome_credor)) > 1 then 1 else 0 end as dcredor
        from pagamento1_rs
        group by 1

    ),
    dcredor_rs as (
        select
            id_pagamento_bd,
            case
                when (count(distinct documento_credor)) > 1 then 1 else 0
            end as ddocumento
        from pagamento1_rs
        group by 1

    ),
    dummies as (
        select d.id_pagamento_bd, ddata, dorgao, dugest, dcredor, ddocumento
        from ddata_rs d
        left join credor_rs c on d.id_pagamento_bd = c.id_pagamento_bd
        left join dcredor_rs dc on d.id_pagamento_bd = dc.id_pagamento_bd
        left join dorgao_rs o on d.id_pagamento_bd = o.id_pagamento_bd
        left join dugest_rs u on d.id_pagamento_bd = u.id_pagamento_bd

    ),
    pagamento_rs as (
        select
            ano,
            mes,
            data,
            sigla_uf,
            id_municipio,
            orgao,
            id_unidade_gestora,
            id_empenho_bd,
            id_empenho,
            numero_empenho,
            id_liquidacao_bd,
            id_liquidacao,
            numero_liquidacao,
            case
                when
                    ddata = 1
                    or dorgao = 1
                    or dugest = 1
                    or dcredor = 1
                    or ddocumento = 1
                    or (numero_liquidacao = '0' and valor_final = 0)
                    or (numero = '0' and valor_final = 0)
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
        from pagamento1_rs p
        left join dummies d on p.id_pagamento_bd = d.id_pagamento_bd

    )
select *
from pagamento_rs
