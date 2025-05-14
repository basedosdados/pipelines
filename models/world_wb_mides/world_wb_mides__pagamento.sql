{{
    config(
        alias="pagamento",
        schema="world_wb_mides",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1996, "end": 2022, "interval": 1},
        },
        cluster_by=["mes", "sigla_uf"],
        labels={"tema": "economia"},
    )
}}
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
    id_pagamento_bd,
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
from
    (
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
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_empenho_ce") }} e
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_municipio_ce` m
                    on e.codigo_municipio = m.codigo_municipio
            ),
            pago_ce as (
                select
                    (
                        safe_cast(extract(year from date(data_nota_pagamento)) as int64)
                    ) as ano,
                    (
                        safe_cast(
                            extract(month from date(data_nota_pagamento)) as int64
                        )
                    ) as mes,
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
                            safe_cast(
                                safe_cast(numero_nota_pagamento as int64) as string
                            ),
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
                    round(
                        safe_cast(valor_nota_pagamento as float64), 2
                    ) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(safe_cast(valor_nota_pagamento as float64), 2) as valor_final,
                    round(safe_cast(0 as float64), 2) as valor_liquido_recebido,
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_pagamento_ce"
                        )
                    }} p
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_municipio_ce` m
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
            ),
            pagamento_mg as (
                select distinct
                    safe_cast(p.ano as int64) as ano,
                    safe_cast(p.mes as int64) as mes,
                    safe_cast(p.data as date) as data,
                    safe_cast(p.sigla_uf as string) as sigla_uf,
                    safe_cast(p.id_municipio as string) as id_municipio,
                    safe_cast(p.orgao as string) as orgao,
                    safe_cast(p.id_unidade_gestora as string) as id_unidade_gestora,
                    safe_cast(
                        case
                            when id_empenho != '-1'
                            then
                                concat(
                                    id_empenho,
                                    ' ',
                                    p.orgao,
                                    ' ',
                                    p.id_municipio,
                                    ' ',
                                    (right(ano, 2))
                                )
                            when id_empenho = '-1'
                            then
                                concat(
                                    id_empenho_origem,
                                    ' ',
                                    r.orgao,
                                    ' ',
                                    r.id_municipio,
                                    ' ',
                                    (right(num_ano_emp_origem, 2))
                                )
                        end as string
                    ) as id_empenho_bd,
                    safe_cast(
                        case
                            when p.id_empenho = '-1'
                            then replace (p.id_empenho, '-1', id_empenho_origem)
                        end as string
                    ) as id_empenho,
                    safe_cast(p.numero_empenho as string) as numero_empenho,
                    safe_cast(
                        case
                            when p.id_liquidacao != '-1'
                            then
                                concat(
                                    p.id_liquidacao,
                                    ' ',
                                    p.orgao,
                                    ' ',
                                    p.id_municipio,
                                    ' ',
                                    (right(p.ano, 2))
                                )
                            when p.id_liquidacao = '-1'
                            then
                                concat(
                                    ' ',
                                    r.orgao,
                                    ' ',
                                    r.id_municipio,
                                    ' ',
                                    (right(p.ano, 2))
                                )
                        end as string
                    ) as id_liquidacao_bd,
                    safe_cast(
                        case
                            when p.id_empenho = '-1'
                            then replace (p.id_liquidacao, '-1', '')
                        end as string
                    ) as id_liquidacao,
                    safe_cast(p.numero_liquidacao as string) as numero_liquidacao,
                    safe_cast(
                        concat(
                            id_pagamento,
                            ' ',
                            p.orgao,
                            ' ',
                            p.id_municipio,
                            ' ',
                            (right(p.ano, 2))
                        ) as string
                    ) as id_pagamento_bd,
                    safe_cast(id_pagamento as string) as id_pagamento,
                    safe_cast(p.numero_pagamento as string) as numero,
                    safe_cast(nome_credor as string) as nome_credor,
                    safe_cast(
                        replace(replace (documento_credor, '.', ''), '-', '') as string
                    ) as documento_credor,
                    safe_cast(
                        case when p.id_rsp != '-1' then 1 else 0 end as bool
                    ) as indicador_restos_pagar,
                    safe_cast(left(fonte, 3) as string) as fonte,
                    round(
                        safe_cast(valor_pagamento_original as float64), 2
                    ) as valor_inicial,
                    round(
                        ifnull(safe_cast(vlr_anu_fonte as float64), 0), 2
                    ) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(
                        safe_cast(valor_pagamento_original as float64)
                        - ifnull(safe_cast(vlr_anu_fonte as float64), 0),
                        2
                    ) as valor_final,
                    round(
                        safe_cast(valor_pagamento_original as float64)
                        - ifnull(safe_cast(vlr_anu_fonte as float64), 0)
                        - ifnull(safe_cast(vlr_ret_fonte as float64), 0),
                        2
                    ) as valor_liquido_recebido,
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_pagamento_mg"
                        )
                    }} as p
                left join
                    `basedosdados-staging.world_wb_mides_staging.raw_rsp_mg` as r
                    on p.id_rsp = r.id_rsp
            ),
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
                        safe_cast(vl_pagamento as float64)
                        - safe_cast(vl_retencao as float64),
                        2
                    ) as valor_liquido_recebido,
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_pagamento_pb"
                        )
                    }} p
                left join
                    `basedosdados-staging.world_wb_mides_staging.raw_empenho_pb` e
                    on p.nu_empenho = e.nu_empenho
                    and p.cd_ugestora = e.cd_ugestora
                    and p.de_uorcamentaria = e.de_uorcamentaria
                    and p.dt_ano = e.dt_ano
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_municipio_pb` m
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
            ),
            pagamento_pe as (
                select
                    safe_cast(p.anoreferencia as int64) as ano,
                    (safe_cast(extract(month from date(data)) as int64)) as mes,
                    safe_cast(extract(date from timestamp(data)) as date) as data,
                    safe_cast(unidadefederativa as string) as sigla_uf,
                    safe_cast(codigoibge as string) as id_municipio,
                    safe_cast(null as string) orgao,
                    safe_cast(id_unidadegestora as string) as id_unidade_gestora,
                    safe_cast(null as string) as id_empenho_bd,
                    safe_cast(trim(idempenho) as string) as id_empenho,
                    safe_cast(p.numeroempenho as string) as numero_empenho,
                    safe_cast(null as string) as id_liquidacao_bd,
                    safe_cast(null as string) as id_liquidacao,
                    safe_cast(null as string) as numero_liquidacao,
                    safe_cast(null as string) as id_pagamento_bd,
                    safe_cast(null as string) as id_pagamento,
                    safe_cast(null as string) as numero,
                    safe_cast(null as string) as nome_credor,
                    safe_cast(null as string) as documento_credor,
                    safe_cast(null as bool) as indicador_restos_pagar,
                    safe_cast(null as string) as fonte,
                    round(safe_cast(valor as float64), 2) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(
                        (
                            case
                                when (safe_cast((valor) as float64) < -1000000000000)
                                then null
                                else safe_cast((valor) as float64)
                            end
                        ),
                        2
                    ) as valor_final,
                    round(safe_cast(0 as float64), 2) as valor_liquido_recebido,
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_pagamento_pe"
                        )
                    }} p
                inner join
                    `basedosdados-staging.world_wb_mides_staging.aux_municipio_pe` m
                    on safe_cast(p.id_unidade_gestora as string)
                    = safe_cast(m.id_unidadegestora as string)
            ),
            pagamento_pr as (
                select
                    safe_cast(nranopagamento as int64) as ano,
                    (safe_cast(extract(month from date(dtoperacao)) as int64)) as mes,
                    safe_cast(extract(date from timestamp(dtoperacao)) as date) as data,
                    sigla_uf,
                    id_municipio,
                    safe_cast(cdorgao as string) as orgao,
                    safe_cast(cdunidade as string) as id_unidade_gestora,
                    safe_cast(
                        concat(p.idempenho, ' ', m.id_municipio) as string
                    ) as id_empenho_bd,
                    safe_cast(p.idempenho as string) as id_empenho,
                    safe_cast(nrempenho as string) as numero_empenho,
                    safe_cast(
                        concat(p.idliquidacao, ' ', m.id_municipio) as string
                    ) as id_liquidacao_bd,
                    safe_cast(p.idliquidacao as string) as id_liquidacao,
                    safe_cast(null as string) as numero_liquidacao,
                    safe_cast(
                        concat(p.idpagamento, ' ', m.id_municipio) as string
                    ) as id_pagamento_bd,
                    safe_cast(idpagamento as string) as id_pagamento,
                    safe_cast(nrpagamento as string) as numero,
                    safe_cast(nmcredor as string) as nome_credor,
                    safe_cast(
                        regexp_replace(nrdoccredor, '[^0-9]', '') as string
                    ) as documento_credor,
                    safe_cast(null as bool) as indicador_restos_pagar,
                    safe_cast(cdfontereceita as string) as fonte,
                    round(safe_cast(vloperacao as float64), 2) as valor_inicial,
                    round(safe_cast(nranoliquidacao as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(safe_cast(p.cdibge as float64), 2) as valor_final,
                    round(safe_cast(0 as float64), 2) as valor_liquido_recebido,
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_pagamento_pr"
                        )
                    }} p
                left join
                    `basedosdados-staging.world_wb_mides_staging.raw_empenho_pr` e
                    on p.idempenho = e.idempenho
                left join
                    basedosdados.br_bd_diretorios_brasil.municipio m
                    on e.cdibge = id_municipio_6
            ),
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
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_despesa_rs") }}
                    as c
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_orgao_rs` as a
                    on c.cd_orgao = a.cd_orgao
                left join
                    `basedosdados.br_bd_diretorios_brasil.municipio` m
                    on m.id_municipio = a.id_municipio
                where tipo_operacao = 'P' and (safe_cast(vl_pagamento as float64) >= 0)
                group by
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                    12,
                    13,
                    14,
                    15,
                    16,
                    17,
                    18,
                    19,
                    20,
                    21,
                    22
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
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_despesa_rs") }}
                    as c
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_orgao_rs` as a
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
                        sum(
                            valor_inicial - ifnull((valor_anulacao / frequencia_id), 0)
                        ),
                        2
                    ) as valor_final,
                    round(safe_cast(0 as float64), 2) as valor_liquido_recebido
                from pago_rs p
                left join estorno_rs e on p.id_empenho_bd = e.id_empenho_bd
                left join frequencia_rs f on p.id_empenho_bd = f.id_empenho_bd
                group by
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                    12,
                    13,
                    14,
                    15,
                    16,
                    17,
                    18,
                    19,
                    20
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
                    case
                        when (count(distinct nome_credor)) > 1 then 1 else 0
                    end as dcredor
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
            ),
            pago_sp as (
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
                    sigla_uf,
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
                    ) as id_pagamento_bd,
                    safe_cast(null as string) as numero_liquidacao,
                    safe_cast(null as string) as id_pagamento,
                    safe_cast(null as string) as numero,
                    safe_cast(ds_despesa as string) as nome_credor,
                    safe_cast(
                        regexp_replace(identificador_despesa, '[^0-9]', '') as string
                    ) as documento_credor,
                    safe_cast(null as bool) as indicador_restos_pagar,
                    safe_cast(null as string) as fonte,
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
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_despesa_sp") }} e
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_municipio_sp` m
                    on m.ds_orgao = e.ds_orgao
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_funcao`
                    on ds_funcao_governo = upper(nome_funcao)
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_subfuncao`
                    on ds_subfuncao_governo = upper(nome_subfuncao)
                where tp_despesa = 'Valor Pago'
            ),
            frequencia as (
                select id_empenho_bd, count(id_empenho_bd) as frequencia_id
                from pago_sp
                group by 1
                order by 2 desc
            ),
            dorgao as (
                select
                    id_empenho_bd,
                    case when (count(distinct orgao)) > 1 then 1 else 0 end as dorgao
                from pago_sp
                group by 1
            ),
            ddesc as (
                select
                    id_empenho_bd,
                    case
                        when (count(distinct ifnull(descricao, ''))) > 1 then 1 else 0
                    end as ddesc
                from pago_sp
                group by 1
            ),
            dmod as (
                select
                    id_empenho_bd,
                    case
                        when (count(distinct modalidade_licitacao)) > 1 then 1 else 0
                    end as dmod
                from pago_sp
                group by 1
            ),
            dfun as (
                select
                    id_empenho_bd,
                    case when (count(distinct funcao)) > 1 then 1 else 0 end as dfun
                from pago_sp
                group by 1
            ),
            dsubf as (
                select
                    id_empenho_bd,
                    case when (count(distinct subfuncao)) > 1 then 1 else 0 end as dsubf
                from pago_sp
                group by 1
            ),
            dprog as (
                select
                    id_empenho_bd,
                    case when (count(distinct programa)) > 1 then 1 else 0 end as dprog
                from pago_sp
                group by 1
            ),
            dacao as (
                select
                    id_empenho_bd,
                    case when (count(distinct acao)) > 1 then 1 else 0 end as dacao
                from pago_sp
                group by 1
            ),
            delem as (
                select
                    id_empenho_bd,
                    case
                        when (count(distinct elemento_despesa)) > 1 then 1 else 0
                    end as delem
                from pago_sp
                group by 1
            ),
            dcredor as (
                select
                    id_empenho_bd,
                    case
                        when (count(distinct nome_credor)) > 1 then 1 else 0
                    end as dcredor
                from pago_sp
                group by 1
            ),
            ddocumento as (
                select
                    id_empenho_bd,
                    case
                        when (count(distinct documento_credor)) > 1 then 1 else 0
                    end as ddocumento
                from pago_sp
                group by 1
            ),
            dummies_sp as (
                select
                    o.id_empenho_bd,
                    dorgao,
                    dmod,
                    ddesc,
                    dfun,
                    dsubf,
                    dprog,
                    dacao,
                    delem,
                    dcredor,
                    ddocumento
                from dorgao o
                full outer join dmod m on o.id_empenho_bd = m.id_empenho_bd
                full outer join ddesc d on o.id_empenho_bd = d.id_empenho_bd
                full outer join dfun f on o.id_empenho_bd = f.id_empenho_bd
                full outer join dsubf s on o.id_empenho_bd = s.id_empenho_bd
                full outer join dprog p on o.id_empenho_bd = p.id_empenho_bd
                full outer join dacao a on o.id_empenho_bd = a.id_empenho_bd
                full outer join delem e on o.id_empenho_bd = e.id_empenho_bd
                full outer join dcredor c on o.id_empenho_bd = c.id_empenho_bd
                full outer join ddocumento dc on o.id_empenho_bd = dc.id_empenho_bd
            ),
            frequencia_pg_sp as (
                select id_pagamento_bd, count(id_pagamento_bd) frequencia_id
                from pago_sp
                group by 1
            ),
            pagamento_sp as (
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
                            else p.id_empenho_bd
                        end
                    ) as id_empenho_bd,
                    id_empenho,
                    numero_empenho,
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
                            else p.id_liquidacao_bd
                        end
                    ) as id_liquidacao_bd,
                    id_liquidacao,
                    numero_liquidacao,
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
                                    or dcredor = 1
                                    or ddocumento = 1
                                )
                                or frequencia_id > 1
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
                    round(safe_cast(sum(valor_inicial) as float64), 2) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(safe_cast(sum(valor_inicial) as float64), 2) as valor_final,
                    round(safe_cast(0 as float64), 2) as valor_liquido_recebido
                from pago_sp p
                left join dummies_sp d on d.id_empenho_bd = p.id_empenho_bd
                left join frequencia_pg_sp f on f.id_pagamento_bd = p.id_pagamento_bd
                group by 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
            ),
            pagamento_municipio_sp as (
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
                    safe_cast(null as string) as numero_liquidacao,
                    safe_cast(null as string) as id_pagamento_bd,
                    safe_cast(null as string) as id_pagamento,
                    safe_cast(null as string) as numero,
                    safe_cast(razao_social as string) as nome_credor,
                    safe_cast(cpf_cnpj as string) as documento_credor,
                    safe_cast(null as bool) as indicador_restos_pagar,
                    safe_cast(codigo_fonte_recurso as string) as fonte,
                    round(safe_cast(pago as float64), 2) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(safe_cast(pago as float64), 2) as valor_final,
                    round(safe_cast(pago as float64), 2) as valor_liquido_recebido,
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_despesa_sp_municipio"
                        )
                    }}
            ),
            pago_municipio_rj_v1 as (
                select
                    safe_cast(exercicio_empenho as int64) as ano,
                    safe_cast(null as int64) as mes,
                    safe_cast(null as date) as data,
                    'RJ' as sigla_uf,
                    '3304557' as id_municipio,
                    safe_cast(orgao_programa_trabalho as string) as orgao,
                    safe_cast(
                        unidade_programa_trabalho as string
                    ) as id_unidade_gestora,
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
                    round(
                        safe_cast(p.valor_inicial as float64), 2
                    ) as valor_liquido_recebido
                from pago_municipio_rj_v2 p
                left join
                    anulacao_municipio_rj_v2 a on p.id_empenho_bd = a.id_empenho_bd
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
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_pagamento_rj"
                        )
                    }}
                where numero_empenho is not null
            ),
            pagamento_df as (
                select
                    (safe_cast(exercicio as int64)) as ano,
                    safe_cast(substring(emissao, -7, 2) as int64) as mes,
                    safe_cast(
                        concat(
                            substring(emissao, -4),
                            '-',
                            substring(emissao, -7, 2),
                            '-',
                            substring(emissao, 1, 2)
                        ) as date
                    ) as data,
                    'DF' as sigla_uf,
                    '5300108' as id_municipio,
                    safe_cast(codigo_ug as string) as orgao,
                    safe_cast(codigo_gestao as string) as id_unidade_gestora,
                    safe_cast(
                        concat(
                            right(nota_empenho, length(nota_empenho) - 6),
                            ' ',
                            codigo_ug,
                            ' ',
                            codigo_gestao,
                            ' ',
                            '5300108',
                            ' ',
                            (right(exercicio, 2))
                        ) as string
                    ) as id_empenho_bd,
                    safe_cast(null as string) as id_empenho,
                    safe_cast(nota_empenho as string) as numero_empenho,
                    case
                        when length(nota_lancamento) = 11
                        then
                            safe_cast(
                                concat(
                                    right(nota_lancamento, length(nota_lancamento) - 6),
                                    ' ',
                                    codigo_ug,
                                    ' ',
                                    codigo_gestao,
                                    ' ',
                                    '5300108',
                                    ' ',
                                    (right(exercicio, 2))
                                ) as string
                            )
                    end as id_liquidacao_bd,
                    safe_cast(null as string) as id_liquidacao,
                    safe_cast(nota_lancamento as string) as numero_liquidacao,
                    case
                        when length(numero_ordem_bancaria) = 11
                        then
                            safe_cast(
                                concat(
                                    right(
                                        numero_ordem_bancaria,
                                        length(numero_ordem_bancaria) - 6
                                    ),
                                    ' ',
                                    codigo_ug,
                                    ' ',
                                    codigo_gestao,
                                    ' ',
                                    '5300108',
                                    ' ',
                                    (right(exercicio, 2))
                                ) as string
                            )
                    end as id_pagamento_bd,
                    safe_cast(null as string) as id_pagamento,
                    safe_cast(numero_ordem_bancaria as string) as numero,
                    safe_cast(credor as string) as nome_credor,
                    safe_cast(cnpj_cpf_credor as string) as documento_credor,
                    case
                        when ano_ordem_bancaria != ano_nota_empenho then true else false
                    end as indicador_restos_pagar,
                    safe_cast(null as string) as fonte,
                    round(
                        safe_cast(replace(valor_final_x, ',', '.') as float64), 2
                    ) as valor_inicial,
                    round(
                        safe_cast(replace(valor_cancelado, ',', '.') as float64), 2
                    ) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(
                        safe_cast(replace(valor_final_x, ',', '.') as float64)
                        - safe_cast(replace(valor_cancelado, ',', '.') as float64),
                        2
                    ) as valor_final,
                    round(
                        safe_cast(replace(valor_final_x, ',', '.') as float64)
                        - safe_cast(replace(valor_cancelado, ',', '.') as float64),
                        2
                    ) as valor_liquido_recebido,
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_pagamento_df"
                        )
                    }}
            ),
            pago_sc as (
                select
                    safe_cast(ano_emp as int64) as ano,
                    safe_cast(substring(trim(data_empenho), -7, 2) as int64) as mes,
                    safe_cast(null as date) as data,
                    'SC' as sigla_uf,
                    safe_cast(id_municipio as string) as id_municipio,
                    safe_cast(codigo_orgao as string) as orgao,
                    safe_cast(null as string) as id_unidade_gestora,
                    safe_cast(
                        concat(
                            num_empenho,
                            ' ',
                            codigo_orgao,
                            ' ',
                            id_municipio,
                            ' ',
                            (right(cast(ano_emp as string), 2))
                        ) as string
                    ) as id_empenho_bd,
                    safe_cast(null as string) as id_empenho,
                    safe_cast(num_empenho as string) as numero_empenho,
                    safe_cast(null as string) as id_liquidacao_bd,
                    safe_cast(null as string) as id_liquidacao,
                    safe_cast(null as string) as numero_liquidacao,
                    safe_cast(null as string) as id_pagamento_bd,
                    safe_cast(null as string) as id_pagamento,
                    safe_cast(null as string) as numero,
                    safe_cast(nome_credor as string) as nome_credor,
                    safe_cast(cpf_cnpj as string) as documento_credor,
                    safe_cast(null as bool) as indicador_restos_pagar,
                    safe_cast(right(especificacao_fonte_recurso, 2) as string) as fonte,
                    round(safe_cast(0 as float64), 2) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(safe_cast(valor_pagamento as float64), 2) as valor_final,
                    round(
                        safe_cast(valor_pagamento as float64), 2
                    ) as valor_liquido_recebido
                from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_sc") }}
            ),
            frequencia_sc as (
                select id_empenho_bd, count(id_empenho_bd) as frequencia_id
                from pago_sc
                group by 1
                order by 2 desc
            ),
            pagamento_sc as (
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
                from pago_sc p
                left join frequencia_sc f on p.id_empenho_bd = f.id_empenho_bd
            )

        select *
        from pagamento_mg
        union all
        (select * from pagamento_sp)
        union all
        (select * from pagamento_municipio_sp)
        union all
        (select * from pagamento_pe)
        union all
        (select * from pagamento_pr)
        union all
        (select * from pagamento_rs)
        union all
        (select * from pagamento_pb)
        union all
        (select * from pagamento_ce)
        union all
        (select * from pagamento_municipio_rj_v1)
        union all
        (select * from pagamento_municipio_rj_v2)
        union all
        (select * from pagamento_rj)
        union all
        (select * from pagamento_df)
        union all
        (select * from pagamento_sc)
    )
