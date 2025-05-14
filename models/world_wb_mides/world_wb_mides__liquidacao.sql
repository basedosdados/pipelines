{{
    config(
        alias="liquidacao",
        schema="world_wb_mides",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1995, "end": 2024, "interval": 1},
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
    numero,
    nome_responsavel,
    documento_responsavel,
    indicador_restos_pagar,
    valor_inicial,
    valor_anulacao,
    valor_ajuste,
    valor_final
from
    (
        with
            liquidacao_ce as (
                select
                    (
                        safe_cast(extract(year from date(data_liquidacao)) as int64)
                    ) as ano,
                    (
                        safe_cast(extract(month from date(data_liquidacao)) as int64)
                    ) as mes,
                    safe_cast(
                        extract(date from timestamp(data_liquidacao)) as date
                    ) as data,
                    'CE' as sigla_uf,
                    safe_cast(geoibgeid as string) as id_municipio,
                    safe_cast(codigo_orgao as string) as orgao,
                    safe_cast(codigo_unidade as string) as id_unidade_gestora,
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
                    safe_cast(numero_empenho as string) as numero_empenho,
                    safe_cast(null as string) as id_liquidacao_bd,
                    safe_cast(null as string) as id_liquidacao,
                    safe_cast(null as string) as numero,
                    safe_cast(
                        nome_responsavel_liquidacao as string
                    ) as nome_responsavel,
                    safe_cast(
                        cpf_responsavel_liquidacao_ as string
                    ) as documento_responsavel,
                    safe_cast(null as bool) as indicador_restos_pagar,
                    round(safe_cast(valor_liquidado as float64), 2) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(safe_cast(valor_liquidado as float64), 2) as valor_final,
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_liquidacao_ce"
                        )
                    }} l
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_municipio_ce` m
                    on l.codigo_municipio = m.codigo_municipio
            ),
            liquidacao_mg as (
                select
                    safe_cast(ano as int64) as ano,
                    safe_cast(mes as int64) as mes,
                    safe_cast(data as date) as data,
                    'MG' as sigla_uf,
                    safe_cast(l.id_municipio as string) as id_municipio,
                    safe_cast(l.orgao as string) as orgao,
                    safe_cast(l.id_unidade_gestora as string) as id_unidade_gestora,
                    safe_cast(
                        (
                            case
                                when id_empenho != '-1'
                                then
                                    concat(
                                        id_empenho,
                                        ' ',
                                        l.orgao,
                                        ' ',
                                        l.id_municipio,
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
                            end
                        ) as string
                    ) as id_empenho_bd,
                    safe_cast(
                        (
                            case
                                when id_empenho = '-1'
                                then replace (id_empenho, '-1', id_empenho_origem)
                            end
                        ) as string
                    ) as id_empenho,
                    safe_cast(numero_empenho as string) as numero_empenho,
                    safe_cast(
                        concat(
                            id_liquidacao,
                            ' ',
                            l.orgao,
                            ' ',
                            l.id_municipio,
                            ' ',
                            (right(ano, 2))
                        ) as string
                    ) as id_liquidacao_bd,
                    safe_cast(id_liquidacao as string) as id_liquidacao,
                    safe_cast(numero_liquidacao as string) as numero,
                    safe_cast(nome_responsavel as string) as nome_responsavel,
                    safe_cast(documento_responsavel as string) as documento_responsavel,
                    safe_cast(
                        (case when l.id_rsp != '-1' then 1 else 0 end) as bool
                    ) as indicador_restos_pagar,
                    round(
                        safe_cast(valor_liquidacao_original as float64), 2
                    ) as valor_inicial,
                    round(safe_cast(valor_anulado as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(
                        safe_cast(valor_liquidacao_original as float64)
                        - ifnull(safe_cast(valor_anulado as float64), 0),
                        2
                    ) as valor_final
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_liquidacao_mg"
                        )
                    }} as l
                left join
                    `basedosdados-staging.world_wb_mides_staging.raw_rsp_mg` as r
                    on l.id_rsp = r.id_rsp
            ),
            liquidacao_pb as (
                select
                    safe_cast(dt_ano as int64) as ano,
                    (safe_cast(substring(dt_liquidacao, -7, 2) as int64)) as mes,
                    safe_cast(
                        concat(
                            substring(dt_liquidacao, -4),
                            '-',
                            substring(dt_liquidacao, -7, 2),
                            '-',
                            substring(dt_liquidacao, 1, 2)
                        ) as date
                    ) as data,
                    'PB' as sigla_uf,
                    safe_cast(id_municipio as string) as id_municipio,
                    safe_cast(null as string) as orgao,
                    safe_cast(l.cd_ugestora as string) as id_unidade_gestora,
                    safe_cast(
                        concat(
                            nu_empenho,
                            ' ',
                            l.cd_ugestora,
                            ' ',
                            m.id_municipio,
                            ' ',
                            (right(dt_ano, 2))
                        ) as string
                    ) as id_empenho_bd,
                    safe_cast(null as string) as id_empenho,
                    safe_cast(nu_empenho as string) as numero_empenho,
                    safe_cast(null as string) as id_liquidacao_bd,
                    safe_cast(null as string) as id_liquidacao,
                    safe_cast(nu_liquidacao as string) as numero,
                    safe_cast(null as string) as nome_responsavel,
                    safe_cast(null as string) as documento_responsavel,
                    safe_cast(null as bool) as indicador_restos_pagar,
                    round(safe_cast(vl_liquidacao as float64), 2) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(safe_cast(vl_liquidacao as float64), 2) as valor_final,
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_liquidacao_pb"
                        )
                    }} l
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_municipio_pb` m
                    on l.cd_ugestora = safe_cast(m.id_unidade_gestora as string)
            ),
            liquidacao_pr as (
                select
                    safe_cast(nranoliquidacao as int64) as ano,
                    (safe_cast(extract(month from date(dtliquidacao)) as int64)) as mes,
                    safe_cast(
                        extract(date from timestamp(dtliquidacao)) as date
                    ) as data,
                    'PR' as sigla_uf,
                    safe_cast(id_municipio as string) as id_municipio,
                    safe_cast(cdorgao as string) as orgao,
                    safe_cast(cdunidade as string) as id_unidade_gestora,
                    safe_cast(
                        concat(l.idempenho, ' ', m.id_municipio) as string
                    ) as id_empenho_bd,
                    safe_cast(l.idempenho as string) as id_empenho,
                    safe_cast(nrempenho as string) as numero_empenho,
                    safe_cast(
                        concat(l.idliquidacao, ' ', m.id_municipio) as string
                    ) as id_liquidacao_bd,
                    safe_cast(idliquidacao as string) as id_liquidacao,
                    safe_cast(nrliquidacao as string) as numero,
                    safe_cast(nmliquidante as string) as nome_responsavel,
                    safe_cast(nrdocliquidante as string) as documento_responsavel,
                    safe_cast(null as bool) as indicador_restos_pagar,
                    round(safe_cast(vlliquidacaobruto as float64), 2) as valor_inicial,
                    round(
                        safe_cast(vlliquidacaoestornado as float64), 2
                    ) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(safe_cast(vlliquidacaoliquido as float64), 2) as valor_final,
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_liquidacao_pr"
                        )
                    }} l
                left join
                    basedosdados.br_bd_diretorios_brasil.municipio m
                    on cdibge = id_municipio_6
                left join
                    `basedosdados-staging.world_wb_mides_staging.raw_empenho_pr` e
                    on l.idempenho = e.idempenho
            ),
            liquidacao_pe as (
                select
                    safe_cast(l.anoreferencia as int64) as ano,
                    (safe_cast(extract(month from date(data)) as int64)) as mes,
                    safe_cast(extract(date from timestamp(data)) as date) as data,
                    'PE' as sigla_uf,
                    safe_cast(codigoibge as string) as id_municipio,
                    safe_cast(null as string) orgao,
                    safe_cast(id_unidadegestora as string) as id_unidade_gestora,
                    safe_cast(null as string) as id_empenho_bd,
                    safe_cast(trim(idempenho) as string) as id_empenho,
                    safe_cast(l.numeroempenho as string) as numero_empenho,
                    safe_cast(null as string) as id_liquidacao_bd,
                    safe_cast(null as string) as id_liquidacao,
                    safe_cast(null as string) as numero,
                    safe_cast(null as string) as nome_responsavel,
                    safe_cast(null as string) as documento_responsavel,
                    safe_cast(null as bool) as indicador_restos_pagar,
                    round(safe_cast(valor as float64), 2) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(safe_cast(valor as float64), 2) as valor_final,
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_liquidacao_pe"
                        )
                    }} l
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_municipio_pe` m
                    on l.id_unidade_gestora = safe_cast(m.id_unidadegestora as string)
            ),
            liquidado_rs as (
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
                    'RS' as sigla_uf,
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
                    safe_cast(nr_liquidacao as string) as numero,
                    safe_cast(null as string) as nome_responsavel,
                    safe_cast(null as string) as documento_responsavel,
                    safe_cast(null as bool) as indicador_restos_pagar,
                    safe_cast(vl_liquidacao as float64) as valor_inicial
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_despesa_rs") }}
                    as c
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_orgao_rs` as a
                    on c.cd_orgao = a.cd_orgao
                left join
                    `basedosdados.br_bd_diretorios_brasil.municipio` m
                    on m.id_municipio = a.id_municipio
                where tipo_operacao = 'L' and (safe_cast(vl_liquidacao as float64) >= 0)
                group by 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
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
                    -1 * sum(safe_cast(vl_liquidacao as float64)) as valor_anulacao
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_despesa_rs") }}
                    as c
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_orgao_rs` as a
                    on c.cd_orgao = a.cd_orgao
                left join
                    `basedosdados.br_bd_diretorios_brasil.municipio` m
                    on m.id_municipio = a.id_municipio
                where tipo_operacao = 'L' and (safe_cast(vl_liquidacao as float64) < 0)
                group by 1
            ),
            frequencia_rs as (
                select id_empenho_bd, count(id_empenho_bd) as frequencia_id
                from liquidado_rs
                group by 1
            ),
            liquidacao1_rs as (
                select
                    ano,
                    mes,
                    data,
                    sigla_uf,
                    id_municipio,
                    orgao,
                    id_unidade_gestora,
                    l.id_empenho_bd,
                    id_empenho,
                    numero_empenho,
                    id_liquidacao_bd,
                    id_liquidacao,
                    numero,
                    nome_responsavel,
                    documento_responsavel,
                    indicador_restos_pagar,
                    sum(valor_inicial) as valor_inicial,
                    sum(valor_anulacao / frequencia_id) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    sum(
                        valor_inicial - ifnull((valor_anulacao / frequencia_id), 0)
                    ) as valor_final
                from liquidado_rs l
                left join estorno_rs e on l.id_empenho_bd = e.id_empenho_bd
                left join frequencia_rs f on l.id_empenho_bd = f.id_empenho_bd
                group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
            ),
            data_rs as (
                select
                    id_liquidacao_bd,
                    case when (count(distinct data)) > 1 then 1 else 0 end as ddata
                from liquidacao1_rs
                group by 1
            ),
            liquidacao_rs as (
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
                    case
                        when ddata = 1
                        then (safe_cast(null as string))
                        else l.id_liquidacao_bd
                    end as id_liquidacao_bd,
                    id_liquidacao,
                    numero,
                    nome_responsavel,
                    documento_responsavel,
                    indicador_restos_pagar,
                    round(valor_inicial, 2),
                    round(ifnull(valor_anulacao, 0), 2),
                    valor_ajuste,
                    round(valor_final, 2)
                from liquidacao1_rs l
                left join data_rs d on l.id_liquidacao_bd = d.id_liquidacao_bd
            ),
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
                where tp_despesa = 'Valor Liquidado'
            ),
            frequencia as (
                select id_empenho_bd, count(id_empenho_bd) as frequencia_id
                from liquidado_sp
                group by 1
                order by 2 desc
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
                    case
                        when (count(distinct elemento_despesa)) > 1 then 1 else 0
                    end as delem
                from liquidado_sp
                group by 1
            ),
            dummies as (
                select
                    o.id_empenho_bd,
                    dorgao,
                    dmod,
                    ddesc,
                    dfun,
                    dsubf,
                    dprog,
                    dacao,
                    delem
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
            ),
            liquidado_municipio_rj_v1 as (
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
                left join
                    anulacao_municipio_rj_v2 a on l.id_empenho_bd = a.id_empenho_bd
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
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_liquidacao_rj"
                        )
                    }}
                where numero_empenho is not null
            ),
            liquidacao_df as (
                select
                    (safe_cast(exercicio as int64)) as ano,
                    (safe_cast(extract(month from date(emissao)) as int64)) as mes,
                    safe_cast(emissao as date) as data,
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
                    safe_cast(nota_lancamento as string) as numero,
                    safe_cast(credor as string) as nome_responsavel,
                    safe_cast(cnpj_cpf_credor as string) as documento_responsavel,
                    safe_cast(null as bool) as indicador_restos_pagar,
                    round(safe_cast(0 as float64), 2) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(
                        safe_cast(replace(valor, ',', '.') as float64), 2
                    ) as valor_inicial,
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_liquidacao_df"
                        )
                    }}
            ),
            liquidado_sc as (
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
                    safe_cast(null as string) as numero,
                    safe_cast(null as string) as nome_responsavel,
                    safe_cast(null as string) as documento_responsavel,
                    safe_cast(null as bool) as indicador_restos_pagar,
                    round(safe_cast(0 as float64), 2) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_reforco,
                    round(safe_cast(0 as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(safe_cast(valor_liquidacao as float64), 2) as valor_final
                from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_sc") }}
            ),
            frequencia_sc as (
                select id_empenho_bd, count(id_empenho_bd) as frequencia_id
                from liquidado_sc
                group by 1
                order by 2 desc
            ),
            liquidacao_sc as (
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
                from liquidado_sc l
                left join frequencia_sc f on l.id_empenho_bd = f.id_empenho_bd
            )

        select *
        from liquidacao_mg
        union all
        (select * from liquidacao_sp)
        union all
        (select * from liquidacao_municipio_sp)
        union all
        (select * from liquidacao_pe)
        union all
        (select * from liquidacao_pr)
        union all
        (select * from liquidacao_rs)
        union all
        (select * from liquidacao_pb)
        union all
        (select * from liquidacao_ce)
        union all
        (select * from liquidacao_municipio_rj_v1)
        union all
        (select * from liquidacao_municipio_rj_v2)
        union all
        (select * from liquidacao_rj)
        union all
        (select * from liquidacao_df)
        union all
        (select * from liquidacao_sc)
    )
