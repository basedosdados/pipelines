{{
    config(
        alias="empenho",
        schema="world_wb_mides",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1994, "end": 2024, "interval": 1},
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
    id_licitacao_bd,
    id_licitacao,
    modalidade_licitacao,
    id_empenho_bd,
    id_empenho,
    numero,
    descricao,
    modalidade,
    funcao,
    subfuncao,
    programa,
    acao,
    elemento_despesa,
    valor_inicial,
    valor_reforco,
    valor_anulacao,
    valor_ajuste,
    valor_final
from
    (
        with
            empenhado_ce as (
                select
                    (
                        safe_cast(
                            extract(year from date(data_emissao_empenho)) as int64
                        )
                    ) as ano,
                    (
                        safe_cast(
                            extract(month from date(data_emissao_empenho)) as int64
                        )
                    ) as mes,
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
                    safe_cast(
                        safe_cast(codigo_subfuncao as int64) as string
                    ) as subfuncao,
                    safe_cast(
                        safe_cast(codigo_programa as int64) as string
                    ) as programa,
                    safe_cast(
                        safe_cast(codigo_projeto_atividade as int64) as string
                    ) as acao,
                    safe_cast(
                        safe_cast(codigo_elemento_despesa as int64) as string
                    ) as modalidade_despesa,
                    round(safe_cast(valor_empenhado as float64), 2) as valor_inicial,
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_empenho_ce") }} e
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
                    round(
                        sum(safe_cast(valor_anulacao as float64)), 2
                    ) as valor_anulacao
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_anulacao_ce") }}
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
            ),
            empenhado_mg as (
                select
                    safe_cast(ano as int64) as ano,
                    safe_cast(mes as int64) as mes,
                    safe_cast(data as date) as data,
                    'MG' as sigla_uf,
                    safe_cast(id_municipio as string) as id_municipio,
                    safe_cast(trim(orgao) as string) as orgao,
                    safe_cast(id_unidade_gestora as string) as id_unidade_gestora,
                    safe_cast(null as string) as id_licitacao_bd,
                    safe_cast(id_licitacao as string) as id_licitacao,
                    safe_cast(null as string) as modalidade_licitacao,
                    safe_cast(
                        concat(
                            id_empenho,
                            ' ',
                            orgao,
                            ' ',
                            id_municipio,
                            ' ',
                            (right(ano, 2))
                        ) as string
                    ) as id_empenho_bd,
                    safe_cast(id_empenho as string) as id_empenho,
                    safe_cast(numero_empenho as string) as numero,
                    safe_cast(lower(descricao) as string) as descricao,
                    safe_cast(substring(dsc_modalidade, 5, 1) as string) as modalidade,
                    safe_cast(cast(left(dsc_funcao, 2) as int64) as string) as funcao,
                    safe_cast(
                        cast(left(dsc_subfuncao, 3) as int64) as string
                    ) as subfuncao,
                    safe_cast(
                        cast(left(dsc_programa, 4) as int64) as string
                    ) as programa,
                    safe_cast(cast(left(dsc_acao, 4) as int64) as string) as acao,
                    safe_cast(
                        replace(left(elemento_despesa, 12), '.', '') as string
                    ) as elemento_despesa,
                    round(
                        safe_cast(valor_empenho_original as float64), 2
                    ) as valor_inicial,
                    round(
                        safe_cast(
                            ifnull(safe_cast(valor_reforco as float64), 0) as float64
                        ),
                        2
                    ) as valor_reforco,
                    round(
                        safe_cast(
                            ifnull(safe_cast(valor_anulacao as float64), 0) as float64
                        ),
                        2
                    ) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(
                        safe_cast(valor_empenho_original as float64) + safe_cast(
                            ifnull(safe_cast(valor_reforco as float64), 0) as float64
                        )
                        - safe_cast(
                            ifnull(safe_cast(valor_anulacao as float64), 0) as float64
                        ),
                        2
                    ) as valor_final
                from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_mg") }}
            ),
            dlic as (
                select
                    id_empenho_bd,
                    case
                        when (count(distinct id_licitacao)) > 1 then 1 else 0
                    end as dlic
                from empenhado_mg
                group by 1
            ),
            empenho_mg as (
                select distinct
                    e.ano,
                    e.mes,
                    e.data,
                    e.sigla_uf,
                    e.id_municipio,
                    e.orgao,
                    e.id_unidade_gestora,
                    e.id_licitacao_bd,
                    case
                        when dlic = 1
                        then (safe_cast(null as string))
                        else e.id_licitacao
                    end as id_licitacao,
                    e.modalidade_licitacao,
                    e.id_empenho_bd,
                    e.id_empenho,
                    e.numero,
                    e.descricao,
                    e.modalidade,
                    e.funcao,
                    e.subfuncao,
                    e.programa,
                    e.acao,
                    e.elemento_despesa,
                    e.valor_inicial,
                    e.valor_reforco,
                    e.valor_anulacao,
                    e.valor_ajuste,
                    e.valor_final
                from empenhado_mg e
                left join dlic l on l.id_empenho_bd = e.id_empenho_bd
            ),
            empenhado_pb as (
                select
                    safe_cast(dt_ano as int64) as ano,
                    safe_cast(substring(trim(dt_empenho), -7, 2) as int64) as mes,
                    safe_cast(
                        concat(
                            substring(trim(dt_empenho), -4),
                            '-',
                            substring(trim(dt_empenho), -7, 2),
                            '-',
                            substring(trim(dt_empenho), 1, 2)
                        ) as date
                    ) as data,
                    'PB' as sigla_uf,
                    safe_cast(m.id_municipio as string) as id_municipio,
                    safe_cast(e.cd_ugestora as string) as orgao,
                    safe_cast(null as string) as id_unidade_gestora,
                    safe_cast(null as string) as id_licitacao_bd,
                    safe_cast(null as string) as id_licitacao,
                    safe_cast(null as string) as modalidade_licitacao,
                    safe_cast(
                        concat(
                            nu_empenho,
                            ' ',
                            e.cd_ugestora,
                            ' ',
                            m.id_municipio,
                            ' ',
                            (right(dt_ano, 2))
                        ) as string
                    ) as id_empenho_bd,
                    safe_cast(null as string) as id_empenho,
                    safe_cast(nu_empenho as string) as numero,
                    safe_cast(lower(de_historico) as string) as descricao,
                    safe_cast(null as string) as modalidade,
                    safe_cast(safe_cast(funcao as int64) as string) as funcao,
                    safe_cast(safe_cast(subfuncao as int64) as string) as subfuncao,
                    safe_cast(de_programa as string) as programa,  -- substituir por código
                    safe_cast(de_acao as string) as acao,  -- substituir por código
                    concat(
                        case
                            when de_cateconomica = 'Despesa Corrente'
                            then '3'
                            when de_cateconomica = 'Despesa de Capital'
                            then '4'
                            when de_cateconomica = 'Reserva de Contingência'
                            then '9'
                        end,
                        case
                            when de_natdespesa = 'Pessoal e Encargos Sociais'
                            then '1'
                            when de_natdespesa = 'Juros e Encargos da Dívida'
                            then '2'
                            when de_natdespesa = 'Outras Despesas Correntes'
                            then '3'
                            when de_natdespesa = 'Investimentos'
                            then '4'
                            when de_natdespesa = 'Inversões Financeiras'
                            then '5'
                            when de_natdespesa = 'Amortização da Dívida'
                            then '6'
                            when de_natdespesa = 'Reserva de Contingência'
                            then '9'
                        end,
                        case
                            when de_modalidade = 'Transferências à União'
                            then '20'
                            when
                                de_modalidade
                                = 'Transferências a Instituições Privadas com Fins Lucrativos'
                            then '30'
                            when
                                de_modalidade
                                = 'Execução Orçamentária Delegada a Estados e ao Distrito Federal'
                            then '32'
                            when
                                de_modalidade
                                = 'Aplicação Direta §§ 1º e 2º do Art. 24 LC 1412'
                            then '35'
                            when de_modalidade = 'Aplicação Direta Art. 25 LC 141'
                            then '36'
                            when de_modalidade = 'Transferências a Municípios'
                            then '40'
                            when
                                de_modalidade
                                = 'Transferências a Municípios – Fundo a Fundo'
                            then '41'
                            when
                                de_modalidade
                                = 'Transferências a Instituições Privadas sem Fins Lucrativos'
                            then '50'
                            when
                                de_modalidade
                                = 'Transferências a Instituições Privadas com Fins Lucrativos'
                            then '60'
                            when
                                de_modalidade
                                = 'Transferências a Instituições Multigovernamentais'
                            then '70'
                            when
                                de_modalidade
                                = 'Transf. a Consórc Púb. C.Rateio §§ 1º e 2º Art. 24  LC141'
                            then '71'
                            when
                                de_modalidade
                                = 'Execução Orçamentária Delegada a Consórcios Públicos'
                            then '72'
                            when de_modalidade = 'Transferências a Consórcios Públicos'
                            then '73'
                            when
                                de_modalidade
                                = 'Transf. a Consórc Púb. C.Rateio Art. 25 LC 141'
                            then '74'
                            when de_modalidade = 'Transferências ao Exterior'
                            then '80'
                            when de_modalidade = 'Aplicações Diretas'
                            then '90'
                            when
                                de_modalidade
                                = 'Ap. Direta Decor. de Op. entre Órg., Fundos e Ent. Integ. dos Orçamentos Fiscal e da Seguridade Social'
                            then '91'
                            when
                                de_modalidade
                                = ' Aplicação Direta Decor. de Oper. de Órgãos, Fundos e Entid. Integr. dos Orç. Fiscal e da Seguri. Social com Cons. Públ. do qual o Ente Participe'
                            then '93'
                            when
                                de_modalidade
                                = ' Aplicação Direta Decor. de Oper. de Órgãos, Fundos e Entid. Integr. dos Orç. Fiscal e da Seguri. Social com Cons. Públ. do qual o Ente Não Participe'
                            then '94'
                            else null
                        end,
                        cd_elemento
                    ) as elemento_despesa,
                    safe_cast(vl_empenho as float64) as valor_inicial
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_empenho_pb") }} e
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_municipio_pb` m
                    on e.cd_ugestora = safe_cast(m.id_unidade_gestora as string)
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_funcao` f
                    on e.de_funcao = f.nome_funcao
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_subfuncao` sf
                    on e.de_subfuncao = sf.nome_subfuncao
            ),
            anulacao_pb as (
                select
                    safe_cast(
                        concat(
                            nu_empenho,
                            ' ',
                            a.cd_ugestora,
                            ' ',
                            m.id_municipio,
                            ' ',
                            (right(dt_ano, 2))
                        ) as string
                    ) as id_empenho_bd,
                    sum(safe_cast(vl_estorno as float64)) as valor_anulacao
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_estorno_pb") }} a
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_municipio_pb` m
                    on a.cd_ugestora = safe_cast(m.id_unidade_gestora as string)
                group by 1
            ),
            frequencia_pb as (
                select id_empenho_bd, count(id_empenho_bd) as frequencia_id
                from empenhado_pb
                group by 1
            ),
            empenho_completo as (
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
                    e.id_empenho_bd,
                    e.id_empenho,
                    e.numero,
                    e.descricao,
                    e.modalidade,
                    e.funcao,
                    e.subfuncao,
                    e.programa,
                    e.acao,
                    e.elemento_despesa,
                    frequencia_id,
                    round(sum(e.valor_inicial), 2) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_reforco,
                    round(sum(a.valor_anulacao / frequencia_id), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                from empenhado_pb e
                full outer join anulacao_pb a on a.id_empenho_bd = e.id_empenho_bd
                left join frequencia_pb f on f.id_empenho_bd = e.id_empenho_bd
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
                    20,
                    21
            ),
            empenho_pb as (
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
                    case
                        when (frequencia_id > 1)
                        then (safe_cast(null as string))
                        else e.id_empenho_bd
                    end as id_empenho_bd,
                    e.id_empenho,
                    e.numero,
                    e.descricao,
                    e.modalidade,
                    e.funcao,
                    e.subfuncao,
                    e.programa,
                    e.acao,
                    e.elemento_despesa,
                    e.valor_inicial,
                    e.valor_reforco,
                    e.valor_anulacao,
                    e.valor_ajuste,
                    round(e.valor_inicial - ifnull(valor_anulacao, 0), 2) as valor_final
                from empenho_completo e
            ),
            empenho_pe as (
                select
                    safe_cast(e.anoreferencia as int64) as ano,
                    (safe_cast(extract(month from date(dataempenho)) as int64)) as mes,
                    safe_cast(
                        extract(date from timestamp(dataempenho)) as date
                    ) as data,
                    'PE' as sigla_uf,
                    safe_cast(codigoibge as string) as id_municipio,
                    safe_cast(null as string) orgao,
                    safe_cast(id_unidadegestora as string) as id_unidade_gestora,
                    safe_cast(null as string) id_licitacao_bd,
                    safe_cast(null as string) id_licitacao,
                    safe_cast(null as string) modalidade_licitacao,
                    safe_cast(null as string) as id_empenho_bd,
                    safe_cast(trim(id_empenho) as string) as id_empenho,
                    safe_cast(e.numeroempenho as string) as numero,
                    safe_cast(lower(historico) as string) as descricao,
                    safe_cast(left(tipo_empenho, 1) as string) as modalidade,
                    safe_cast(safe_cast(fun.funcao as int64) as string) as funcao,
                    safe_cast(safe_cast(sub.subfuncao as int64) as string) as subfuncao,
                    safe_cast(programa as string) as programa,
                    safe_cast(codigo_tipo_acao as string) as acao,
                    concat(
                        case
                            when categoria = 'Despesa Corrente'
                            then '3'
                            when categoria = 'Despesa de Capital'
                            then '4'
                        end,
                        case
                            when natureza = 'Pessoal e Encargos Sociais'
                            then '1'
                            when natureza = 'Juros e Encargos da Dívida'
                            then '2'
                            when natureza = 'Outras Despesas Correntes'
                            then '3'
                            when natureza = 'Investimentos'
                            then '4'
                            when natureza = 'Inversões Financeiras'
                            then '5'
                            when natureza = 'Amortização da Dívida'
                            then '6'
                            when natureza = 'Reserva de Contingência'
                            then '9'
                        end,
                        case
                            when modalidade = 'Transferências à União'
                            then '20'
                            when
                                modalidade
                                = 'Transferências a Instituições Privadas com Fins Lucrativos'
                            then '30'
                            when
                                modalidade
                                = 'Execução Orçamentária Delegada a Estados e ao Distrito Federal'
                            then '32'
                            when
                                modalidade
                                = 'Aplicação Direta à conta de recursos de que tratam os §§ 1o e 2o do art. 24 da Lei Complementar no 141, de 2012'
                            then '35'
                            when
                                modalidade
                                = 'Aplicação Direta à conta de recursos de que trata o art. 25 da Lei Complementar no 141, de 2012'
                            then '36'
                            when modalidade = 'Transferências a Municípios'
                            then '40'
                            when
                                modalidade
                                = 'Transferências a Municípios – Fundo a Fundo'
                            then '41'
                            when
                                modalidade
                                = 'Transferências a Instituições Privadas sem Fins Lucrativos'
                            then '50'
                            when
                                modalidade
                                = 'Transferências a Instituições Privadas com Fins Lucrativos'
                            then '60'
                            when
                                modalidade
                                = 'Transferências a Instituições Multigovernamentais'
                            then '70'
                            when
                                modalidade
                                = 'Transferências a Consórcios Públicos mediante contrato de rateio à conta de recursos de que tratam os §§ 1o e 2o do art. 24 da Lei Complementar no 141, de 2012'
                            then '71'
                            when
                                modalidade
                                = 'Execução Orçamentária Delegada a Consórcios Públicos'
                            then '72'
                            when modalidade = 'Transferências a Consórcios Públicos'
                            then '73'
                            when modalidade = 'Transferências ao Exterior'
                            then '80'
                            when modalidade = 'Aplicações Diretas'
                            then '90'
                            when
                                modalidade
                                = 'Ap. Direta Decor. de Op. entre Órg., Fundos e Ent. Integ. dos Orçamentos Fiscal e da Seguridade Social'
                            then '91'
                            when
                                modalidade
                                = ' Aplicação Direta Decor. de Oper. de Órgãos, Fundos e Entid. Integr. dos Orç. Fiscal e da Seguri. Social com Cons. Públ. do qual o Ente Participe'
                            then '93'
                            when
                                modalidade
                                = ' Aplicação Direta Decor. de Oper. de Órgãos, Fundos e Entid. Integr. dos Orç. Fiscal e da Seguri. Social com Cons. Públ. do qual o Ente Não Participe'
                            then '94'
                            else null
                        end,
                        case
                            when elementodespesa = 'Pensões do RPPS e do militar'
                            then '03'
                            when elementodespesa = 'Contratação por Tempo Determinado'
                            then '04'
                            when
                                elementodespesa
                                = 'Outros Benefícios Previdenciários do RPPS'
                            then '05'
                            when
                                elementodespesa
                                = 'Outros Benefícios Previdenciários do servidor ou do militar'
                            then '05'
                            when
                                elementodespesa
                                = 'Beneficio Mensal ao Deficiente e ao Idoso'
                            then '06'
                            when
                                elementodespesa
                                = 'Contribuição a Entidades Fechadas de Previdência'
                            then '07'
                            when elementodespesa = 'Outros Benefícios Assistenciais'
                            then '08'
                            when
                                elementodespesa
                                = 'Outros Benefícios Assistenciais do servidor e do militar'
                            then '08'
                            when elementodespesa = 'Salário Família'
                            then '09'
                            when elementodespesa = 'Seguro Desemprego e Abono Salarial'
                            then '10'
                            when
                                elementodespesa
                                = 'Vencimentos e Vantagens Fixas - Pessoal Civil'
                            then '11'
                            when
                                elementodespesa
                                = 'Vencimentos e Vantagens Fixas - Pessoal Militar'
                            then '12'
                            when elementodespesa = 'Obrigações Patronais'
                            then '13'
                            when
                                elementodespesa
                                = 'Aporte para Cobertura do Déficit Atuarial do RPPS'
                            then '13'
                            when elementodespesa = 'Diárias - Civil'
                            then '14'
                            when
                                elementodespesa
                                = 'Outras Despesas Variáveis - Pessoal Civil'
                            then '16'
                            when elementodespesa = 'Auxílio Financeiro a Estudantes'
                            then '18'
                            when elementodespesa = 'Auxílio Fardamento'
                            then '19'
                            when elementodespesa = 'Auxílio Financeiro a Pesquisadores'
                            then '20'
                            when
                                elementodespesa
                                = 'Outros Encargos sobre a Dívida por Contrato'
                            then '22'
                            when
                                elementodespesa
                                = 'Juros, Deságios e Descontos da Dívida Mobiliária'
                            then '23'
                            when
                                elementodespesa
                                = 'Outros Encargos sobre a Dívida Mobiliária'
                            then '24'
                            when
                                elementodespesa
                                = 'Encargos sobre Operações de Crédito por Antecipação da Receita'
                            then '25'
                            when
                                elementodespesa
                                = 'Encargos pela Honra de Avais, Garantias, Seguros e Similares'
                            then '27'
                            when
                                elementodespesa
                                = 'Remuneração de Cotas de Fundos Autárquicos'
                            then '28'
                            when elementodespesa = 'Material de Consumo'
                            then '30'
                            when
                                elementodespesa
                                = 'Premiações Culturais, Artísticas, Científicas, Desportivas e Outras'
                            then '31'
                            when
                                elementodespesa
                                = 'Material, Bem ou Serviço para Distribuição Gratuita'
                            then '32'
                            when elementodespesa = 'Passagens e Despesas de Locomoção'
                            then '33'
                            when
                                elementodespesa
                                = 'Outras Despesas de Pessoal decorrentes de Contratos de Terceirização'
                            then '34'
                            when elementodespesa = 'Serviços de Consultoria'
                            then '35'
                            when elementodespesa = 'Locação de Mão-de-Obra'
                            then '37'
                            when
                                elementodespesa
                                = 'Outros Serviços de Terceiros ? Pessoa Jurídica'
                            then '39'
                            when
                                elementodespesa
                                = 'Serviços de Tecnologia da Informação e Comunicação - Pessoa Jurídica'
                            then '40'
                            when
                                elementodespesa
                                = 'Serviços de Tecnologia da Informação e Comunicação ? Pessoa Jurídica'
                            then '40'
                            when elementodespesa = 'Contribuições'
                            then '41'
                            when elementodespesa = 'Auxílios'
                            then '42'
                            when
                                elementodespesa
                                = 'Obrigações Tributárias e Contributivas'
                            then '47'
                            when elementodespesa = 'Auxílio-Transporte'
                            then '49'
                            when elementodespesa = 'Obras e Instalações'
                            then '51'
                            when elementodespesa = 'Equipamentos e Material Permanente'
                            then '52'
                            when
                                elementodespesa = 'Aposentadorias do RGPS ? Área Urbana'
                            then '54'
                            when elementodespesa = 'Pensões, exclusiva do RGPS'
                            then '56'
                            when
                                elementodespesa
                                = 'Outros Benefícios do RGPS ? Área Urbana'
                            then '58'
                            when elementodespesa = 'Pensões Especiais'
                            then '59'
                            when elementodespesa = 'Aquisição de Imóveis'
                            then '61'
                            when
                                elementodespesa
                                = 'Constituição ou Aumento de Capital de Empresas'
                            then '65'
                            when
                                elementodespesa
                                = 'Concessão de Empréstimos e Financiamentos'
                            then '66'
                            when elementodespesa = 'Depósitos Compulsórios'
                            then '67'
                            when
                                elementodespesa
                                = 'Rateio pela Participação em Consórcio Público'
                            then '70'
                            when
                                elementodespesa
                                = 'Principal da Dívida Contratual Resgatado'
                            then '71'
                            when
                                elementodespesa
                                = 'Principal da Dívida Mobiliária Resgatado'
                            then '72'
                            when
                                elementodespesa
                                = 'Correção Monetária ou Cambial da Dívida Contratual Resgatada'
                            then '73'
                            when
                                elementodespesa
                                = 'Principal Corrigido da Dívida Contratual Refinanciado'
                            then '77'
                            when
                                elementodespesa
                                = 'Distribuição Constitucional ou Legal de Receitas'
                            then '81'
                            when elementodespesa = 'Sentenças Judiciais'
                            then '91'
                            when elementodespesa = 'Despesas de Exercícios Anteriores'
                            then '92'
                            when elementodespesa = 'Indenizações e Restituições'
                            then '93'
                            when
                                elementodespesa
                                = 'Indenização pela Execução de Trabalhos de Campo'
                            then '95'
                            when
                                elementodespesa
                                = 'Ressarcimento de Despesas de Pessoal Requisitado'
                            then '96'
                            else null
                        end
                    ) as elemento_despesa,
                    round(safe_cast(0 as float64), 2) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_reforco,
                    round(safe_cast(0 as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(safe_cast(valorempenhado as float64), 2) as valor_final
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_empenho_pe") }} e
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_municipio_pe` m
                    on e.nomeunidadegestora = m.nomeunidadegestora
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_funcao` fun
                    on upper(
                        trim(
                            replace(
                                replace(
                                    e.funcao, 'Encargos Especias', 'Encargos Especiais'
                                ),
                                'Assistêncial Social',
                                'Assistência Social'
                            )
                        )
                    )
                    = upper(nome_funcao)
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_subfuncao` sub
                    on upper(trim(e.subfuncao)) = upper(nome_subfuncao)
            ),
            empenho_pr as (
                select
                    safe_cast(nranoempenho as int64) as ano,
                    (safe_cast(extract(month from date(dtempenho)) as int64)) as mes,
                    safe_cast(extract(date from timestamp(dtempenho)) as date) as data,
                    'PR' as sigla_uf,
                    safe_cast(m.id_municipio as string) as id_municipio,
                    safe_cast(trim(cdorgao, '0') as string) as orgao,
                    safe_cast(cdunidade as string) as id_unidade_gestora,
                    safe_cast(null as string) as id_licitacao_bd,
                    safe_cast(null as string) as id_licitacao,
                    safe_cast(null as string) as modalidade_licitacao,
                    safe_cast(
                        concat(idempenho, ' ', m.id_municipio) as string
                    ) as id_empenho_bd,
                    safe_cast(idempenho as string) as id_empenho,
                    safe_cast(nrempenho as string) as numero,
                    safe_cast(lower(dshistorico) as string) as descricao,
                    safe_cast(left(dstipoempenho, 1) as string) as modalidade,
                    safe_cast(safe_cast(cdfuncao as int64) as string) as funcao,
                    safe_cast(safe_cast(cdsubfuncao as int64) as string) as subfuncao,
                    safe_cast(safe_cast(cdprograma as int64) as string) as programa,
                    safe_cast(safe_cast(cdprojetoatividade as int64) as string) as acao,
                    safe_cast(
                        concat(
                            cdcategoriaeconomica,
                            cdgruponatureza,
                            cdmodalidade,
                            cdelemento
                        ) as string
                    ) as elemento_despesa,
                    round(safe_cast(vlempenho as float64), 2) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_reforco,
                    round(safe_cast(vlestornoempenho as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(
                        safe_cast(vlempenho as float64)
                        - ifnull(safe_cast(vlestornoempenho as float64), 0),
                        2
                    ) as valor_final
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_empenho_pr") }} e
                left join
                    basedosdados.br_bd_diretorios_brasil.municipio m
                    on e.cdibge = m.id_municipio_6
            ),
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
                    safe_cast(
                        replace(cd_elemento, '.', '') as string
                    ) as elemento_despesa,
                    safe_cast(vl_empenho as float64) as valor_inicial
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_despesa_rs") }}
                    as c
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_orgao_rs` as a
                    on c.cd_orgao = a.cd_orgao
                left join
                    `basedosdados.br_bd_diretorios_brasil.municipio` m
                    on m.id_municipio = a.id_municipio
                where tipo_operacao = 'E' and (safe_cast(vl_empenho as float64) >= 0)
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
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_despesa_rs") }}
                    as c
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_orgao_rs` as a
                    on c.cd_orgao = a.cd_orgao
                left join
                    `basedosdados.br_bd_diretorios_brasil.municipio` m
                    on m.id_municipio = a.id_municipio
                where tipo_operacao = 'E' and (safe_cast(vl_empenho as float64) < 0)
                group by 1
            ),
            empenho_anulacao as (
                select
                    e.*,
                    f.frequencia_id,
                    a.valor_anulacao / f.frequencia_id as valor_anulacao
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
                    case
                        when (count(distinct elemento_despesa)) > 1 then 1 else 0
                    end as delem
                from empenho_anulacao
                group by 1
            ),
            dummies as (
                select
                    o.id_empenho_bd,
                    dorgao,
                    dugest,
                    ddesc,
                    dfun,
                    dsubf,
                    dprog,
                    dacao,
                    delem
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
            ),
            empenhado_sp as (
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
                    safe_cast(null as string) as id_licitacao_bd,
                    safe_cast(null as string) as id_licitacao,
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
                    safe_cast(nr_empenho as string) as numero,
                    safe_cast(lower(historico_despesa) as string) as descricao,
                    safe_cast(null as string) as modalidade,
                    safe_cast(safe_cast(funcao as int64) as string) as funcao,
                    safe_cast(safe_cast(subfuncao as int64) as string) as subfuncao,
                    safe_cast(safe_cast(cd_programa as int64) as string) as programa,
                    safe_cast(safe_cast(cd_acao as int64) as string) as acao,
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
                where tp_despesa = 'Empenhado'
            ),
            frequencia_sp as (
                select id_empenho_bd, count(id_empenho_bd) as frequencia_id
                from empenhado_sp
                group by 1
                order by 2 desc
            ),
            anulacao as (
                select
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
                    sum(
                        safe_cast(replace(vl_despesa, ',', '.') as float64)
                    ) as valor_anulacao
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_despesa_sp") }} a
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_municipio_sp` m
                    on m.ds_orgao = a.ds_orgao
                where tp_despesa = 'Anulação'
                group by 1
            ),
            reforco as (
                select
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
                    sum(
                        safe_cast(replace(vl_despesa, ',', '.') as float64)
                    ) as valor_reforco
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_despesa_sp") }} r
                left join
                    `basedosdados-staging.world_wb_mides_staging.aux_municipio_sp` m
                    on m.ds_orgao = r.ds_orgao
                where tp_despesa = 'Reforço'
                group by 1
            ),
            empenho_completo_sp as (
                select
                    e.*,
                    r.valor_reforco / frequencia_id as valor_reforco,
                    a.valor_anulacao / frequencia_id as valor_anulacao,
                from empenhado_sp e
                left join frequencia_sp f on e.id_empenho_bd = f.id_empenho_bd
                left join anulacao a on e.id_empenho_bd = a.id_empenho_bd
                left join reforco r on e.id_empenho_bd = r.id_empenho_bd
            ),
            dorgao_sp as (
                select
                    id_empenho_bd,
                    case when (count(distinct orgao)) > 1 then 1 else 0 end as dorgao
                from empenho_completo_sp
                group by 1
            ),
            ddesc_sp as (
                select
                    id_empenho_bd,
                    case
                        when (count(distinct ifnull(descricao, ''))) > 1 then 1 else 0
                    end as ddesc
                from empenho_completo_sp
                group by 1
            ),
            dmod_sp as (
                select
                    id_empenho_bd,
                    case
                        when (count(distinct modalidade_licitacao)) > 1 then 1 else 0
                    end as dmod
                from empenho_completo_sp
                group by 1
            ),
            dfun_sp as (
                select
                    id_empenho_bd,
                    case when (count(distinct funcao)) > 1 then 1 else 0 end as dfun
                from empenho_completo_sp
                group by 1
            ),
            dsubf_sp as (
                select
                    id_empenho_bd,
                    case when (count(distinct subfuncao)) > 1 then 1 else 0 end as dsubf
                from empenho_completo_sp
                group by 1
            ),
            dprog_sp as (
                select
                    id_empenho_bd,
                    case when (count(distinct programa)) > 1 then 1 else 0 end as dprog
                from empenho_completo_sp
                group by 1
            ),
            dacao_sp as (
                select
                    id_empenho_bd,
                    case when (count(distinct acao)) > 1 then 1 else 0 end as dacao
                from empenho_completo_sp
                group by 1
            ),
            delem_sp as (
                select
                    id_empenho_bd,
                    case
                        when (count(distinct elemento_despesa)) > 1 then 1 else 0
                    end as delem
                from empenho_completo_sp
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
                    delem
                from dorgao_sp o
                full outer join dmod_sp m on o.id_empenho_bd = m.id_empenho_bd
                full outer join ddesc_sp d on o.id_empenho_bd = d.id_empenho_bd
                full outer join dfun_sp f on o.id_empenho_bd = f.id_empenho_bd
                full outer join dsubf_sp s on o.id_empenho_bd = s.id_empenho_bd
                full outer join dprog_sp p on o.id_empenho_bd = p.id_empenho_bd
                full outer join dacao_sp a on o.id_empenho_bd = a.id_empenho_bd
                full outer join delem_sp e on o.id_empenho_bd = e.id_empenho_bd
            ),
            empenho_sp as (
                select
                    min(ano) as ano,
                    min(mes) as mes,
                    min(data) as data,
                    sigla_uf,
                    id_municipio,
                    orgao,
                    id_unidade_gestora,
                    id_licitacao_bd,
                    id_licitacao,
                    modalidade_licitacao,
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
                            else e.id_empenho_bd
                        end
                    ) as id_empenho_bd,
                    id_empenho,
                    numero,
                    case
                        when
                            (
                                ddesc = 1
                                and (
                                    dorgao = 0
                                    or dmod = 0
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
                                    or dmod = 1
                                    or dfun = 1
                                    or dsubf = 1
                                    or dprog = 1
                                    or dacao = 1
                                    or delem = 1
                                )
                            )
                        then (safe_cast(e.descricao as string))
                        else e.descricao
                    end as descricao,
                    modalidade,
                    funcao,
                    subfuncao,
                    programa,
                    acao,
                    elemento_despesa,
                    round(sum(valor_inicial), 2) as valor_inicial,
                    round(sum(valor_reforco), 2) as valor_reforco,
                    round(sum(valor_anulacao), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(
                        ifnull(sum(valor_inicial), 0)
                        + ifnull(sum(valor_reforco), 0)
                        - ifnull(sum(valor_anulacao), 0),
                        2
                    ) as valor_final
                from empenho_completo_sp e
                left join dummies_sp d on d.id_empenho_bd = e.id_empenho_bd
                group by 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
            ),
            empenho_municipio_sp as (
                select
                    (safe_cast(exercicio as int64)) as ano,
                    (safe_cast(extract(month from date(data_empenho)) as int64)) as mes,
                    safe_cast(data_empenho as date) as data,
                    'SP' as sigla_uf,
                    '3550308' as id_municipio,
                    safe_cast(codigo_orgao as string) as orgao,
                    safe_cast(codigo_unidade as string) as id_unidade_gestora,
                    safe_cast(null as string) as id_licitacao_bd,
                    safe_cast(null as string) as id_licitacao,
                    safe_cast(null as string) as modalidade_licitacao,
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
                    safe_cast(nr_empenho as string) as numero,
                    safe_cast(observacoes as string) as descricao,
                    safe_cast(
                        left(
                            replace(tipo_empenho, 'Por Estimativa', 'Estimativo'), 1
                        ) as string
                    ) as modalidade,
                    safe_cast(codigo_funcao as string) as funcao,
                    safe_cast(codigo_subfuncao as string) as subfuncao,
                    safe_cast(codigo_programa_governo as string) as programa,
                    safe_cast(codigo_projeto_atividade as string) as acao,
                    safe_cast(codigo_conta_despesa as string) as modalidade_despesa,
                    round(safe_cast(valor_empenho as float64), 2) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_reforco,
                    round(safe_cast(cancelado as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(
                        safe_cast(valor_empenho as float64)
                        - safe_cast(cancelado as float64),
                        2
                    ) as valor_final,
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_despesa_sp_municipio"
                        )
                    }}
            ),
            empenhado_municipio_rj_v1 as (
                select
                    (safe_cast(exercicio_empenho as int64)) as ano,
                    (safe_cast(extract(month from date(data_empenho)) as int64)) as mes,
                    safe_cast(data_empenho as date) as data,
                    'RJ' as sigla_uf,
                    '3304557' as id_municipio,
                    safe_cast(orgao_programa_trabalho as string) as orgao,
                    safe_cast(
                        unidade_programa_trabalho as string
                    ) as id_unidade_gestora,
                    safe_cast(null as string) as id_licitacao_bd,
                    safe_cast(n_mero_licita__o as string) as id_licitacao,
                    case
                        when modalidade_licitacao = 'Convite'
                        then '1'
                        when modalidade_licitacao = 'Tomada De Preços'
                        then '2'
                        when modalidade_licitacao = 'Tomada de Preços'
                        then '2'
                        when modalidade_licitacao = 'Concorrência'
                        then '3'
                        when modalidade_licitacao = 'Pregão'
                        then '4'
                        when modalidade_licitacao = 'Leilão'
                        then '7'
                        when modalidade_licitacao = 'Dispensa'
                        then '8'
                        when modalidade_licitacao = 'Inexigibilidade'
                        then '10'
                        when modalidade_licitacao = 'Concurso'
                        then '11'
                        when modalidade_licitacao = 'Seleção Pública'
                        then '31'
                        when modalidade_licitacao = 'Não Sujeito'
                        then '99'
                    end as modalidade_licitacao,
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
                    safe_cast(nr_empenho as string) as numero,
                    safe_cast(null as string) as descricao,
                    safe_cast(left(especie, 1) as string) as modalidade,
                    safe_cast(
                        cast(substring(programa_trabalho, 7, 2) as int64) as string
                    ) as funcao,
                    safe_cast(
                        cast(substring(programa_trabalho, 10, 3) as int64) as string
                    ) as subfuncao,
                    safe_cast(
                        substring(programa_trabalho, 14, 4) as string
                    ) as programa,
                    safe_cast(substring(programa_trabalho, 19, 4) as string) as acao,
                    safe_cast(
                        safe_cast(natureza_despesa as int64) as string
                    ) as modalidade_despesa,
                    round(safe_cast(valor_empenhado as float64), 2) as valor_final,
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
                from empenhado_municipio_rj_v1
                group by 1
                order by 2 desc
            ),
            empenho_municipio_rj_v1 as (
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
                    round(safe_cast(0 as float64), 2) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_reforco,
                    round(safe_cast(0 as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    e.valor_final as valor_final
                from empenhado_municipio_rj_v1 e
                left join frequencia_rj_v1 f on e.id_empenho_bd = f.id_empenho_bd
            ),
            empenhado_municipio_rj_v2 as (
                select
                    (safe_cast(exercicio as int64)) as ano,
                    (safe_cast(extract(month from date(data)) as int64)) as mes,
                    safe_cast(data as date) as data,
                    'RJ' as sigla_uf,
                    '3304557' as id_municipio,
                    safe_cast(ug as string) as orgao,
                    safe_cast(uo as string) as id_unidade_gestora,
                    safe_cast(null as string) as id_licitacao_bd,
                    safe_cast(null as string) as id_licitacao,
                    case
                        when licitacao = 'CONVITE'
                        then '1'
                        when licitacao = 'TOMADA DE PREÇOS'
                        then '2'
                        when licitacao = 'CONCORRÊNCIA'
                        then '3'
                        when licitacao = 'PREGÃO'
                        then '4'
                        when licitacao = 'PREÇO REGISTRADO/PREGÃO'
                        then '4'
                        when licitacao = 'REGISTRO DE PREÇOS EXTERNO/PREGÃO'
                        then '4'
                        when licitacao = 'DISPENSA'
                        then '8'
                        when licitacao = 'INEXIGIBILIDADE'
                        then '10'
                        when licitacao = 'CONCURSO'
                        then '11'
                        when licitacao = 'SELEÇÃO PÚBLICA'
                        then '31'
                        when licitacao = 'NÃO SUJEITO'
                        then '99'
                    end as modalidade_licitacao,
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
                    safe_cast(empenhoexercicio as string) as numero,
                    safe_cast(historico as string) as descricao,
                    safe_cast(null as string) as modalidade,
                    safe_cast(cast(funcao as int64) as string) as funcao,
                    safe_cast(subfuncao as string) as subfuncao,
                    safe_cast(programa as string) as programa,
                    safe_cast(acao as string) as acao,
                    safe_cast(
                        concat(
                            -- categoria econômica
                            case
                                when grupo = 'PESSOAL E ENCARGOS SOCIAIS'
                                then '3'
                                when grupo = 'JUROS E ENCARGOS DA DIVIDA'
                                then '3'
                                when grupo = 'OUTRAS DESPESAS CORRENTES'
                                then '3'
                                when grupo = 'INVESTIMENTOS'
                                then '4'
                                when grupo = 'INVERSOES FINANCEIRAS'
                                then '4'
                                when grupo = 'AMORTIZACAO DA DIVIDA'
                                then '4'
                            end,
                            -- natureza da despesa
                            case
                                when grupo = 'PESSOAL E ENCARGOS SOCIAIS'
                                then '1'
                                when grupo = 'JUROS E ENCARGOS DA DIVIDA'
                                then '2'
                                when grupo = 'OUTRAS DESPESAS CORRENTES'
                                then '3'
                                when grupo = 'INVESTIMENTOS'
                                then '4'
                                when grupo = 'INVERSOES FINANCEIRAS'
                                then '5'
                                when grupo = 'AMORTIZACAO DA DIVIDA'
                                then '6'
                            end,
                            -- modalidade de aplicação
                            case
                                when modalidade = 'TRANSFERENCIAS A UNIAO'
                                then '20'
                                when
                                    modalidade
                                    = 'TRANSFERENCIAS A ESTADOS E AO DISTRITO FEDERAL'
                                then '30'
                                when
                                    modalidade
                                    = 'TRANSFERENCIAS A INSTITUICOES PRIVADAS SEM FINS LUCRATIVOS'
                                then '50'
                                when
                                    modalidade
                                    = 'TRANSFERENCIAS A INSTITUICOES PRIVADAS COM FINS LUCRATIVOS'
                                then '60'
                                when
                                    modalidade
                                    = 'EXECUCAO DE CONTRATO DE PARCERIA PUBLICO-PRIVADA'
                                then '67'
                                when
                                    modalidade
                                    = 'EXECUCAO DE CONTRATO DE PARCERIA PUBLICO-PRIVADA - PPP'
                                then '67'
                                when
                                    modalidade
                                    = 'EXECUCAO DE CONTRATO DE PARCERIA PUBLICO PRIVADA - PPP'
                                then '67'
                                when
                                    modalidade
                                    = 'DESP. DECORRENTES DA PART. EM FUNDOS, ORGANISMOS OU ENTIDADES ASSEMELHADAS NAC. E INTERN.'
                                then '84'
                                when modalidade = 'APLICACOES DIRETAS'
                                then '90'
                                when
                                    modalidade
                                    = 'APLIC. DIRETA DECOR. DE OPER. ENTRE ORG., FUNDOS E ENTID. INTEG. DO ORC. FISC. E SEG. SOC.'
                                then '91'
                                when
                                    modalidade
                                    = 'APLIC DIRETAS DECOR DE OPER ENTRE ORG, FUNDOS E ENTID INTEGRANTES DOS ORC FISC E SEG SOC'
                                then '91'
                                else null
                            end,
                            -- elemento e item da despesa
                            elemento,
                            subelemento
                        ) as string
                    ) as elemento_despesa,
                    round(safe_cast(valor as float64), 2) as valor_inicial,
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_despesa_ato_rj_municipio"
                        )
                    }}
                where tipoato = 'EMPENHO'
            ),
            anulacao_municipio_rj_v2 as (
                select
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
                    round(sum(safe_cast(valor as float64)), 2) as valor_anulacao,
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_despesa_ato_rj_municipio"
                        )
                    }}
                where tipoato = 'CANCELAMENTO EMPENHO'
                group by 1
            ),
            empenho_municipio_rj_v2 as (
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
                    e.id_empenho_bd,
                    e.id_empenho,
                    e.numero,
                    e.descricao,
                    e.modalidade,
                    e.funcao,
                    e.subfuncao,
                    e.programa,
                    e.acao,
                    e.elemento_despesa,
                    e.valor_inicial as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_reforco,
                    safe_cast(ifnull(a.valor_anulacao, 0) as float64) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(
                        safe_cast(
                            (e.valor_inicial - ifnull(a.valor_anulacao, 0)) as float64
                        ),
                        2
                    ) as valor_final
                from empenhado_municipio_rj_v2 e
                left join
                    anulacao_municipio_rj_v2 a on e.id_empenho_bd = a.id_empenho_bd
            ),
            empenhado_rj as (
                select
                    (safe_cast(ano as int64)) as ano,
                    (safe_cast(extract(month from date(data)) as int64)) as mes,
                    safe_cast(data as date) as data,
                    'RJ' as sigla_uf,
                    safe_cast(id_municipio as string) as id_municipio,
                    safe_cast(id_orgao as string) as orgao,
                    safe_cast(unidade_administrativa as string) as id_unidade_gestora,
                    safe_cast(null as string) as id_licitacao_bd,
                    safe_cast(null as string) as id_licitacao,
                    safe_cast(null as string) as modalidade_licitacao,
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
                    safe_cast(numero_empenho as string) as numero,
                    safe_cast(descricao as string) as descricao,
                    safe_cast(modalidade as string) as modalidade,
                    safe_cast(cast(funcao as int64) as string) as funcao,
                    safe_cast(subfuncao as string) as subfuncao,
                    safe_cast(programa as string) as programa,
                    safe_cast(atividade as string) as acao,
                    safe_cast(elemento_despesa as string) as elemento_despesa,
                    round(safe_cast(valor as float64), 2) as valor_inicial,
                from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_rj") }}
                where numero_empenho is not null
            ),
            anulacao_rj as (
                select
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
                    round(safe_cast(valor as float64), 2) as valor_anulacao,
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_anulacao_rj") }}
                where despesa_liquidada = 'NÃO' and numero_empenho is not null
            ),
            empenho_rj as (
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
                    e.id_empenho_bd,
                    e.id_empenho,
                    e.numero,
                    e.descricao,
                    e.modalidade,
                    e.funcao,
                    e.subfuncao,
                    e.programa,
                    e.acao,
                    e.elemento_despesa,
                    e.valor_inicial as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_reforco,
                    safe_cast(ifnull(a.valor_anulacao, 0) as float64) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(
                        safe_cast(
                            (e.valor_inicial - ifnull(a.valor_anulacao, 0)) as float64
                        ),
                        2
                    ) as valor_final
                from empenhado_rj e
                left join anulacao_rj a on e.id_empenho_bd = a.id_empenho_bd
            ),
            empenho_df as (
                select
                    (safe_cast(exercicio as int64)) as ano,
                    (safe_cast(extract(month from date(lancamento)) as int64)) as mes,
                    safe_cast(lancamento as date) as data,
                    'DF' as sigla_uf,
                    '5300108' as id_municipio,
                    safe_cast(codigo_ug as string) as orgao,
                    safe_cast(codigo_gestao as string) as id_unidade_gestora,
                    safe_cast(null as string) as id_licitacao_bd,
                    safe_cast(null as string) as id_licitacao,
                    case
                        when codigo_licitacao = '1'
                        then '11'
                        when codigo_licitacao = '2'
                        then '1'
                        when codigo_licitacao = '3'
                        then '2'
                        when codigo_licitacao = '4'
                        then '3'
                        when codigo_licitacao = '5'
                        then '8'
                        when codigo_licitacao = '6'
                        then '10'
                        when codigo_licitacao = '7'
                        then '99'
                        when codigo_licitacao = '8'
                        then '32'
                        when codigo_licitacao = '9'
                        then '4'
                        when codigo_licitacao = '10'
                        then '32'
                        when codigo_licitacao = '11'
                        then '31'
                        when codigo_licitacao = '12'
                        then ''
                        when codigo_licitacao = '13'
                        then '5'
                        when codigo_licitacao = '14'
                        then '6'
                        when codigo_licitacao = '15'
                        then '5'
                        when codigo_licitacao = '16'
                        then '5'
                        when codigo_licitacao = '17'
                        then '6'
                        when codigo_licitacao = '18'
                        then '3'
                        when codigo_licitacao = '19'
                        then '32'
                        when codigo_licitacao = '20'
                        then '31'
                        when codigo_licitacao = '21'
                        then '31'
                        when codigo_licitacao = '22'
                        then '32'
                        when codigo_licitacao = '23'
                        then '12'
                        when codigo_licitacao = '25'
                        then '98'
                        when codigo_licitacao = 'INEXIGÍVEL'
                        then '10'
                    end as modalidade_licitacao,
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
                    safe_cast(nota_empenho as string) as numero,
                    safe_cast(descricao as string) as descricao,
                    safe_cast(left(modalidade_empenho, 1) as string) as modalidade,
                    safe_cast(cast(codigo_funcao as int64) as string) as funcao,
                    safe_cast(codigo_subfuncao as string) as subfuncao,
                    safe_cast(codigo_programa as string) as programa,
                    safe_cast(codigo_acao as string) as acao,
                    safe_cast(codigo_natureza as string) as elemento_despesa,
                    round(safe_cast(0 as float64), 2) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_reforco,
                    round(safe_cast(0 as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(
                        safe_cast(replace (valor_final, ',', '.') as float64), 2
                    ) as valor_final
                from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_df") }}
            ),
            empenhado_sc as (
                select
                    safe_cast(ano_emp as int64) as ano,
                    safe_cast(substring(trim(data_empenho), -7, 2) as int64) as mes,
                    safe_cast(
                        concat(
                            substring(trim(data_empenho), -4),
                            '-',
                            substring(trim(data_empenho), -7, 2),
                            '-',
                            substring(trim(data_empenho), 1, 2)
                        ) as date
                    ) as data,
                    'SC' as sigla_uf,
                    safe_cast(id_municipio as string) as id_municipio,
                    safe_cast(codigo_orgao as string) as orgao,
                    safe_cast(null as string) as id_unidade_gestora,
                    safe_cast(null as string) as id_licitacao_bd,
                    safe_cast(
                        case
                            when
                                (
                                    split(nr_licitacao_contrato_convenio, ' / ')[
                                        offset(0)
                                    ]
                                )
                                != "Sem Licitação"
                                and (
                                    split(nr_licitacao_contrato_convenio, ' / ')[
                                        offset(0)
                                    ]
                                )
                                != "Sem licitação"
                                and (
                                    split(nr_licitacao_contrato_convenio, ' / ')[
                                        offset(0)
                                    ]
                                )
                                != "Sem Licitacao"
                                and (
                                    split(nr_licitacao_contrato_convenio, ' / ')[
                                        offset(0)
                                    ]
                                )
                                != "SEM LICITACAO"
                            then
                                (
                                    split(nr_licitacao_contrato_convenio, ' / ')[
                                        offset(0)
                                    ]
                                )
                            else ''
                        end as string
                    ) as id_licitacao,
                    safe_cast(null as string) as modalidade_licitacao,
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
                    safe_cast(num_empenho as string) as numero,
                    safe_cast(
                        lower(descricao_historico_empenho) as string
                    ) as descricao,
                    safe_cast(null as string) as modalidade,
                    safe_cast(cast(left(funcao, 2) as int64) as string) as funcao,
                    safe_cast(cast(left(subfuncao, 3) as int64) as string) as subfuncao,
                    safe_cast(null as string) as programa,
                    safe_cast(null as string) as acao,
                    safe_cast(elemento_despesa as string) as elemento_despesa,
                    round(safe_cast(valor_empenho as float64), 2) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_reforco,
                    round(safe_cast(0 as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    round(safe_cast(valor_empenho as float64), 2) as valor_final
                from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_sc") }}
            ),
            frequencia_sc as (
                select id_empenho_bd, count(id_empenho_bd) as frequencia_id
                from empenhado_sc
                group by 1
                order by 2 desc
            ),
            empenho_sc as (
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
                    e.elemento_despesa,
                    round(safe_cast(0 as float64), 2) as valor_inicial,
                    round(safe_cast(0 as float64), 2) as valor_reforco,
                    round(safe_cast(0 as float64), 2) as valor_anulacao,
                    round(safe_cast(0 as float64), 2) as valor_ajuste,
                    e.valor_final as valor_final
                from empenhado_sc e
                left join frequencia_sc f on e.id_empenho_bd = f.id_empenho_bd
            )

        select *
        from empenho_mg
        union all
        (select * from empenho_sp)
        union all
        (select * from empenho_municipio_sp)
        union all
        (select * from empenho_pe)
        union all
        (select * from empenho_pr)
        union all
        (select * from empenho_rs)
        union all
        (select * from empenho_pb)
        union all
        (select * from empenho_ce)
        union all
        (select * from empenho_rj)
        union all
        (select * from empenho_municipio_rj_v1)
        union all
        (select * from empenho_municipio_rj_v2)
        union all
        (select * from empenho_df)
        union all
        (select * from empenho_sc)
    )
