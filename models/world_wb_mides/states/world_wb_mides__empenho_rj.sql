-- Rio de Janeiro (RJ) contribution to world_wb_mides.empenho.
-- Split out of the monolithic empenho model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    empenhado_municipio_rj_v1 as (
        select
            (safe_cast(exercicio_empenho as int64)) as ano,
            (safe_cast(extract(month from date(data_empenho)) as int64)) as mes,
            safe_cast(data_empenho as date) as data,
            'RJ' as sigla_uf,
            '3304557' as id_municipio,
            safe_cast(orgao_programa_trabalho as string) as orgao,
            safe_cast(unidade_programa_trabalho as string) as id_unidade_gestora,
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
            safe_cast(substring(programa_trabalho, 14, 4) as string) as programa,
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
                safe_cast((e.valor_inicial - ifnull(a.valor_anulacao, 0)) as float64), 2
            ) as valor_final
        from empenhado_municipio_rj_v2 e
        left join anulacao_municipio_rj_v2 a on e.id_empenho_bd = a.id_empenho_bd

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
        from {{ set_datalake_project("world_wb_mides_staging.raw_anulacao_rj") }}
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
                safe_cast((e.valor_inicial - ifnull(a.valor_anulacao, 0)) as float64), 2
            ) as valor_final
        from empenhado_rj e
        left join anulacao_rj a on e.id_empenho_bd = a.id_empenho_bd

    )
select *
from empenho_rj
union all
(select * from empenho_municipio_rj_v1)
union all
(select * from empenho_municipio_rj_v2)
