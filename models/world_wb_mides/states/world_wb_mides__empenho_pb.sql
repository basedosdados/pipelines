-- Paraíba (PB) contribution to world_wb_mides.empenho.
-- Split out of the monolithic empenho model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
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
                        de_modalidade = 'Aplicação Direta §§ 1º e 2º do Art. 24 LC 1412'
                    then '35'
                    when de_modalidade = 'Aplicação Direta Art. 25 LC 141'
                    then '36'
                    when de_modalidade = 'Transferências a Municípios'
                    then '40'
                    when de_modalidade = 'Transferências a Municípios – Fundo a Fundo'
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
                        de_modalidade = 'Transf. a Consórc Púb. C.Rateio Art. 25 LC 141'
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
        from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_pb") }} e
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_municipio_pb") }} m
            on e.cd_ugestora = safe_cast(m.id_unidade_gestora as string)
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_funcao") }} f
            on e.de_funcao = f.nome_funcao
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_subfuncao") }} sf
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
        from {{ set_datalake_project("world_wb_mides_staging.raw_estorno_pb") }} a
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_municipio_pb") }} m
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
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21

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

    )
select *
from empenho_pb
