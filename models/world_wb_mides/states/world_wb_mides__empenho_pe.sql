-- Pernambuco (PE) contribution to world_wb_mides.empenho.
-- Split out of the monolithic empenho model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    empenho_pe as (
        select
            safe_cast(e.anoreferencia as int64) as ano,
            (safe_cast(extract(month from date(dataempenho)) as int64)) as mes,
            safe_cast(extract(date from timestamp(dataempenho)) as date) as data,
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
                    when modalidade = 'Transferências a Municípios – Fundo a Fundo'
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
                        modalidade = 'Transferências a Instituições Multigovernamentais'
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
                    when elementodespesa = 'Outros Benefícios Previdenciários do RPPS'
                    then '05'
                    when
                        elementodespesa
                        = 'Outros Benefícios Previdenciários do servidor ou do militar'
                    then '05'
                    when elementodespesa = 'Beneficio Mensal ao Deficiente e ao Idoso'
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
                    when elementodespesa = 'Outras Despesas Variáveis - Pessoal Civil'
                    then '16'
                    when elementodespesa = 'Auxílio Financeiro a Estudantes'
                    then '18'
                    when elementodespesa = 'Auxílio Fardamento'
                    then '19'
                    when elementodespesa = 'Auxílio Financeiro a Pesquisadores'
                    then '20'
                    when elementodespesa = 'Outros Encargos sobre a Dívida por Contrato'
                    then '22'
                    when
                        elementodespesa
                        = 'Juros, Deságios e Descontos da Dívida Mobiliária'
                    then '23'
                    when elementodespesa = 'Outros Encargos sobre a Dívida Mobiliária'
                    then '24'
                    when
                        elementodespesa
                        = 'Encargos sobre Operações de Crédito por Antecipação da Receita'
                    then '25'
                    when
                        elementodespesa
                        = 'Encargos pela Honra de Avais, Garantias, Seguros e Similares'
                    then '27'
                    when elementodespesa = 'Remuneração de Cotas de Fundos Autárquicos'
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
                    when elementodespesa = 'Obrigações Tributárias e Contributivas'
                    then '47'
                    when elementodespesa = 'Auxílio-Transporte'
                    then '49'
                    when elementodespesa = 'Obras e Instalações'
                    then '51'
                    when elementodespesa = 'Equipamentos e Material Permanente'
                    then '52'
                    when elementodespesa = 'Aposentadorias do RGPS ? Área Urbana'
                    then '54'
                    when elementodespesa = 'Pensões, exclusiva do RGPS'
                    then '56'
                    when elementodespesa = 'Outros Benefícios do RGPS ? Área Urbana'
                    then '58'
                    when elementodespesa = 'Pensões Especiais'
                    then '59'
                    when elementodespesa = 'Aquisição de Imóveis'
                    then '61'
                    when
                        elementodespesa
                        = 'Constituição ou Aumento de Capital de Empresas'
                    then '65'
                    when elementodespesa = 'Concessão de Empréstimos e Financiamentos'
                    then '66'
                    when elementodespesa = 'Depósitos Compulsórios'
                    then '67'
                    when
                        elementodespesa
                        = 'Rateio pela Participação em Consórcio Público'
                    then '70'
                    when elementodespesa = 'Principal da Dívida Contratual Resgatado'
                    then '71'
                    when elementodespesa = 'Principal da Dívida Mobiliária Resgatado'
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
        from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_pe") }} e
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_municipio_pe") }} m
            on e.nomeunidadegestora = m.nomeunidadegestora
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_funcao") }} fun
            on upper(
                trim(
                    replace(
                        replace(e.funcao, 'Encargos Especias', 'Encargos Especiais'),
                        'Assistêncial Social',
                        'Assistência Social'
                    )
                )
            )
            = upper(nome_funcao)
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_subfuncao") }} sub
            on upper(trim(e.subfuncao)) = upper(nome_subfuncao)

    )
select *
from empenho_pe
