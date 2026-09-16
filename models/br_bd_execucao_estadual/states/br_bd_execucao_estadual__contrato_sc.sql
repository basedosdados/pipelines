{{ config(materialized="ephemeral") }}

-- Santa Catarina contracts. Source: sc_contrato staging, the dados.sc.gov.br "Contratos"
-- package read from its XLSX (the CKAN CSV carries the same unquoted free-text defect
-- that made SC's empenho CSV unparseable, and there is no portal contrato view). One row
-- per contract; (unidade gestora, nucontrato) is unique. Dates are 'YYYY-MM-DD HH:MM:SS.s'
-- and values dot-decimal; some DTFIMATUAL run to the year 3031 and are read as no date.
with
    fonte as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.sc_contrato") }}
    ),
    base as (
        select
            safe.parse_date('%Y-%m-%d', substr(trim(DTASSINATURA), 1, 10)) as dt_assin,
            safe.parse_date('%Y-%m-%d', substr(trim(DTINICIO), 1, 10)) as dt_ini,
            safe.parse_date('%Y-%m-%d', substr(trim(DTFIMATUAL), 1, 10)) as dt_fim,
            nullif(trim(CDUNIDADEGESTORA), '') as id_ug,
            nullif(trim(NMUNIDADEGESTORA), '') as nome_ug,
            nullif(trim(NUCONTRATO), '') as numero_contrato,
            nullif(trim(NUPROCESSO), '') as numero_processo,
            nullif(trim(OBJETO), '') as objeto,
            nullif(trim(NMMODALIDADE), '') as modalidade,
            nullif(trim(DETIPOCONTRATO), '') as tipo_contrato,
            nullif(trim(IDCONTRATADO), '') as documento_contratado,
            nullif(trim(CONTRATADO), '') as nome_contratado,
            nullif(trim(SITUACAO), '') as situacao,
            safe_cast(replace(VLORIGINAL, ',', '.') as float64) as valor_inicial,
            safe_cast(replace(VLATUAL, ',', '.') as float64) as valor_atual
        from fonte
    )
select
    case
        when extract(year from coalesce(dt_assin, dt_ini)) between 1990 and 2030
        then extract(year from coalesce(dt_assin, dt_ini))
    end as ano,
    'SC' as sigla_uf,
    concat(
        'SC-', coalesce(id_ug, 'SEMUG'), '-', numero_contrato, '-',
        row_number() over (
            partition by id_ug, numero_contrato
            order by dt_assin, valor_inicial, nome_contratado
        )
    ) as id_contrato_bd,
    numero_contrato as numero_contrato,
    numero_processo as numero_processo,
    id_ug as id_unidade_gestora,
    nome_ug as nome_unidade_gestora,
    objeto as objeto,
    modalidade as modalidade,
    tipo_contrato as tipo_contrato,
    documento_contratado as documento_contratado,
    nome_contratado as nome_contratado,
    situacao as situacao,
    dt_assin as data_assinatura,
    dt_ini as data_inicio_vigencia,
    dt_fim as data_fim_vigencia,
    valor_inicial as valor_inicial,
    valor_atual as valor_atual
from base
where numero_contrato is not null
