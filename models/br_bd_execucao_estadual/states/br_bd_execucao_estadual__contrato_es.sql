{{ config(materialized="ephemeral") }}

-- Espírito Santo contracts. Source: es_contrato staging (dados.es.gov.br SIGA), the
-- rows whose TipoDocumento is a contract. The es_contrato dataset also carries purchase
-- authorizations, supply orders and empenho notes -- ~60k of its 78.6k rows -- which
-- are
-- excluded here so the table holds contracts only (SC and RS publish contracts only).
-- Dates are %d/%m/%Y and values comma-decimal; the SQL-Server floor 1753 and the stray
-- 5024 in the file-name year are treated as no year.
with
    fonte as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.es_contrato") }}
        where upper(trim(tipodocumento)) = 'CONTRATO'
    ),
    base as (
        select
            safe.parse_date(
                '%d/%m/%Y', substr(trim(datacelebracao), 1, 10)
            ) as dt_assin,
            safe.parse_date(
                '%d/%m/%Y', substr(trim(datainiciovigencia), 1, 10)
            ) as dt_ini,
            safe.parse_date('%d/%m/%Y', substr(trim(datafimvigencia), 1, 10)) as dt_fim,
            nullif(trim(idorgao), '') as id_ug,
            nullif(trim(nomeorgao), '') as nome_ug,
            nullif(trim(numerodocumento), '') as numero_contrato,
            nullif(trim(numeroprocesso), '') as numero_processo,
            nullif(trim(objeto), '') as objeto,
            nullif(trim(modalidadeprocesso), '') as modalidade,
            nullif(trim(cnpjfornecedor), '') as documento_contratado,
            nullif(trim(fornecedor), '') as nome_contratado,
            nullif(trim(situacao), '') as situacao,
            safe_cast(replace(valorinicial, ',', '.') as float64) as valor_inicial,
            safe_cast(replace(valorfinal, ',', '.') as float64) as valor_atual
        from fonte
    )
select
    case
        when extract(year from coalesce(dt_assin, dt_ini)) between 1990 and 2030
        then extract(year from coalesce(dt_assin, dt_ini))
    end as ano,
    'ES' as sigla_uf,
    concat(
        'ES-',
        coalesce(id_ug, 'SEMUG'),
        '-',
        numero_contrato,
        '-',
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
    'Contrato' as tipo_contrato,
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
