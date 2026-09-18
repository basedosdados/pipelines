{{ config(materialized="ephemeral") }}

-- Rio Grande do Sul contracts. Source: rs_contrato staging, the dados.rs.gov.br
-- "Contratos do Estado" package -- four typed CSV/zip files (fornecimento de bens,
-- locação, obras e serviços de engenharia, serviços de terceiros) unioned to the
-- superset of their columns, with the file's type carried in `tipo_contrato`.
--
-- The four files disagree on which dates and value column they publish: fornecimento
-- and
-- obras give a publication date (`datapublicacaodoe`) and a free-text `vigencia`,
-- locação
-- gives `datainiciovigencia`/`datafimvigencia`, serviços gives
-- `datainiciovigenciacontrato`/`datafimvigenciacontrato` and `valorcontrato` instead of
-- `valor`. RS publishes the contractor's NAME but no CNPJ, and no contract status, so
-- documento_contratado and situacao are null. Dates are %d/%m/%Y and values
-- comma-decimal.
with
    fonte as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.rs_contrato") }}
    ),
    base as (
        select
            safe.parse_date(
                '%d/%m/%Y', substr(trim(datainiciovigencia), 1, 10)
            ) as dt_ini_loc,
            safe.parse_date(
                '%d/%m/%Y', substr(trim(datainiciovigenciacontrato), 1, 10)
            ) as dt_ini_serv,
            safe.parse_date(
                '%d/%m/%Y', substr(trim(datafimvigencia), 1, 10)
            ) as dt_fim_loc,
            safe.parse_date(
                '%d/%m/%Y', substr(trim(datafimvigenciacontrato), 1, 10)
            ) as dt_fim_serv,
            safe.parse_date(
                '%d/%m/%Y', substr(trim(datapublicacaodoe), 1, 10)
            ) as dt_doe,
            nullif(trim(cod_orgao), '') as id_ug,
            nullif(trim(orgao), '') as nome_ug,
            nullif(trim(numerocontrato), '') as numero_contrato,
            nullif(trim(numeroprocesso), '') as numero_processo,
            nullif(trim(objeto), '') as objeto,
            coalesce(
                nullif(trim(tipoprocedimento), ''),
                nullif(trim(procedimentolicitatorio), '')
            ) as modalidade,
            nullif(trim(tipo_contrato), '') as tipo_contrato,
            nullif(trim(contratada), '') as nome_contratado,
            coalesce(
                safe_cast(replace(valor, ',', '.') as float64),
                safe_cast(replace(valorcontrato, ',', '.') as float64)
            ) as valor
        from fonte
    )
select
    case
        when
            extract(year from coalesce(dt_ini_loc, dt_ini_serv, dt_doe))
            between 1990 and 2030
        then extract(year from coalesce(dt_ini_loc, dt_ini_serv, dt_doe))
    end as ano,
    'RS' as sigla_uf,
    concat(
        'RS-',
        coalesce(id_ug, 'SEMUG'),
        '-',
        numero_contrato,
        '-',
        row_number() over (
            partition by id_ug, numero_contrato
            order by tipo_contrato, valor, nome_contratado
        )
    ) as id_contrato_bd,
    numero_contrato as numero_contrato,
    numero_processo as numero_processo,
    id_ug as id_unidade_gestora,
    nome_ug as nome_unidade_gestora,
    objeto as objeto,
    modalidade as modalidade,
    tipo_contrato as tipo_contrato,
    -- RS publishes the contractor's name but no document.
    safe_cast(null as string) as documento_contratado,
    nome_contratado as nome_contratado,
    -- RS publishes no contract status.
    safe_cast(null as string) as situacao,
    -- The publication in the DOE is the closest thing RS gives to a signing date.
    dt_doe as data_assinatura,
    coalesce(dt_ini_loc, dt_ini_serv) as data_inicio_vigencia,
    coalesce(dt_fim_loc, dt_fim_serv) as data_fim_vigencia,
    valor as valor_inicial,
    valor as valor_atual
from base
where numero_contrato is not null
