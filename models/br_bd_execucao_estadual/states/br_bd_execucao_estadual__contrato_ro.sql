{{ config(materialized="ephemeral") }}

-- Rondônia contracts. Source: ro_contrato staging, the CGE-RO public API
-- (`transparencia.api.ro.gov.br/api/v1/contratos`), one row per contract document.
-- Dates arrive as ISO 'YYYY-MM-DDT00:00:00'; `valorInicial` is a "R$ 1.234,56" string
-- (with a non-breaking space), stripped to a number here. RO publishes only a single
-- value (no aditivo-adjusted amount) and no vigência start, contract status or
-- contractor/UG split beyond name+code, so valor_atual repeats valor_inicial (the RS
-- convention) and data_inicio_vigencia/situacao are null. `origem` is the originating
-- procedure (Dispensa / Inexigibilidade / "Pregão Eletrônico/<n>/<ano>" / …) ->
-- modalidade.
with
    fonte as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.ro_contrato") }}
    ),
    base as (
        select
            safe.parse_date(
                '%Y-%m-%d', substr(trim(dataassinatura), 1, 10)
            ) as dt_assin,
            safe.parse_date('%Y-%m-%d', substr(trim(dataelaboracao), 1, 10)) as dt_elab,
            safe.parse_date('%Y-%m-%d', substr(trim(datavigencia), 1, 10)) as dt_fim,
            nullif(trim(codigoug), '') as id_ug,
            nullif(trim(nomeug), '') as nome_ug,
            nullif(trim(numerodocumento), '') as numero_contrato,
            nullif(trim(numeroprocesso), '') as numero_processo,
            nullif(trim(objeto), '') as objeto,
            nullif(trim(origem), '') as modalidade,
            nullif(trim(cnpj_cpf), '') as documento_contratado,
            nullif(trim(empresa), '') as nome_contratado,
            -- strip "R$", the non-breaking space, spaces and thousands dots, then the
            -- comma decimal -> a number. Everything but digits, comma and minus goes.
            safe_cast(
                replace(
                    regexp_replace(valorinicial, r'[^0-9,-]', ''), ',', '.'
                ) as float64
            ) as valor
        from fonte
    )
select
    case
        when extract(year from coalesce(dt_assin, dt_elab)) between 1990 and 2030
        then extract(year from coalesce(dt_assin, dt_elab))
    end as ano,
    'RO' as sigla_uf,
    concat(
        'RO-',
        coalesce(id_ug, 'SEMUG'),
        '-',
        numero_contrato,
        '-',
        row_number() over (
            partition by id_ug, numero_contrato
            order by dt_assin, valor, nome_contratado
        )
    ) as id_contrato_bd,
    numero_contrato as numero_contrato,
    numero_processo as numero_processo,
    id_ug as id_unidade_gestora,
    nome_ug as nome_unidade_gestora,
    objeto as objeto,
    modalidade as modalidade,
    -- RO publishes no contract type and no status.
    safe_cast(null as string) as tipo_contrato,
    documento_contratado as documento_contratado,
    nome_contratado as nome_contratado,
    safe_cast(null as string) as situacao,
    dt_assin as data_assinatura,
    -- RO publishes no vigência start; dataVigencia is the end.
    safe_cast(null as date) as data_inicio_vigencia,
    dt_fim as data_fim_vigencia,
    valor as valor_inicial,
    valor as valor_atual
from base
where numero_contrato is not null
