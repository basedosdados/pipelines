{{
    config(
        schema="br_cgu_servidores_executivo_federal",
        alias="cadastro_aposentados",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2020, "end": 2024, "interval": 1},
        },
        cluster_by=["ano", "mes"],
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 7)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) <= 7)',
        ],
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_servidor as string) id_servidor,
    safe_cast(nome as string) nome,
    safe_cast(cpf as string) cpf,
    safe_cast(matricula as string) matricula,
    safe_cast(id_tipo_aposentadoria as string) id_tipo_aposentadoria,
    safe_cast(tipo_aposentadoria as string) tipo_aposentadoria,
    (
        case
            when data_aposentadoria = "Não informada"
            then null
            else parse_date('%d/%m/%Y', data_aposentadoria)
        end
    ) as data_aposentadoria,
    safe_cast(descricao_cargo as string) descricao_cargo,
    safe_cast(id_uorg_lotacao as string) id_uorg_lotacao,
    safe_cast(uorg_lotacao as string) uorg_lotacao,
    safe_cast(id_org_lotacao as string) id_org_lotacao,
    safe_cast(org_lotacao as string) org_lotacao,
    safe_cast(id_orgsup_lotacao as string) id_orgsup_lotacao,
    safe_cast(orgsup_lotacao as string) orgsup_lotacao,
    safe_cast(id_tipo_vinculo as string) id_tipo_vinculo,
    safe_cast(tipo_vinculo as string) tipo_vinculo,
    safe_cast(situacao_vinculo as string) situacao_vinculo,
    safe_cast(regime_juridico as string) regime_juridico,
    safe_cast(jornada_trabalho as string) jornada_trabalho,
    (
        case
            when data_ingresso_cargo_funcao = "Não informada"
            then null
            else parse_date('%d/%m/%Y', data_ingresso_cargo_funcao)
        end
    ) as data_ingresso_cargo_funcao,
    (
        case
            when data_nomeacao_cargo_funcao = "Não informada"
            then null
            else parse_date('%d/%m/%Y', data_nomeacao_cargo_funcao)
        end
    ) as data_nomeacao_cargo_funcao,
    (
        case
            when data_ingresso_orgao = "Não informada"
            then null
            else parse_date('%d/%m/%Y', data_ingresso_orgao)
        end
    ) as data_ingresso_orgao,
    safe_cast(
        documento_ingresso_servico_publico as string
    ) documento_ingresso_servico_publico,
    (
        case
            when data_diploma_ingresso_servico_publico = "Não informada"
            then null
            else parse_date('%d/%m/%Y', data_diploma_ingresso_servico_publico)
        end
    ) as data_diploma_ingresso_servico_publico,

    safe_cast(diploma_ingresso_cargo_funcao as string) diploma_ingresso_cargo_funcao,
    safe_cast(diploma_ingresso_orgao as string) diploma_ingresso_orgao,
    safe_cast(
        diploma_ingresso_servico_publico as string
    ) diploma_ingresso_servico_publico,
    safe_cast(origem as string) origem,
from
    {{
        set_datalake_project(
            "br_cgu_servidores_executivo_federal_staging.cadastro_aposentados"
        )
    }} as t
