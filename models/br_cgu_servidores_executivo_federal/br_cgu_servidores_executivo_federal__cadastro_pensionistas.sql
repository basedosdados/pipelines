{{
    config(
        schema="br_cgu_servidores_executivo_federal",
        alias="cadastro_pensionistas",
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
    safe_cast(cpf_representante_legal as string) cpf_representante_legal,
    safe_cast(nome_representante_legal as string) nome_representante_legal,
    safe_cast(cpf_instituidor_pensao as string) cpf_instituidor_pensao,
    safe_cast(nome_instituidor_pensao as string) nome_instituidor_pensao,
    safe_cast(id_tipo_pensao as string) id_tipo_pensao,
    safe_cast(tipo_pensao as string) tipo_pensao,
    safe_cast(data_inicio_pensao as date) data_inicio_pensao,
    safe_cast(
        descricao_cargo_instituidor_pensao as string
    ) descricao_cargo_instituidor_pensao,
    safe_cast(
        id_uorg_lotacao_instituidor_pensao as string
    ) id_uorg_lotacao_instituidor_pensao,
    safe_cast(
        uorg_lotacao_instituidor_pensao as string
    ) uorg_lotacao_instituidor_pensao,
    safe_cast(
        id_org_lotacao_instituidor_pensao as string
    ) id_org_lotacao_instituidor_pensao,
    safe_cast(org_lotacao_instituidor_pensao as string) org_lotacao_instituidor_pensao,
    safe_cast(
        id_orgsup_lotacao_instituidor_pensao as string
    ) id_orgsup_lotacao_instituidor_pensao,
    safe_cast(
        orgsup_lotacao_instituidor_pensao as string
    ) orgsup_lotacao_instituidor_pensao,
    safe_cast(id_tipo_vinculo as string) id_tipo_vinculo,
    safe_cast(tipo_vinculo as string) tipo_vinculo,
    safe_cast(situacao_vinculo as string) situacao_vinculo,
    safe_cast(
        regime_juridico_instituidor_pensao as string
    ) regime_juridico_instituidor_pensao,
    safe_cast(
        jornada_trabalho_instituidor_pensao as string
    ) jornada_trabalho_instituidor_pensao,
    (
        case
            when data_ingresso_cargo_funcao_instituidor_pensao = "Não informada"
            then null
            else parse_date('%d/%m/%Y', data_ingresso_cargo_funcao_instituidor_pensao)
        end
    ) as data_ingresso_cargo_funcao_instituidor_pensao,
    (
        case
            when data_nomeacao_cargo_funcao_instituidor_pensao = "Não informada"
            then null
            else parse_date('%d/%m/%Y', data_nomeacao_cargo_funcao_instituidor_pensao)
        end
    ) as data_nomeacao_cargo_funcao_instituidor_pensao,
    (
        case
            when data_ingresso_orgao_instituidor_pensao = "Não informada"
            then null
            else parse_date('%d/%m/%Y', data_ingresso_orgao_instituidor_pensao)
        end
    ) as data_ingresso_orgao_instituidor_pensao,
    (
        case
            when
                data_diploma_ingresso_servico_publico_instituidor_pensao
                = "Não informada"
            then null
            else
                parse_date(
                    '%d/%m/%Y', data_diploma_ingresso_servico_publico_instituidor_pensao
                )
        end
    ) as data_diploma_ingresso_servico_publico_instituidor_pensao,
    safe_cast(
        documento_ingresso_servico_publico_instituidor_pensao as string
    ) documento_ingresso_servico_publico_instituidor_pensao,
    safe_cast(
        diploma_ingresso_cargo_funcao_instituidor_pensao as string
    ) diploma_ingresso_cargo_funcao_instituidor_pensao,
    safe_cast(
        diploma_ingresso_orgao_instituidor_pensao as string
    ) diploma_ingresso_orgao_instituidor_pensao,
    safe_cast(
        diploma_ingresso_servicopublico_instituidor_pensao as string
    ) diploma_ingresso_servicopublico_instituidor_pensao,
    safe_cast(origem as string) origem,
from
    {{
        set_datalake_project(
            "br_cgu_servidores_executivo_federal_staging.cadastro_pensionistas"
        )
    }} as t
