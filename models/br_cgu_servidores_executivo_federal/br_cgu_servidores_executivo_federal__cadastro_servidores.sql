{{
    config(
        schema="br_cgu_servidores_executivo_federal",
        alias="cadastro_servidores",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2024, "interval": 1},
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
    safe_cast(descricao_cargo as string) descricao_cargo,
    safe_cast(classe_cargo as string) classe_cargo,
    safe_cast(
        (
            case
                when referencia_cargo = "-1.0"
                then "-1"
                when referencia_cargo = "0.0"
                then "0"
                else referencia_cargo
            end
        ) as string
    ) referencia_cargo,
    safe_cast(padrao_cargo as string) padrao_cargo,
    safe_cast(safe_cast(nivel_cargo as float64) as string) nivel_cargo,
    safe_cast(sigla_funcao as string) sigla_funcao,
    safe_cast(
        (
            case
                when safe_cast(safe_cast(nivel_funcao as float64) as string) is null
                then nivel_funcao
                else cast(cast(nivel_funcao as float64) as string)
            end
        ) as string
    ) nivel_funcao,
    safe_cast(funcao as string) funcao,
    safe_cast(id_atividade as string) id_atividade,
    safe_cast(atividade as string) atividade,
    safe_cast(opcao_parcial as string) opcao_parcial,
    safe_cast(id_uorg_lotacao as string) id_uorg_lotacao,
    safe_cast(uorg_lotacao as string) uorg_lotacao,
    safe_cast(id_org_lotacao as string) id_org_lotacao,
    safe_cast(org_lotacao as string) org_lotacao,
    safe_cast(id_orgsup_lotacao as string) id_orgsup_lotacao,
    safe_cast(orgsup_lotacao as string) orgsup_lotacao,
    safe_cast(id_uorg_exercicio as string) id_uorg_exercicio,
    safe_cast(uorg_exercicio as string) uorg_exercicio,
    safe_cast(id_org_exercicio as string) id_org_exercicio,
    safe_cast(org_exercicio as string) org_exercicio,
    safe_cast(id_orgsup_exercicio as string) id_orgsup_exercicio,
    safe_cast(orgsup_exercicio as string) orgsup_exercicio,
    safe_cast(id_tipo_vinculo as string) id_tipo_vinculo,
    safe_cast(tipo_vinculo as string) tipo_vinculo,
    safe_cast(situacao_vinculo as string) situacao_vinculo,
    (
        case
            when data_inicio_afastamento = "Não informada"
            then null
            else parse_date('%d/%m/%Y', data_inicio_afastamento)
        end
    ) as data_inicio_afastamento,
    (
        case
            when data_termino_afastamento = "Não informada"
            then null
            else parse_date('%d/%m/%Y', data_termino_afastamento)
        end
    ) as data_termino_afastamento,
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
    safe_cast(
        documento_ingresso_servico_publico as string
    ) documento_ingresso_servico_publico,
    (case when sigla_uf in ("-1", "-3") then null else sigla_uf end) as sigla_uf,
    safe_cast(origem as string) origem,
from
    {{
        set_datalake_project(
            "br_cgu_servidores_executivo_federal_staging.cadastro_servidores"
        )
    }} as t
