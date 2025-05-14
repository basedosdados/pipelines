{{
    config(
        alias="microdados",
        schema="br_ms_sinasc",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1994, "end": 2023, "interval": 1},
        },
        cluster_by="sigla_uf",
    )
}}
with
    municipio_mae_6 as (
        select distinct id_municipio, id_municipio_6
        from {{ set_datalake_project("br_ms_sinasc_staging.microdados") }} mm6
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` m
            on m.id_municipio_6 = mm6.id_municipio_mae
    )
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(sequencial_nascimento as string) sequencial_nascimento,
    safe_cast(id_municipio_nascimento as string) id_municipio_nascimento,
    safe_cast(local_nascimento as string) local_nascimento,
    safe_cast(codigo_estabelecimento as string) codigo_estabelecimento,
    safe_cast(data_nascimento as date) data_nascimento,
    safe_cast(hora_nascimento as time) hora_nascimento,
    safe_cast(sexo as string) sexo,
    safe_cast(peso as int64) peso,
    safe_cast(raca_cor as string) raca_cor,
    safe_cast(apgar1 as int64) apgar1,
    safe_cast(apgar5 as int64) apgar5,
    safe_cast(id_anomalia as string) id_anomalia,
    safe_cast(codigo_anomalia as string) codigo_anomalia,
    safe_cast(semana_gestacao as int64) semana_gestacao,
    safe_cast(semana_gestacao_estimada as string) semana_gestacao_estimada,
    safe_cast(gestacao_agr as string) gestacao_agr,
    safe_cast(tipo_gravidez as string) tipo_gravidez,
    safe_cast(tipo_parto as string) tipo_parto,
    safe_cast(inicio_pre_natal as string) inicio_pre_natal,
    safe_cast(pre_natal as int64) pre_natal,
    safe_cast(pre_natal_agr as string) pre_natal_agr,
    safe_cast(classificacao_pre_natal as string) classificacao_pre_natal,
    safe_cast(quantidade_filhos_vivos as int64) quantidade_filhos_vivos,
    safe_cast(quantidade_filhos_mortos as int64) quantidade_filhos_mortos,
    safe_cast(id_pais_mae as string) id_pais_mae,
    safe_cast(id_uf_mae as string) id_uf_mae,
    safe_cast(
        case
            when length(id_municipio_mae) = 6
            then
                (
                    select id_municipio
                    from municipio_mae_6 m1
                    where m1.id_municipio_6 = t.id_municipio_mae
                )
            when length(id_municipio_mae) = 7
            then id_municipio_mae
            else null
        end as string
    ) id_municipio_mae,
    safe_cast(id_pais_residencia as string) id_pais_residencia,
    safe_cast(id_municipio_residencia as string) id_municipio_residencia,
    safe_cast(data_nascimento_mae as date) data_nascimento_mae,
    safe_cast(idade_mae as int64) idade_mae,
    safe_cast(escolaridade_mae as string) escolaridade_mae,
    safe_cast(serie_escolar_mae as string) serie_escolar_mae,
    safe_cast(escolaridade_2010_mae as string) escolaridade_2010_mae,
    safe_cast(escolaridade_2010_agr_mae as string) escolaridade_2010_agr_mae,
    safe_cast(estado_civil_mae as string) estado_civil_mae,
    safe_cast(ocupacao_mae as string) ocupacao_mae,
    safe_cast(raca_cor_mae as string) raca_cor_mae,
    safe_cast(gestacoes_ant as int64) gestacoes_ant,
    safe_cast(quantidade_parto_normal as int64) quantidade_parto_normal,
    safe_cast(quantidade_parto_cesareo as int64) quantidade_parto_cesareo,
    safe_cast(data_ultima_menstruacao as date) data_ultima_menstruacao,
    safe_cast(tipo_apresentacao as string) tipo_apresentacao,
    safe_cast(inducao_parto as string) inducao_parto,
    safe_cast(cesarea_antes_parto as string) cesarea_antes_parto,
    safe_cast(tipo_robson as string) tipo_robson,
    safe_cast(idade_pai as int64) idade_pai,
    safe_cast(cartorio as string) cartorio,
    safe_cast(registro_cartorio as string) registro_cartorio,
    safe_cast(data_registro_cartorio as date) data_registro_cartorio,
    safe_cast(origem as string) origem,
    safe_cast(numero_lote as int64) numero_lote,
    safe_cast(versao_sistema as string) versao_sistema,
    safe_cast(data_cadastro as date) data_cadastro,
    safe_cast(data_recebimento as date) data_recebimento,
    safe_cast(data_recebimento_original as date) data_recebimento_original,
    safe_cast(diferenca_data as int64) diferenca_data,
    safe_cast(data_declaracao as date) data_declaracao,
    safe_cast(funcao_responsavel as string) funcao_responsavel,
    safe_cast(documento_responsavel as string) documento_responsavel,
    safe_cast(
        formacao_profissional_responsavel as string
    ) formacao_profissional_responsavel,
    safe_cast(status_dn as string) status_dn,
    safe_cast(status_dn_nova as string) status_dn_nova,
    safe_cast(paridade as string) paridade
from {{ set_datalake_project("br_ms_sinasc_staging.microdados") }} as t
