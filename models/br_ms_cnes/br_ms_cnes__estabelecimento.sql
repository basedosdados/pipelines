{{
    config(
        schema="br_ms_cnes",
        alias="estabelecimento",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2005, "end": 2024, "interval": 1},
        },
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) <= 6)',
        ],
    )
}}
with
    raw_cnes_estabelecimento as (
        -- 1. Retirar linhas com id_estabelecimento_cnes nulo
        select *
        from {{ set_datalake_project("br_ms_cnes_staging.estabelecimento") }}
        where cnes is not null
    ),
    raw_cnes_estabelecimento_without_duplicates as (
        -- 2. Distinct nas linhas
        select distinct * from raw_cnes_estabelecimento
    ),
    cnes_add_muni as (
        -- 3. Adicionar id_municipio
        select *
        from raw_cnes_estabelecimento_without_duplicates
        left join
            (
                select id_municipio, id_municipio_6,
                from `basedosdados.br_bd_diretorios_brasil.municipio`
            ) as mun
            on raw_cnes_estabelecimento_without_duplicates.codufmun = mun.id_municipio_6
    )
-- 4. padronização, ordenação de colunas e conversão de tipos
-- 5. Aplica macro clean_cols em certas colunas
select
    safe_cast(ano as int64) as ano,
    safe_cast(mes as int64) as mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(substr(cast(dt_atual as string), 1, 4) as int64) as ano_atualizacao,
    safe_cast(substr(cast(dt_atual as string), 5, 2) as int64) as mes_atualizacao,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(codufmun as string) id_municipio_6,
    safe_cast({{ clean_cols("REGSAUDE") }} as string) id_regiao_saude,
    safe_cast({{ clean_cols("MICR_REG") }} as string) id_microrregiao_saude,
    safe_cast({{ clean_cols("DISTRSAN") }} as string) id_distrito_sanitario,
    safe_cast({{ clean_cols("DISTRADM") }} as string) id_distrito_administrativo,
    safe_cast(cod_cep as string) cep,
    safe_cast(cnes as string) id_estabelecimento_cnes,
    safe_cast({{ clean_cols("PF_PJ") }} as string) tipo_pessoa,
    -- fazer replace em valores de linha com 14 zeros para null. 14 zeros é o tamanho
    -- de um valor nulo na variável cpf/cnpj
    safe_cast(regexp_replace(cpf_cnpj, '0{14}', '') as string) cpf_cnpj,
    safe_cast({{ clean_cols("NIV_DEP") }} as string) tipo_grau_dependencia,
    -- fazer replace em valores de linha com 14 zeros para null. 14 zeros é o tamanho
    -- de um cpf/cnpj nulo
    safe_cast(regexp_replace(cnpj_man, '0{14}', '') as string) cnpj_mantenedora,
    case
        when safe_cast({{ clean_cols("COD_IR") }} as string) = 'nan'
        then null
        else safe_cast({{ clean_cols("COD_IR") }} as string)
    end as tipo_retencao_tributos_mantenedora,
    safe_cast(vinc_sus as int64) indicador_vinculo_sus,
    safe_cast(tpgestao as string) tipo_gestao,
    case
        when safe_cast({{ clean_cols("ESFERA_A") }} as string) = 'nan'
        then null
        else safe_cast({{ clean_cols("ESFERA_A") }} as string)
    end as tipo_esfera_administrativa,
    case
        when regexp_replace(safe_cast(retencao as string), '^0+', '') = ''
        then '0'
        when safe_cast(retencao as string) = 'nan'
        then null
        else regexp_replace(safe_cast(retencao as string), '^0+', '')
    end as tipo_retencao_tributos,
    safe_cast({{ clean_cols("ATIVIDAD") }} as string) tipo_atividade_ensino_pesquisa,
    case
        when regexp_replace(safe_cast(natureza as string), '^0+', '') = ''
        then '0'
        when safe_cast(natureza as string) = 'nan'
        then null
        else regexp_replace(safe_cast(natureza as string), '^0+', '')
    end as tipo_natureza_administrativa,
    nullif(safe_cast(nat_jur as string), '') id_natureza_juridica,
    case
        when regexp_replace(safe_cast(clientel as string), '^0+', '') = ''
        then '0'
        when safe_cast(clientel as string) = 'nan'
        then null
        else regexp_replace(safe_cast(clientel as string), '^0+', '')
    end as tipo_fluxo_atendimento,
    safe_cast({{ clean_cols("TP_UNID") }} as string) tipo_unidade,
    case
        when safe_cast({{ clean_cols("TURNO_AT") }} as string) = 'nan'
        then null
        else safe_cast({{ clean_cols("TURNO_AT") }} as string)
    end as tipo_turno,
    safe_cast({{ clean_cols("NIV_HIER") }} as string) tipo_nivel_hierarquia,
    safe_cast({{ clean_cols("TP_PREST") }} as string) tipo_prestador,
    safe_cast(co_banco as string) banco,
    safe_cast(co_agenc as string) agencia,
    safe_cast(c_corren as string) conta_corrente,
    safe_cast(contratm as string) id_contrato_municipio_sus,
    safe_cast(
        safe.parse_date('%Y%m%d', cast(dt_publm as string)) as date
    ) data_publicacao_contrato_municipal,
    safe_cast(
        safe.parse_date('%Y%m%d', cast(dt_puble as string)) as date
    ) data_publicacao_contrato_estadual,
    safe_cast(contrate as string) id_contrato_estado_sus,
    safe_cast(alvara as string) numero_alvara,
    safe_cast(
        safe.parse_date('%Y%m%d', cast(dt_exped as string)) as date
    ) data_expedicao_alvara,
    case
        when safe_cast({{ clean_cols("ORGEXPED") }} as string) = 'nan'
        then null
        else safe_cast({{ clean_cols("ORGEXPED") }} as string)
    end as tipo_orgao_expedidor,
    case
        when safe_cast({{ clean_cols("AV_ACRED") }} as string) = 'nan'
        then null
        else safe_cast({{ clean_cols("AV_ACRED") }} as string)
    end as tipo_avaliacao_acreditacao_hospitalar,
    case
        when regexp_replace(safe_cast(clasaval as string), '^0+', '') = ''
        then '0'
        when safe_cast(clasaval as string) = 'nan'
        then null
        else regexp_replace(safe_cast(clasaval as string), '^0+', '')
    end as tipo_classificacao_acreditacao_hospitalar,
    safe_cast(substr(cast(dt_pnass as string), 1, 4) as int64) as ano_avaliacao_pnass,
    safe_cast(substr(cast(dt_pnass as string), 5, 2) as int64) as mes_avaliacao_pnass,
    safe_cast(nivate_a as int64) indicador_atencao_ambulatorial,
    safe_cast(gesprg1e as int64) indicador_gestao_basica_ambulatorial_estadual,
    safe_cast(gesprg1m as int64) indicador_gestao_basica_ambulatorial_municipal,
    safe_cast(gesprg2e as int64) indicador_gestao_media_ambulatorial_estadual,
    safe_cast(gesprg2m as int64) indicador_gestao_media_ambulatorial_municipal,
    safe_cast(gesprg4e as int64) indicador_gestao_alta_ambulatorial_estadual,
    safe_cast(gesprg4m as int64) indicador_gestao_alta_ambulatorial_municipal,
    safe_cast(nivate_h as int64) indicador_atencao_hospitalar,
    safe_cast(gesprg5e as int64) indicador_gestao_media_hospitalar_estadual,
    safe_cast(gesprg5m as int64) indicador_gestao_media_hospitalar_municipal,
    safe_cast(gesprg6e as int64) indicador_gestao_alta_hospitalar_estadual,
    safe_cast(gesprg6m as int64) indicador_gestao_alta_hospitalar_municipal,
    safe_cast(gesprg3e as int64) indicador_gestao_hospitalar_estadual,
    safe_cast(gesprg3m as int64) indicador_gestao_hospitalar_municipal,
    safe_cast(leithosp as int64) indicador_leito_hospitalar,
    safe_cast(qtleitp1 as int64) quantidade_leito_cirurgico,
    safe_cast(qtleitp2 as int64) quantidade_leito_clinico,
    safe_cast(qtleitp3 as int64) quantidade_leito_complementar,
    safe_cast(qtleit05 as int64) quantidade_leito_repouso_pediatrico_urgencia,
    safe_cast(qtleit06 as int64) quantidade_leito_repouso_feminino_urgencia,
    safe_cast(qtleit07 as int64) quantidade_leito_repouso_masculino_urgencia,
    safe_cast(qtleit08 as int64) quantidade_leito_repouso_indiferenciado_urgencia,
    safe_cast(urgemerg as int64) indicador_instalacao_urgencia,
    safe_cast(qtinst01 as int64) quantidade_consultorio_pediatrico_urgencia,
    safe_cast(qtinst02 as int64) quantidade_consultorio_feminino_urgencia,
    safe_cast(qtinst03 as int64) quantidade_consultorio_masculino_urgencia,
    safe_cast(qtinst04 as int64) quantidade_consultorio_indiferenciado_urgencia,
    safe_cast(qtinst09 as int64) quantidade_consultorio_odontologia_urgencia,
    safe_cast(qtinst05 as int64) quantidade_sala_repouso_pediatrico_urgencia,
    safe_cast(qtinst06 as int64) quantidade_sala_repouso_feminino_urgencia,
    safe_cast(qtinst07 as int64) quantidade_sala_repouso_masculino_urgencia,
    safe_cast(qtinst08 as int64) quantidade_sala_repouso_indiferenciado_urgencia,
    safe_cast(qtleit09 as int64) quantidade_equipos_odontologia_urgencia,
    safe_cast(qtinst10 as int64) quantidade_sala_higienizacao_urgencia,
    safe_cast(qtinst11 as int64) quantidade_sala_gesso_urgencia,
    safe_cast(qtinst12 as int64) quantidade_sala_curativo_urgencia,
    safe_cast(qtinst13 as int64) quantidade_sala_pequena_cirurgia_urgencia,
    safe_cast(qtinst14 as int64) quantidade_consultorio_medico_urgencia,
    safe_cast(atendamb as int64) indicador_instalacao_ambulatorial,
    safe_cast(qtinst15 as int64) quantidade_consultorio_clinica_basica_ambulatorial,
    safe_cast(
        qtinst16 as int64
    ) quantidade_consultorio_clinica_especializada_ambulatorial,
    safe_cast(
        qtinst17 as int64
    ) quantidade_consultorio_clinica_indiferenciada_ambulatorial,
    safe_cast(qtinst18 as int64) quantidade_consultorio_nao_medico_ambulatorial,
    safe_cast(qtinst19 as int64) quantidade_sala_repouso_feminino_ambulatorial,
    safe_cast(qtleit19 as int64) quantidade_leito_repouso_feminino_ambulatorial,
    safe_cast(qtinst20 as int64) quantidade_sala_repouso_masculino_ambulatorial,
    safe_cast(qtleit20 as int64) quantidade_leito_repouso_masculino_ambulatorial,
    safe_cast(qtinst21 as int64) quantidade_sala_repouso_pediatrico_ambulatorial,
    safe_cast(qtleit21 as int64) quantidade_leito_repouso_pediatrico_ambulatorial,
    safe_cast(qtinst22 as int64) quantidade_sala_repouso_indiferenciado_ambulatorial,
    safe_cast(qtleit22 as int64) quantidade_leito_repouso_indiferenciado_ambulatorial,
    safe_cast(qtinst23 as int64) quantidade_consultorio_odontologia_ambulatorial,
    safe_cast(qtleit23 as int64) quantidade_equipos_odontologia_ambulatorial,
    safe_cast(qtinst24 as int64) quantidade_sala_pequena_cirurgia_ambulatorial,
    safe_cast(qtinst25 as int64) quantidade_sala_enfermagem_ambulatorial,
    safe_cast(qtinst26 as int64) quantidade_sala_imunizacao_ambulatorial,
    safe_cast(qtinst27 as int64) quantidade_sala_nebulizacao_ambulatorial,
    safe_cast(qtinst28 as int64) quantidade_sala_gesso_ambulatorial,
    safe_cast(qtinst29 as int64) quantidade_sala_curativo_ambulatorial,
    safe_cast(qtinst30 as int64) quantidade_sala_cirurgia_ambulatorial,
    safe_cast(atendhos as int64) indicador_instalacao_hospitalar,
    safe_cast(centrcir as int64) indicador_instalacao_hospitalar_centro_cirurgico,
    safe_cast(qtinst31 as int64) quantidade_sala_cirurgia_centro_cirurgico,
    safe_cast(qtinst32 as int64) quantidade_sala_recuperacao_centro_cirurgico,
    safe_cast(qtleit32 as int64) quantidade_leito_recuperacao_centro_cirurgico,
    safe_cast(qtinst33 as int64) quantidade_sala_cirurgia_ambulatorial_centro_cirurgico,
    safe_cast(centrobs as int64) indicador_instalacao_hospitalar_centro_obstetrico,
    safe_cast(qtinst34 as int64) quantidade_sala_pre_parto_centro_obstetrico,
    safe_cast(qtleit34 as int64) quantidade_leito_pre_parto_centro_obstetrico,
    safe_cast(qtinst35 as int64) quantidade_sala_parto_normal_centro_obstetrico,
    safe_cast(qtinst36 as int64) quantidade_sala_curetagem_centro_obstetrico,
    safe_cast(qtinst37 as int64) quantidade_sala_cirurgia_centro_obstetrico,
    safe_cast(centrneo as int64) indicador_instalacao_hospitalar_neonatal,
    safe_cast(qtleit38 as int64) quantidade_leito_recem_nascido_normal_neonatal,
    safe_cast(qtleit39 as int64) quantidade_leito_recem_nascido_patologico_neonatal,
    safe_cast(qtleit40 as int64) quantidade_leito_conjunto_neonatal,
    safe_cast(serapoio as int64) indicador_servico_apoio,
    safe_cast(serap01p as int64) indicador_servico_same_spp_proprio,
    safe_cast(serap01t as int64) indicador_servico_same_spp_terceirizado,
    safe_cast(serap02p as int64) indicador_servico_social_proprio,
    safe_cast(serap02t as int64) indicador_servico_social_terceirizado,
    safe_cast(serap03p as int64) indicador_servico_farmacia_proprio,
    safe_cast(serap03t as int64) indicador_servico_farmacia_terceirizado,
    safe_cast(serap04p as int64) indicador_servico_esterilizacao_proprio,
    safe_cast(serap04t as int64) indicador_servico_esterilizacao_terceirizado,
    safe_cast(serap05p as int64) indicador_servico_nutricao_proprio,
    safe_cast(serap05t as int64) indicador_servico_nutricao_terceirizado,
    safe_cast(serap06p as int64) indicador_servico_lactario_proprio,
    safe_cast(serap06t as int64) indicador_servico_lactario_terceirizado,
    safe_cast(serap07p as int64) indicador_servico_banco_leite_proprio,
    safe_cast(serap07t as int64) indicador_servico_banco_leite_terceirizado,
    safe_cast(serap08p as int64) indicador_servico_lavanderia_proprio,
    safe_cast(serap08t as int64) indicador_servico_lavanderia_terceirizado,
    safe_cast(serap09p as int64) indicador_servico_manutencao_proprio,
    safe_cast(serap09t as int64) indicador_servico_manutencao_terceirizado,
    safe_cast(serap10p as int64) indicador_servico_ambulancia_proprio,
    safe_cast(serap10t as int64) indicador_servico_ambulancia_terceirizado,
    safe_cast(serap11p as int64) indicador_servico_necroterio_proprio,
    safe_cast(serap11t as int64) indicador_servico_necroterio_terceirizado,
    safe_cast(coletres as int64) indicador_coleta_residuo,
    safe_cast(res_biol as int64) indicador_coleta_residuo_biologico,
    safe_cast(res_quim as int64) indicador_coleta_residuo_quimico,
    safe_cast(res_radi as int64) indicador_coleta_rejeito_radioativo,
    safe_cast(res_comu as int64) indicador_coleta_rejeito_comum,
    safe_cast(comissao as int64) indicador_comissao,
    safe_cast(comiss01 as int64) indicador_comissao_etica_medica,
    safe_cast(comiss02 as int64) indicador_comissao_etica_enfermagem,
    safe_cast(comiss03 as int64) indicador_comissao_farmacia_terapeutica,
    safe_cast(comiss04 as int64) indicador_comissao_controle_infeccao,
    safe_cast(comiss05 as int64) indicador_comissao_apropriacao_custos,
    safe_cast(comiss06 as int64) indicador_comissao_cipa,
    safe_cast(comiss07 as int64) indicador_comissao_revisao_prontuario,
    safe_cast(comiss08 as int64) indicador_comissao_revisao_documentacao,
    safe_cast(comiss09 as int64) indicador_comissao_analise_obito_biopisias,
    safe_cast(comiss10 as int64) indicador_comissao_investigacao_epidemiologica,
    safe_cast(comiss11 as int64) indicador_comissao_notificacao_doencas,
    safe_cast(comiss12 as int64) indicador_comissao_zoonose_vetores,
    safe_cast(atend_pr as int64) indicador_atendimento_prestado,
    safe_cast(ap01cv01 as int64) indicador_atendimento_internacao_sus,
    safe_cast(ap01cv02 as int64) indicador_atendimento_internacao_particular,
    safe_cast(ap01cv03 as int64) indicador_atendimento_internacao_plano_seguro_proprio,
    safe_cast(ap01cv04 as int64) indicador_atendimento_internacao_plano_seguro_terceiro,
    safe_cast(ap01cv05 as int64) indicador_atendimento_internacao_plano_saude_publico,
    safe_cast(ap01cv06 as int64) indicador_atendimento_internacao_plano_saude_privado,
    safe_cast(ap02cv01 as int64) indicador_atendimento_ambulatorial_sus,
    safe_cast(ap02cv02 as int64) indicador_atendimento_ambulatorial_particular,
    safe_cast(
        ap02cv03 as int64
    ) indicador_atendimento_ambulatorial_plano_seguro_proprio,
    safe_cast(
        ap02cv04 as int64
    ) indicador_atendimento_ambulatorial_plano_seguro_terceiro,
    safe_cast(ap02cv05 as int64) indicador_atendimento_ambulatorial_plano_saude_publico,
    safe_cast(ap02cv06 as int64) indicador_atendimento_ambulatorial_plano_saude_privado,
    safe_cast(ap03cv01 as int64) indicador_atendimento_sadt_sus,
    safe_cast(ap03cv02 as int64) indicador_atendimento_sadt_privado,
    safe_cast(ap03cv03 as int64) indicador_atendimento_sadt_plano_seguro_proprio,
    safe_cast(ap03cv04 as int64) indicador_atendimento_sadt_plano_seguro_terceiro,
    safe_cast(ap03cv05 as int64) indicador_atendimento_sadt_plano_saude_publico,
    safe_cast(ap03cv06 as int64) indicador_atendimento_sadt_plano_saude_privado,
    safe_cast(ap04cv01 as int64) indicador_atendimento_urgencia_sus,
    safe_cast(ap04cv02 as int64) indicador_atendimento_urgencia_privado,
    safe_cast(ap04cv03 as int64) indicador_atendimento_urgencia_plano_seguro_proprio,
    safe_cast(ap04cv04 as int64) indicador_atendimento_urgencia_plano_seguro_terceiro,
    safe_cast(ap04cv05 as int64) indicador_atendimento_urgencia_plano_saude_publico,
    safe_cast(ap04cv06 as int64) indicador_atendimento_urgencia_plano_saude_privado,
    safe_cast(ap05cv01 as int64) indicador_atendimento_outros_sus,
    safe_cast(ap05cv02 as int64) indicador_atendimento_outros_privado,
    safe_cast(ap05cv03 as int64) indicador_atendimento_outros_plano_seguro_proprio,
    safe_cast(ap05cv04 as int64) indicador_atendimento_outros_plano_seguro_terceiro,
    safe_cast(ap05cv05 as int64) indicador_atendimento_outros_plano_saude_publico,
    safe_cast(ap05cv06 as int64) indicador_atendimento_outros_plano_saude_privado,
    safe_cast(ap06cv01 as int64) indicador_atendimento_vigilancia_sus,
    safe_cast(ap06cv02 as int64) indicador_atendimento_vigilancia_privado,
    safe_cast(ap06cv03 as int64) indicador_atendimento_vigilancia_plano_seguro_proprio,
    safe_cast(ap06cv04 as int64) indicador_atendimento_vigilancia_plano_seguro_terceiro,
    safe_cast(ap06cv05 as int64) indicador_atendimento_vigilancia_plano_saude_publico,
    safe_cast(ap06cv06 as int64) indicador_atendimento_vigilancia_plano_saude_privado,
    safe_cast(ap07cv01 as int64) indicador_atendimento_regulacao_sus,
    safe_cast(ap07cv02 as int64) indicador_atendimento_regulacao_privado,
    safe_cast(ap07cv03 as int64) indicador_atendimento_regulacao_plano_seguro_proprio,
    safe_cast(ap07cv04 as int64) indicador_atendimento_regulacao_plano_seguro_terceiro,
    safe_cast(ap07cv05 as int64) indicador_atendimento_regulacao_plano_saude_publico,
    safe_cast(ap07cv06 as int64) indicador_atendimento_regulacao_plano_saude_privado
from cnes_add_muni
{% if is_incremental() %}
    where

        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
