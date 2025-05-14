{{
    config(
        alias="microdados_dengue",
        schema="br_ms_sinan",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2000, "end": 2025, "interval": 1},
        },
        cluster_by=["sigla_uf_notificacao", "sigla_uf_residencia"],
        labels={"tema": "saude"},
    )
}}
{%- set columns = adapter.get_columns_in_relation(this) -%}
with
    sql as (
        select
            safe_cast(ano as int64) ano,
            safe_cast(tp_not as string) tipo_notificacao,
            case
                when id_agravo = 'A900'
                then 'A90'
                when id_agravo = 'A90  '
                then 'A90'
                else trim(id_agravo)
            end id_agravo,
            case
                when dt_notific = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_notific, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_notificacao,

            safe_cast(sem_not as int64) semana_notificacao,
            case
                when sigla_uf_notificacao = '11'
                then 'RO'
                when sigla_uf_notificacao = '12'
                then 'AC'
                when sigla_uf_notificacao = '13'
                then 'AM'
                when sigla_uf_notificacao = '14'
                then 'RR'
                when sigla_uf_notificacao = '15'
                then 'PA'
                when sigla_uf_notificacao = '16'
                then 'AP'
                when sigla_uf_notificacao = '17'
                then 'TO'
                when sigla_uf_notificacao = '21'
                then 'MA'
                when sigla_uf_notificacao = '22'
                then 'PI'
                when sigla_uf_notificacao = '23'
                then 'CE'
                when sigla_uf_notificacao = '24'
                then 'RN'
                when sigla_uf_notificacao = '25'
                then 'PB'
                when sigla_uf_notificacao = '26'
                then 'PE'
                when sigla_uf_notificacao = '27'
                then 'AL'
                when sigla_uf_notificacao = '28'
                then 'SE'
                when sigla_uf_notificacao = '29'
                then 'BA'
                when sigla_uf_notificacao = '31'
                then 'MG'
                when sigla_uf_notificacao = '32'
                then 'ES'
                when sigla_uf_notificacao = '33'
                then 'RJ'
                when sigla_uf_notificacao = '35'
                then 'SP'
                when sigla_uf_notificacao = '41'
                then 'PR'
                when sigla_uf_notificacao = '42'
                then 'SC'
                when sigla_uf_notificacao = '43'
                then 'RS'
                when sigla_uf_notificacao = '50'
                then 'MS'
                when sigla_uf_notificacao = '51'
                then 'MT'
                when sigla_uf_notificacao = '52'
                then 'GO'
                when sigla_uf_notificacao = '53'
                then 'DF'
                when sigla_uf_notificacao = '"s'
                then null
                else sigla_uf_notificacao
            end sigla_uf_notificacao,
            case
                when id_regiona = '' then null else id_regiona
            end id_regional_saude_notificacao,
            case
                when id_municip = '' then null else id_municip
            end id_municipio_notificacao,
            case when id_unidade = '' then null else id_unidade end id_estabelecimento,
            case
                when dt_sin_pri = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_sin_pri, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_primeiros_sintomas,

            safe_cast(sem_pri as int64) semana_sintomas,
            safe_cast(id_pais as string) pais_residencia,
            case
                when sg_uf = '11'
                then 'RO'
                when sg_uf = '12'
                then 'AC'
                when sg_uf = '13'
                then 'AM'
                when sg_uf = '14'
                then 'RR'
                when sg_uf = '15'
                then 'PA'
                when sg_uf = '16'
                then 'AP'
                when sg_uf = '17'
                then 'TO'
                when sg_uf = '21'
                then 'MA'
                when sg_uf = '22'
                then 'PI'
                when sg_uf = '23'
                then 'CE'
                when sg_uf = '24'
                then 'RN'
                when sg_uf = '25'
                then 'PB'
                when sg_uf = '26'
                then 'PE'
                when sg_uf = '27'
                then 'AL'
                when sg_uf = '28'
                then 'SE'
                when sg_uf = '29'
                then 'BA'
                when sg_uf = '31'
                then 'MG'
                when sg_uf = '32'
                then 'ES'
                when sg_uf = '33'
                then 'RJ'
                when sg_uf = '35'
                then 'SP'
                when sg_uf = '41'
                then 'PR'
                when sg_uf = '42'
                then 'SC'
                when sg_uf = '43'
                then 'RS'
                when sg_uf = '50'
                then 'MS'
                when sg_uf = '51'
                then 'MT'
                when sg_uf = '52'
                then 'GO'
                when sg_uf = '53'
                then 'DF'
                when sg_uf = '1 '
                then null
                when sg_uf = '07'
                then null
                when sg_uf = '00'
                then null
                when sg_uf = 'MH'
                then null
                when sg_uf = 'NG'
                then null
                when sg_uf = '2 '
                then null
                when sg_uf = '61'
                then null
                when sg_uf = 'F '
                then null
                when sg_uf = 'MF'
                then null
                else sg_uf
            end sigla_uf_residencia,
            safe_cast(id_rg_resi as string) id_regional_saude_residencia,
            safe_cast(id_mn_resi as string) id_municipio_residencia,
            safe_cast(ano_nasc as int64) ano_nascimento_paciente,

            case
                when dt_nasc = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_nasc, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_nascimento_paciente,

            safe_cast(nu_idade_n as int64) idade_paciente,
            safe_cast(cs_sexo as string) sexo_paciente,
            case
                when cs_raca = '@' then null when cs_raca = '' then null else cs_raca
            end raca_cor_paciente,
            case
                when cs_escol_n = '0'
                then null
                else trim(regexp_replace(cs_escol_n, r'^0+', ''))
            end escolaridade_paciente,
            safe_cast(id_ocupa_n as string) ocupacao_paciente,
            safe_cast(cs_gestant as string) gestante_paciente,
            safe_cast(auto_imune as string) possui_doenca_autoimune,
            safe_cast(diabetes as string) possui_diabetes,
            safe_cast(hematolog as string) possui_doencas_hematologicas,
            safe_cast(hepatopat as string) possui_hepatopatias,
            safe_cast(renal as string) possui_doenca_renal,
            safe_cast(hipertensa as string) possui_hipertensao,
            safe_cast(acido_pept as string) possui_doenca_acido_peptica,
            safe_cast(vacinado as string) paciente_vacinado,

            case
                when dt_dose = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_dose, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_vacina,

            case
                when dt_invest = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_invest, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_investigacao,
            safe_cast(febre as string) as apresenta_febre,
            case
                when dt_febre = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_febre, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_febre,
            safe_cast(duracao as string) duracao_febre,
            safe_cast(cefaleia as string) apresenta_cefaleia,
            safe_cast(exantema as string) apresenta_exantema,
            safe_cast(dor_costas as string) apresenta_dor_costas,
            safe_cast(prostacao as string) apresenta_prostacao,
            safe_cast(mialgia as string) apresenta_mialgia,
            safe_cast(vomito as string) apresenta_vomito,
            safe_cast(nauseas as string) apresenta_nausea,
            safe_cast(diarreia as string) apresenta_diarreia,
            safe_cast(conjuntvit as string) apresenta_conjutivite,
            safe_cast(dor_retro as string) apresenta_dor_retroorbital,
            safe_cast(artralgia as string) apresenta_artralgia,
            safe_cast(artrite as string) apresenta_artrite,
            safe_cast(leucopenia as string) apresenta_leucopenia,
            safe_cast(epistaxe as string) apresenta_epistaxe,
            safe_cast(petequia_n as string) apresenta_petequias,
            safe_cast(gengivo as string) apresenta_gengivorragia,
            safe_cast(metro as string) apresenta_metrorragia,
            safe_cast(hematura as string) apresenta_hematuria,
            safe_cast(sangram as string) apresenta_sangramento,
            safe_cast(complica as string) apresenta_complicacao,
            safe_cast(ascite as string) apresenta_ascite,
            safe_cast(pleural as string) apresenta_pleurite,
            safe_cast(pericardi as string) apresenta_pericardite,
            safe_cast(abdominal as string) apresenta_dor_abdominal,
            safe_cast(hepato as string) apresenta_hepatomegalia,
            safe_cast(miocardi as string) apresenta_miocardite,
            safe_cast(hipotensao as string) apresenta_hipotensao,
            safe_cast(choque as string) apresenta_choque,
            safe_cast(insuficien as string) apresenta_insuficiencia_orgao,
            safe_cast(outros as string) apresenta_sintoma_outro,
            safe_cast(sin_out as string) apresenta_qual_sintoma,
            safe_cast(laco_n as string) prova_laco,
            case
                when dt_choque = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_choque, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_choque,
            safe_cast(hospitaliz as string) internacao,
            case
                when dt_interna = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_interna, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_internacao,
            case
                when uf = '11'
                then 'RO'
                when uf = '12'
                then 'AC'
                when uf = '13'
                then 'AM'
                when uf = '14'
                then 'RR'
                when uf = '15'
                then 'PA'
                when uf = '16'
                then 'AP'
                when uf = '17'
                then 'TO'
                when uf = '21'
                then 'MA'
                when uf = '22'
                then 'PI'
                when uf = '23'
                then 'CE'
                when uf = '24'
                then 'RN'
                when uf = '25'
                then 'PB'
                when uf = '26'
                then 'PE'
                when uf = '27'
                then 'AL'
                when uf = '28'
                then 'SE'
                when uf = '29'
                then 'BA'
                when uf = '31'
                then 'MG'
                when uf = '32'
                then 'ES'
                when uf = '33'
                then 'RJ'
                when uf = '35'
                then 'SP'
                when uf = '41'
                then 'PR'
                when uf = '42'
                then 'SC'
                when uf = '43'
                then 'RS'
                when uf = '50'
                then 'MS'
                when uf = '51'
                then 'MT'
                when uf = '52'
                then 'GO'
                when uf = '53'
                then 'DF'
                when uf = '`'
                then null
                when uf = ' `'
                then null
                else uf
            end sigla_uf_internacao,
            case
                when municipio = '280020' then null else municipio
            end id_municipio_internacao,
            safe_cast(alrm_hipot as string) alarme_hipotensao,
            safe_cast(alrm_plaq as string) alarme_plaqueta,
            safe_cast(alrm_vom as string) alarme_vomito,
            safe_cast(alrm_sang as string) alarme_sangramento,
            safe_cast(alrm_hemat as string) alarme_hematocrito,
            safe_cast(alrm_abdom as string) alarme_dor_abdominal,
            safe_cast(alrm_letar as string) alarme_letargia,
            safe_cast(alrm_hepat as string) alarme_hepatomegalia,
            safe_cast(alrm_liq as string) alarme_liquidos,
            case
                when dt_alrm = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_alrm, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_alarme,

            safe_cast(grav_pulso as string) grave_pulso,
            safe_cast(grav_conv as string) grave_convulsao,
            safe_cast(grav_ench as string) grave_enchimento_capilar,
            safe_cast(grav_insuf as string) grave_insuficiencia_respiratoria,
            safe_cast(grav_taqui as string) grave_taquicardia,
            safe_cast(grav_extre as string) grave_extremidade_fria,
            safe_cast(grav_hipot as string) grave_hipotensao,
            safe_cast(grav_hemat as string) grave_hematemese,
            safe_cast(grav_melen as string) grave_melena,
            safe_cast(grav_metro as string) grave_metrorragia,
            safe_cast(grav_sang as string) grave_sangramento,
            safe_cast(grav_ast as string) grave_ast_alt,
            safe_cast(grav_mioc as string) grave_miocardite,
            safe_cast(grav_consc as string) grave_consciencia,
            safe_cast(grav_orgao as string) grave_orgaos,
            case
                when dt_grav = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_grav, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_sinais_gravidade,
            case
                when dt_col_hem = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_col_hem, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_hematocrito,

            safe_cast(hema_maior as string) hematocrito_maior,

            case
                when dt_col_plq = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_col_plq, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_plaquetas,

            safe_cast(palq_maior as string) plaqueta_maior,

            case
                when dt_col_he2 = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_col_he2, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_hematocrito_2,

            safe_cast(hema_menor as string) hematocrito_menor,

            case
                when dt_col_pl2 = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_col_pl2, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_plaquetas_2,

            safe_cast(plaq_menor as string) plaqueta_menor,

            case
                when dt_chik_s1 = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_chik_s1, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_sorologia1_chikungunya,

            case
                when dt_soror1 = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_soror1, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_resultado_sorologia1_chikungunya,
            case
                when dt_soror2 = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_soror2, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_resultado_sorologia2_chikungunya,
            safe_cast(res_chiks1 as string) resultado_sorologia1_chikungunya,
            safe_cast(s1_igm as string) sorologia1_igm,
            safe_cast(s1_igg as string) sorologia1_igg,
            safe_cast(s1_tit1 as string) sorologia1_tit1,
            case
                when dt_chik_s2 = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_chik_s2, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_sorologia2_chikungunya,
            safe_cast(res_chiks2 as string) resultado_sorologia2_chikungunya,
            safe_cast(s2_igm as string) sorologia2_igm,
            safe_cast(s2_igg as string) sorologia2_igg,
            safe_cast(s2_tit1 as string) sorologia2_tit1,
            safe_cast(resul_prnt as string) resultado_prnt,
            case
                when dt_prnt = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_prnt, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_prnt,
            case
                when dt_ns1 = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_ns1, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_ns1,

            safe_cast(resul_ns1 as string) resultado_ns1,

            case
                when dt_viral = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_viral, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_viral,

            safe_cast(resul_vi_n as string) resultado_viral,

            case
                when dt_pcr = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_pcr, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_pcr,

            safe_cast(resul_pcr_ as string) resultado_pcr,
            safe_cast(amos_pcr as string) amostra_pcr,
            safe_cast(amos_out as string) amostra_outra,
            safe_cast(tecnica as string) tecnica,
            safe_cast(resul_out as string) resultado_amostra_outra,

            case
                when dt_soro = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_soro, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_sorologia_dengue,

            safe_cast(resul_soro as string) resultado_sorologia_dengue,
            safe_cast(sorotipo as string) sorotipo,
            safe_cast(histopa_n as string) histopatologia,
            safe_cast(imunoh_n as string) imunohistoquimica,
            safe_cast(mani_hemor as string) manifestacao_hemorragica,
            safe_cast(classi_fin as string) classificacao_final,
            safe_cast(criterio as string) criterio_confirmacao,
            safe_cast(con_fhd as string) caso_fhd,
            safe_cast(tpautocto as string) caso_autoctone,
            safe_cast(copaisinf as string) pais_infeccao,
            case
                when coufinf = '11'
                then 'RO'
                when coufinf = '12'
                then 'AC'
                when coufinf = '13'
                then 'AM'
                when coufinf = '14'
                then 'RR'
                when coufinf = '15'
                then 'PA'
                when coufinf = '16'
                then 'AP'
                when coufinf = '17'
                then 'TO'
                when coufinf = '21'
                then 'MA'
                when coufinf = '22'
                then 'PI'
                when coufinf = '23'
                then 'CE'
                when coufinf = '24'
                then 'RN'
                when coufinf = '25'
                then 'PB'
                when coufinf = '26'
                then 'PE'
                when coufinf = '27'
                then 'AL'
                when coufinf = '28'
                then 'SE'
                when coufinf = '29'
                then 'BA'
                when coufinf = '31'
                then 'MG'
                when coufinf = '32'
                then 'ES'
                when coufinf = '33'
                then 'RJ'
                when coufinf = '35'
                then 'SP'
                when coufinf = '41'
                then 'PR'
                when coufinf = '42'
                then 'SC'
                when coufinf = '43'
                then 'RS'
                when coufinf = '50'
                then 'MS'
                when coufinf = '51'
                then 'MT'
                when coufinf = '52'
                then 'GO'
                when coufinf = '53'
                then 'DF'
                else coufinf
            end sigla_uf_infeccao,
            case
                when comuninf = '       ' then null else comuninf
            end id_municipio_infeccao,
            safe_cast(doenca_tra as string) doenca_trabalho,
            safe_cast(clinc_chik as string) apresentacao_clinica,
            case
                when evolucao = ']'
                then null
                when evolucao = '0'
                then null
                else evolucao
            end evolucao_caso,
            case
                when dt_obito = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_obito, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_obito,

            case
                when dt_encerra = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_encerra, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_encerramento,

            safe_cast(tp_sistema as string) tipo_sistema,

            case
                when dt_digita = ''
                then null
                else
                    safe_cast(
                        format_date(
                            '%Y-%m-%d',
                            safe.parse_date(
                                '%Y%m%d', regexp_replace(dt_digita, r'[^0-9]', '')
                            )
                        ) as date
                    )
            end data_digitacao,
            safe_cast(nduplic_n as string) duplicidade,
            safe_cast(cs_flxret as string) fluxo_retorno,
        from {{ set_datalake_project("br_ms_sinan_staging.microdados_dengue") }}
    ),
    tabelas_join as (
        select
            t1.*,
            mun_residencia.id_municipio as novo_id_municipio_residencia,
            mun_internacao.id_municipio as novo_id_municipio_internacao,
            mun_infeccao.id_municipio as novo_id_municipio_infeccao,
            mun_notificacao.id_municipio as novo_id_municipio_notificacao,
            concat(
                left(cast(semana_notificacao as string), 4),
                "-",
                right(cast(semana_notificacao as string), 2)
            ) as semana_notificacao_certa,
            concat(
                left(cast(semana_sintomas as string), 4),
                "-",
                right(cast(semana_sintomas as string), 2)
            ) as semana_sintomas_certa
        from sql as t1
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` as mun_residencia
            on t1.id_municipio_residencia = mun_residencia.id_municipio_6
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` as mun_internacao
            on t1.id_municipio_internacao = mun_internacao.id_municipio_6
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` as mun_infeccao
            on t1.id_municipio_infeccao = mun_infeccao.id_municipio_6
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` as mun_notificacao
            on t1.id_municipio_notificacao = mun_notificacao.id_municipio_6
        where ano > 2006
        union all

        select
            t1.*,
            t1.id_municipio_residencia as novo_id_municipio_residencia,
            t1.id_municipio_internacao as novo_id_municipio_internacao,
            t1.id_municipio_infeccao as novo_id_municipio_infeccao,
            t1.id_municipio_notificacao as novo_id_municipio_notificacao,
            concat(
                right(cast(semana_notificacao as string), 4),
                "-",
                left(cast(semana_notificacao as string), 2)
            ) as semana_notificacao_certa,
            concat(
                right(cast(semana_sintomas as string), 4),
                "-",
                left(cast(semana_sintomas as string), 2)
            ) as semana_sintomas_certa
        from sql as t1
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` as mun_residencia
            on t1.id_municipio_residencia = mun_residencia.id_municipio
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` as mun_internacao
            on t1.id_municipio_internacao = mun_internacao.id_municipio
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` as mun_infeccao
            on t1.id_municipio_infeccao = mun_infeccao.id_municipio
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` as mun_notificacao
            on t1.id_municipio_notificacao = mun_notificacao.id_municipio
        where ano <= 2006
    ),
    table_final as (
        select
            ano,
            tipo_notificacao,
            id_agravo,
            data_notificacao,
            semana_notificacao_certa as semana_notificacao,
            sigla_uf_notificacao,
            id_regional_saude_notificacao,
            novo_id_municipio_notificacao as id_municipio_notificacao,
            id_estabelecimento,
            data_primeiros_sintomas,
            semana_sintomas_certa as semana_sintomas,
            pais_residencia,
            sigla_uf_residencia,
            id_regional_saude_residencia,
            novo_id_municipio_residencia as id_municipio_residencia,
            ano_nascimento_paciente,
            data_nascimento_paciente,
            concat(
                left(cast(idade_paciente as string), 1),
                "-",
                right(cast(idade_paciente as string), 3)
            ) as idade_paciente,
            case
                when sexo_paciente = 'O' then null else sexo_paciente
            end sexo_paciente,
            raca_cor_paciente,
            escolaridade_paciente,
            ocupacao_paciente,
            gestante_paciente,
            possui_doenca_autoimune,
            possui_diabetes,
            possui_doencas_hematologicas,
            possui_hepatopatias,
            possui_doenca_renal,
            possui_hipertensao,
            possui_doenca_acido_peptica,
            paciente_vacinado,
            data_vacina,
            data_investigacao,
            apresenta_febre,
            data_febre,
            duracao_febre,
            apresenta_cefaleia,
            apresenta_exantema,
            apresenta_dor_costas,
            case
                when apresenta_prostacao = '6' then null else apresenta_prostacao
            end as apresenta_prostacao,
            apresenta_mialgia,
            apresenta_vomito,
            apresenta_nausea,
            apresenta_diarreia,
            apresenta_conjutivite,
            apresenta_dor_retroorbital,
            apresenta_artralgia,
            apresenta_artrite,
            apresenta_leucopenia,
            case
                when apresenta_epistaxe = "!"
                then null
                when apresenta_epistaxe = '4'
                then null
                else apresenta_epistaxe
            end apresenta_epistaxe,
            case
                when apresenta_petequias = '4' then null else apresenta_petequias
            end as apresenta_petequias,
            apresenta_gengivorragia,
            case
                when apresenta_metrorragia = '4'
                then null
                when apresenta_metrorragia = '3'
                then null
                else apresenta_metrorragia
            end as apresenta_metrorragia,
            apresenta_hematuria,
            case
                when apresenta_sangramento = '4' then null else apresenta_sangramento
            end as apresenta_sangramento,
            apresenta_complicacao,
            apresenta_ascite,
            apresenta_pleurite,
            apresenta_pericardite,
            apresenta_dor_abdominal,
            apresenta_hepatomegalia,
            apresenta_miocardite,
            apresenta_hipotensao,
            apresenta_choque,
            apresenta_insuficiencia_orgao,
            apresenta_sintoma_outro,
            apresenta_qual_sintoma,
            prova_laco,
            case
                when extract(year from data_choque) > extract(year from current_date)
                then null
                else data_choque
            end data_choque,
            internacao,
            data_internacao,
            sigla_uf_internacao,
            novo_id_municipio_internacao as id_municipio_internacao,
            alarme_hipotensao,
            alarme_plaqueta,
            alarme_vomito,
            alarme_sangramento,
            alarme_hematocrito,
            alarme_dor_abdominal,
            alarme_letargia,
            alarme_hepatomegalia,
            alarme_liquidos,
            case
                when extract(year from data_alarme) > extract(year from current_date)
                then null
                else data_alarme
            end data_alarme,
            grave_pulso,
            grave_convulsao,
            grave_enchimento_capilar,
            grave_insuficiencia_respiratoria,
            grave_taquicardia,
            grave_extremidade_fria,
            grave_hipotensao,
            grave_hematemese,
            grave_melena,
            grave_metrorragia,
            grave_sangramento,
            grave_ast_alt,
            grave_miocardite,
            grave_consciencia,
            grave_orgaos,
            data_hematocrito,
            case
                when
                    extract(year from data_sinais_gravidade)
                    > extract(year from current_date)
                then null
                else data_sinais_gravidade
            end data_sinais_gravidade,
            hematocrito_maior,
            case
                when extract(year from data_plaquetas) > extract(year from current_date)
                then null
                else data_plaquetas
            end data_plaquetas,
            plaqueta_maior,
            data_hematocrito_2,
            hematocrito_menor,
            data_plaquetas_2,
            plaqueta_menor,
            case
                when
                    extract(year from data_sorologia1_chikungunya)
                    > extract(year from current_date)
                then null
                else data_sorologia1_chikungunya
            end data_sorologia1_chikungunya,
            data_resultado_sorologia1_chikungunya,
            case
                when resultado_sorologia1_chikungunya = "9"
                then null
                else resultado_sorologia1_chikungunya
            end resultado_sorologia1_chikungunya,
            sorologia1_igm,
            sorologia1_igg,
            sorologia1_tit1,
            resultado_sorologia2_chikungunya,
            case
                when
                    extract(year from data_sorologia2_chikungunya)
                    > extract(year from current_date)
                then null
                else data_sorologia2_chikungunya
            end data_sorologia2_chikungunya,
            case
                when
                    extract(year from data_resultado_sorologia2_chikungunya)
                    > extract(year from current_date)
                then null
                else data_resultado_sorologia2_chikungunya
            end data_resultado_sorologia2_chikungunya,
            sorologia2_igm,
            sorologia2_igg,
            sorologia2_tit1,
            resultado_prnt,
            case
                when extract(year from data_prnt) > extract(year from current_date)
                then null
                else data_prnt
            end data_prnt,
            data_ns1,
            resultado_ns1,
            data_viral,
            case
                when resultado_viral = '5' then null else resultado_viral
            end resultado_viral,
            data_pcr,
            resultado_pcr,
            amostra_pcr,
            amostra_outra,
            tecnica,
            resultado_amostra_outra,
            data_sorologia_dengue,
            case
                when resultado_sorologia_dengue = '"'
                then null
                else resultado_sorologia_dengue
            end resultado_sorologia_dengue,
            sorotipo,
            histopatologia,
            imunohistoquimica,
            manifestacao_hemorragica,
            classificacao_final,
            case
                when criterio_confirmacao = 'r' then null else criterio_confirmacao
            end criterio_confirmacao,
            caso_fhd,
            caso_autoctone,
            pais_infeccao,
            sigla_uf_infeccao,
            novo_id_municipio_infeccao as id_municipio_infeccao,
            doenca_trabalho,
            apresentacao_clinica,
            evolucao_caso,
            case
                when extract(year from data_obito) > extract(year from current_date)
                then null
                else data_obito
            end data_obito,
            case
                when
                    extract(year from data_encerramento)
                    > extract(year from current_date)
                then null
                else data_encerramento
            end data_encerramento,
            tipo_sistema,
            case
                when extract(year from data_digitacao) > extract(year from current_date)
                then null
                else data_digitacao
            end data_digitacao,
        from tabelas_join
    )
select
    {% for col in columns %}
        {% if col.data_type == "STRING" %}
            case
                when trim({{ col.name }}) = '' then null else {{ col.name }}
            end as {{ col.name }}
        {% else %} {{ col.name }}
        {% endif %}
        {% if not loop.last %}, {% endif %}
    {% endfor %}
from table_final
