{{
    config(
        schema="br_ms_sinan",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2008, "end": 2025, "interval": 1},
        },
        labels={"tema": "saude"},
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(data_notificacao as date) data_notificacao,
    safe_cast(id_municipio_notificacao as string) id_municipio_notificacao,
    safe_cast(id_municipio_6_notificacao as string) id_municipio_6_notificacao,
    safe_cast(sistema as string) sistema,
    safe_cast(id_estabelecimento_cnes as string) id_estabelecimento_cnes,
    safe_cast(semana_notificacao as int64) semana_notificacao,
    safe_cast(data_primeiros_sintomas as date) data_primeiros_sintomas,
    safe_cast(semana_sintomas as int64) semana_sintomas,
    safe_cast(
        safe_cast(paciente_estrangeiro as numeric) as string
    ) paciente_estrangeiro,
    safe_cast(pais_residencia as string) pais_residencia,
    safe_cast(sigla_uf_residencia as string) sigla_uf_residencia,
    safe_cast(id_municipio_residencia as string) id_municipio_residencia,
    safe_cast(id_municipio_6_residencia as string) id_municipio_6_residencia,
    safe_cast(safe_cast(zona_residencia as numeric) as string) zona_residencia,
    safe_cast(
        safe_cast(caso_natural_residencia as numeric) as string
    ) caso_natural_residencia,
    safe_cast(data_nascimento as string) data_nascimento,
    safe_cast(safe_cast(sexo as numeric) as string) sexo,
    safe_cast(safe_cast(raca_cor as numeric) as string) raca_cor,
    safe_cast(etnia as string) etnia,
    safe_cast(safe_cast(gestante as numeric) as string) gestante,
    safe_cast(safe_cast(puerpera as numeric) as string) puerpera,
    safe_cast(safe_cast(escolaridade as numeric) as string) escolaridade,
    safe_cast(safe_cast(ocupacao as numeric) as string) ocupacao,
    safe_cast(
        safe_cast(doenca_relacionada_trabalho as numeric) as string
    ) doenca_relacionada_trabalho,
    safe_cast(safe_cast(tabagista as numeric) as string) tabagista,
    safe_cast(safe_cast(sindrome_gripal as numeric) as string) sindrome_gripal,
    safe_cast(safe_cast(infeccao_hospitalar as numeric) as string) infeccao_hospitalar,
    safe_cast(safe_cast(contato_aves_suinos as numeric) as string) contato_aves_suinos,
    safe_cast(
        safe_cast(contato_outro_animal as numeric) as string
    ) contato_outro_animal,
    safe_cast(safe_cast(apresenta_febre as numeric) as string) apresenta_febre,
    safe_cast(safe_cast(apresenta_tosse as numeric) as string) apresenta_tosse,
    safe_cast(safe_cast(apresenta_calafrio as numeric) as string) apresenta_calafrio,
    safe_cast(
        safe_cast(apresenta_dor_garganta as numeric) as string
    ) apresenta_dor_garganta,
    safe_cast(safe_cast(apresenta_dispneia as numeric) as string) apresenta_dispneia,
    safe_cast(safe_cast(apresenta_artralgia as numeric) as string) apresenta_artralgia,
    safe_cast(safe_cast(apresenta_mialgia as numeric) as string) apresenta_mialgia,
    safe_cast(
        safe_cast(apresenta_conjuntivite as numeric) as string
    ) apresenta_conjuntivite,
    safe_cast(safe_cast(apresenta_coriza as numeric) as string) apresenta_coriza,
    safe_cast(
        safe_cast(apresenta_desconforto_respiratorio as numeric) as string
    ) apresenta_desconforto_respiratorio,
    safe_cast(
        safe_cast(apresenta_saturacao_anormal as numeric) as string
    ) apresenta_saturacao_anormal,
    safe_cast(safe_cast(apresenta_diarreia as numeric) as string) apresenta_diarreia,
    safe_cast(safe_cast(apresenta_vomito as numeric) as string) apresenta_vomito,
    safe_cast(
        safe_cast(apresenta_dor_abdominal as numeric) as string
    ) apresenta_dor_abdominal,
    safe_cast(safe_cast(apresenta_fadiga as numeric) as string) apresenta_fadiga,
    safe_cast(
        safe_cast(apresenta_perda_olfato as numeric) as string
    ) apresenta_perda_olfato,
    safe_cast(
        safe_cast(apresenta_perda_paladar as numeric) as string
    ) apresenta_perda_paladar,
    safe_cast(
        safe_cast(apresenta_outros_sintomas as numeric) as string
    ) apresenta_outros_sintomas,
    safe_cast(qual_outro_sintoma as string) qual_outro_sintoma,
    safe_cast(
        safe_cast(apresenta_fator_risco as numeric) as string
    ) apresenta_fator_risco,
    safe_cast(safe_cast(possui_cardiopatia as numeric) as string) possui_cardiopatia,
    safe_cast(safe_cast(possui_pneumopatia as numeric) as string) possui_pneumopatia,
    safe_cast(
        safe_cast(possui_hemoglobinopatia as numeric) as string
    ) possui_hemoglobinopatia,
    safe_cast(
        safe_cast(possui_doenca_metabolica as numeric) as string
    ) possui_doenca_metabolica,
    safe_cast(
        safe_cast(possui_sindrome_down as numeric) as string
    ) possui_sindrome_down,
    safe_cast(safe_cast(possui_hepatite as numeric) as string) possui_hepatite,
    safe_cast(
        safe_cast(possui_doenca_neurologica as numeric) as string
    ) possui_doenca_neurologica,
    safe_cast(
        safe_cast(possui_imunodeficiencia as numeric) as string
    ) possui_imunodeficiencia,
    safe_cast(safe_cast(possui_doenca_renal as numeric) as string) possui_doenca_renal,
    safe_cast(safe_cast(possui_obesidade as numeric) as string) possui_obesidade,
    safe_cast(imc_obeso as string) imc_obeso,
    safe_cast(safe_cast(possui_hematologia as numeric) as string) possui_hematologia,
    safe_cast(safe_cast(possui_asma as numeric) as string) possui_asma,
    safe_cast(safe_cast(possui_diabetes as numeric) as string) possui_diabetes,
    safe_cast(
        safe_cast(possui_outra_morbidade as numeric) as string
    ) possui_outra_morbidade,
    safe_cast(qual_outra_morbidade as string) qual_outra_morbidade,
    safe_cast(safe_cast(vacina_gripe as numeric) as string) vacina_gripe,
    safe_cast(data_ultima_dose as date) data_ultima_dose,
    safe_cast(safe_cast(vacina_gripe_mae as numeric) as string) vacina_gripe_mae,
    safe_cast(data_vacina_mae as date) data_vacina_mae,
    safe_cast(safe_cast(mae_amamenta as numeric) as string) mae_amamenta,
    safe_cast(data_vacina_crianca_dose_unica as date) data_vacina_crianca_dose_unica,
    safe_cast(data_vacina_crianca_1dose as date) data_vacina_crianca_1dose,
    safe_cast(data_vacina_crianca_2dose as date) data_vacina_crianca_2dose,
    safe_cast(safe_cast(vacina_covid as numeric) as string) vacina_covid,
    safe_cast(data_vacina_covid_dose_1 as date) data_vacina_covid_dose1,
    safe_cast(data_vacina_covid_dose_2 as date) data_vacina_covid_dose2,
    safe_cast(laboratorio_vacina_covid as string) laboratorio_vacina_covid,
    safe_cast(lote_dose1_vacina_covid as string) lote_dose1_vacina_covid,
    safe_cast(lote_dose2_vacina_covid as string) lote_dose2_vacina_covid,
    safe_cast(
        safe_cast(fonte_dados_vacina_covid as numeric) as string
    ) fonte_dados_vacina_covid,
    safe_cast(safe_cast(antiviral_gripe as numeric) as string) antiviral_gripe,
    safe_cast(safe_cast(tipo_antiviral as numeric) as string) tipo_antiviral,
    safe_cast(outro_tipo_antiviral as string) outro_tipo_antiviral,
    safe_cast(data_tratamento_antiviral as date) data_tratamento_antiviral,
    safe_cast(safe_cast(internacao as numeric) as string) internacao,
    safe_cast(data_internacao as date) data_internacao,
    safe_cast(sigla_uf_internacao as string) sigla_uf_internacao,
    safe_cast(
        safe_cast(id_regional_saude_internacao as numeric) as string
    ) id_regional_saude_internacao,
    safe_cast(
        safe_cast(id_municipio_internacao as numeric) as string
    ) id_municipio_internacao,
    safe_cast(
        safe_cast(id_municipio_6_internacao as numeric) as string
    ) id_municipio_6_internacao,
    safe_cast(safe_cast(internacao_uti as numeric) as string) internacao_uti,
    safe_cast(data_entrada_uti as date) data_entrada_uti,
    safe_cast(data_saida_uti as date) data_saida_uti,
    safe_cast(
        safe_cast(suporte_ventilatorio as numeric) as string
    ) suporte_ventilatorio,
    safe_cast(
        safe_cast(resultado_raiox_torax as numeric) as string
    ) resultado_raiox_torax,
    safe_cast(
        safe_cast(outro_resultado_raiox as numeric) as string
    ) outro_resultado_raiox,
    safe_cast(data_raiox as date) data_raiox,
    safe_cast(
        safe_cast(resultado_tomografia as numeric) as string
    ) resultado_tomografia,
    safe_cast(outro_resultado_tomografia as string) outro_resultado_tomografia,
    safe_cast(data_tomografia as date) data_tomografia,
    safe_cast(safe_cast(coleta_amostra as numeric) as string) coleta_amostra,
    safe_cast(data_coleta as date) data_coleta,
    safe_cast(safe_cast(tipo_amostra as numeric) as string) tipo_amostra,
    safe_cast(outro_tipo_amostra as string) outro_tipo_amostra,
    safe_cast(safe_cast(resultado_amostra as numeric) as string) resultado_amostra,
    safe_cast(data_coleta_hemaglutinacao as date) data_coleta_hemaglutinacao,
    safe_cast(
        safe_cast(resultado_hemaglutinacao as numeric) as string
    ) resultado_hemaglutinacao,
    safe_cast(
        safe_cast(tipo_resultado_hemaglutinacao as numeric) as string
    ) tipo_resultado_hemaglutinacao,
    safe_cast(
        safe_cast(hemaglutinacao_tipo_hemaglutinina as numeric) as string
    ) hemaglutinacao_tipo_hemaglutinina,
    safe_cast(
        safe_cast(hemaglutinacao_tipo_neuraminidase as numeric) as string
    ) hemaglutinacao_tipo_neuraminidase,
    safe_cast(safe_cast(tipo_pcr as numeric) as string) tipo_pcr,
    safe_cast(data_coleta_pcr as date) data_coleta_pcr,
    safe_cast(safe_cast(tipo_amostra_pcr as numeric) as string) tipo_amostra_pcr,
    safe_cast(qual_outro_tipo_amostra_pcr as string) qual_outro_tipo_amostra_pcr,
    safe_cast(data_resultado_pcr as string) data_resultado_pcr,
    safe_cast(safe_cast(resultado_pcr as numeric) as string) resultado_pcr,
    safe_cast(safe_cast(diagnostico_pcr as numeric) as string) diagnostico_pcr,
    safe_cast(safe_cast(id_laboratorio_pcr as numeric) as string) id_laboratorio_pcr,
    safe_cast(safe_cast(tipo_resultado_pcr as numeric) as string) tipo_resultado_pcr,
    safe_cast(
        safe_cast(pcr_tipo_hemaglutinina as numeric) as string
    ) pcr_tipo_hemaglutinina,
    safe_cast(
        safe_cast(pcr_tipo_neuraminidase as numeric) as string
    ) pcr_tipo_neuraminidase,
    safe_cast(
        safe_cast(pcr_positivo_influenza as numeric) as string
    ) pcr_positivo_influenza,
    safe_cast(safe_cast(tipo_influenza_pcr as numeric) as string) tipo_influenza_pcr,
    safe_cast(safe_cast(subtipo_influenza_a as numeric) as string) subtipo_influenza_a,
    safe_cast(outro_subtitpo_influenza as string) outro_subtitpo_influenza,
    safe_cast(safe_cast(linhagem_influeza_b as numeric) as string) linhagem_influeza_b,
    safe_cast(outra_linhagem_influenza_b as string) outra_linhagem_influenza_b,
    safe_cast(
        safe_cast(pcr_positivo_outro_virus as numeric) as string
    ) pcr_positivo_outro_virus,
    safe_cast(safe_cast(pcr_sarscov2 as numeric) as int64) pcr_sarscov2,
    safe_cast(
        safe_cast(pcr_virus_sincicial_respiratorio as numeric) as int64
    ) pcr_virus_sincicial_respiratorio,
    safe_cast(safe_cast(pcr_parainfluenza_1 as numeric) as string) pcr_parainfluenza_1,
    safe_cast(safe_cast(pcr_parainfluenza_2 as numeric) as string) pcr_parainfluenza_2,
    safe_cast(safe_cast(pcr_parainfluenza_3 as numeric) as string) pcr_parainfluenza_3,
    safe_cast(safe_cast(pcr_parainfluenza_4 as numeric) as string) pcr_parainfluenza_4,
    safe_cast(safe_cast(pcr_adenovirus as numeric) as int64) pcr_adenovirus,
    safe_cast(safe_cast(pcr_metapneumovirus as numeric) as int64) pcr_metapneumovirus,
    safe_cast(safe_cast(pcr_bocavirus as numeric) as int64) pcr_bocavirus,
    safe_cast(safe_cast(pcr_rinovirus as numeric) as int64) pcr_rinovirus,
    safe_cast(safe_cast(pcr_outro_virus as numeric) as string) pcr_outro_virus,
    safe_cast(qual_outro_virus as string) qual_outro_virus,
    safe_cast(
        safe_cast(diagnostico_imunofluorescencia as numeric) as string
    ) diagnostico_imunofluorescencia,
    safe_cast(
        safe_cast(resultado_imunofluorescencia as numeric) as string
    ) resultado_imunofluorescencia,
    safe_cast(
        data_resultado_imunofluorescencia as date
    ) data_resultado_imunofluorescencia,
    safe_cast(
        safe_cast(id_laboratorio_imunofluorescencia as numeric) as string
    ) id_laboratorio_imunofluorescencia,
    safe_cast(
        safe_cast(diagnostico_influenza_a as numeric) as string
    ) diagnostico_influenza_a,
    safe_cast(
        safe_cast(diagnostico_subtipo_influenza_a as numeric) as string
    ) diagnostico_subtipo_influenza_a,
    safe_cast(
        safe_cast(diagnostico_influenza_b as numeric) as string
    ) diagnostico_influenza_b,
    safe_cast(
        safe_cast(diagnostico_virus_sincicial_respiratorio as numeric) as string
    ) diagnostico_virus_sincicial_respiratorio,
    safe_cast(
        safe_cast(diagnostico_parainfluenza_1 as numeric) as string
    ) diagnostico_parainfluenza_1,
    safe_cast(
        safe_cast(diagnostico_parainfluenza_2 as numeric) as string
    ) diagnostico_parainfluenza_2,
    safe_cast(
        safe_cast(diagnostico_parainfluenza_3 as numeric) as string
    ) diagnostico_parainfluenza_3,
    safe_cast(
        safe_cast(diagnostico_adenovirus as numeric) as string
    ) diagnostico_adenovirus,
    safe_cast(
        safe_cast(diagnostico_outro_virus as numeric) as string
    ) diagnostico_outro_virus,
    safe_cast(
        safe_cast(diagnostico_outra_metodologia as numeric) as string
    ) diagnostico_outra_metodologia,
    safe_cast(
        safe_cast(imunofluorescencia_positivo_influenza as numeric) as string
    ) imunofluorescencia_positivo_influenza,
    safe_cast(
        safe_cast(tipo_influenza_imunofluorescencia as numeric) as string
    ) tipo_influenza_imunofluorescencia,
    safe_cast(
        safe_cast(imunofluorescencia_positivo_outro_virus as numeric) as string
    ) imunofluorescencia_positivo_outro_virus,
    safe_cast(
        safe_cast(imunofluorescencia_virus_sincicial_respiratorio as numeric) as string
    ) imunofluorescencia_virus_sincicial_respiratorio,
    safe_cast(
        safe_cast(imunofluorescencia_parainfluenza_1 as numeric) as string
    ) imunofluorescencia_parainfluenza_1,
    safe_cast(
        safe_cast(imunofluorescencia_parainfluenza_2 as numeric) as string
    ) imunofluorescencia_parainfluenza_2,
    safe_cast(
        safe_cast(imunofluorescencia_parainfluenza_3 as numeric) as string
    ) imunofluorescencia_parainfluenza_3,
    safe_cast(
        safe_cast(imunofluorescencia_adenovirus as numeric) as string
    ) imunofluorescencia_adenovirus,
    safe_cast(
        safe_cast(imunofluorescencia_outro_virus as numeric) as string
    ) imunofluorescencia_outro_virus,
    safe_cast(
        safe_cast(imunofluorescencia_qual_outro_virus as numeric) as string
    ) imunofluorescencia_qual_outro_virus,
    safe_cast(
        safe_cast(tipo_teste_antigenico as numeric) as string
    ) tipo_teste_antigenico,
    safe_cast(data_resultado_teste as date) data_resultado_teste,
    safe_cast(
        safe_cast(resultado_teste_antigenico as numeric) as string
    ) resultado_teste_antigenico,
    safe_cast(
        safe_cast(teste_positivo_influenza as numeric) as string
    ) teste_positivo_influenza,
    safe_cast(
        safe_cast(tipo_influenza_teste as numeric) as string
    ) tipo_influenza_teste,
    safe_cast(
        safe_cast(teste_positivo_outro_virus as numeric) as string
    ) teste_positivo_outro_virus,
    safe_cast(safe_cast(teste_sarscov2 as numeric) as string) teste_sarscov2,
    safe_cast(
        safe_cast(teste_virus_sincicial_respiratorio as numeric) as int64
    ) teste_virus_sincicial_respiratorio,
    safe_cast(
        safe_cast(teste_parainfluenza_1 as numeric) as string
    ) teste_parainfluenza_1,
    safe_cast(
        safe_cast(teste_parainfluenza_2 as numeric) as string
    ) teste_parainfluenza_2,
    safe_cast(
        safe_cast(teste_parainfluenza_3 as numeric) as string
    ) teste_parainfluenza_3,
    safe_cast(safe_cast(teste_adenovirus as numeric) as int64) teste_adenovirus,
    safe_cast(safe_cast(teste_outro_virus as numeric) as int64) teste_outro_virus,
    safe_cast(teste_outro_virus_nome as string) teste_outro_virus_nome,
    safe_cast(
        safe_cast(tipo_amostra_sorologica as numeric) as string
    ) tipo_amostra_sorologica,
    safe_cast(qual_outra_amostra as string) qual_outra_amostra,
    safe_cast(data_coleta_amostra as date) data_coleta_amostra,
    safe_cast(safe_cast(tipo_sorologia as numeric) as string) tipo_sorologia,
    safe_cast(qual_outra_sorologia as string) qual_outra_sorologia,
    safe_cast(data_resultado_sorologia as date) data_resultado_sorologia,
    safe_cast(
        safe_cast(resultado_sorologia_igg as numeric) as string
    ) resultado_sorologia_igg,
    safe_cast(
        safe_cast(resultado_sorologia_igm as numeric) as string
    ) resultado_sorologia_igm,
    safe_cast(
        safe_cast(resultado_sorologia_iga as numeric) as string
    ) resultado_sorologia_iga,
    safe_cast(safe_cast(historico_viagem as numeric) as string) historico_viagem,
    safe_cast(pais_viagem as string) pais_viagem,
    safe_cast(local_viagem as string) local_viagem,
    safe_cast(data_viagem as date) data_viagem_paciente,
    safe_cast(data_retorno as date) data_retorno_paciente,
    safe_cast(
        safe_cast(status_monitoramento as numeric) as string
    ) status_monitoramento,
    safe_cast(safe_cast(classificacao_final as numeric) as string) classificacao_final,
    safe_cast(outro_agente_etiologico as string) outro_agente_etiologico,
    safe_cast(
        safe_cast(criterio_encerramento as numeric) as string
    ) criterio_encerramento,
    safe_cast(safe_cast(evolucao_caso as numeric) as string) evolucao_caso,
    safe_cast(data_alta_obito as date) data_alta_obito,
    safe_cast(data_encerramento as date) data_encerramento,
    safe_cast(data_digitacao as date) data_digitacao,
    safe_cast(safe_cast(tipo_ficha as numeric) as string) tipo_ficha,
    safe_cast(requisicao_sistema_gal as string) requisicao_sistema_gal,
    safe_cast(safe_cast(controle_srag_sinan as numeric) as string) controle_srag_sinan
from {{ set_datalake_project("br_ms_sinan_staging.microdados_influenza_srag") }} as t
