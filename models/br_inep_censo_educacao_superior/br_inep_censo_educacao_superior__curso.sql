{{
    config(
        alias="curso",
        schema="br_inep_censo_educacao_superior",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2009, "end": 2024, "interval": 1},
        },
        cluster_by="sigla_uf",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(tipo_dimensao as string) tipo_dimensao,
    safe_cast(tipo_organizacao_academica as string) tipo_organizacao_academica,
    safe_cast(
        tipo_organizacao_administrativa as string
    ) tipo_organizacao_administrativa,
    safe_cast(rede as string) rede,
    safe_cast(id_ies as string) id_ies,
    safe_cast(nome_curso as string) nome_curso,
    safe_cast(id_curso as string) id_curso,
    safe_cast(nome_curso_cine as string) nome_curso_cine,
    safe_cast(id_curso_cine as string) id_curso_cine,
    safe_cast(id_area_geral as string) id_area_geral,
    safe_cast(nome_area_geral as string) nome_area_geral,
    safe_cast(id_area_especifica as string) id_area_especifica,
    safe_cast(nome_area_especifica as string) nome_area_especifica,
    safe_cast(id_area_detalhada as string) id_area_detalhada,
    safe_cast(nome_area_detalhada as string) nome_area_detalhada,
    safe_cast(tipo_grau_academico as string) tipo_grau_academico,
    safe_cast(indicador_gratuito as boolean) indicador_gratuito,
    safe_cast(tipo_modalidade_ensino as string) tipo_modalidade_ensino,
    safe_cast(tipo_nivel_academico as string) tipo_nivel_academico,
    safe_cast(quantidade_vagas as int64) quantidade_vagas,
    safe_cast(quantidade_vagas_diurno as int64) quantidade_vagas_diurno,
    safe_cast(quantidade_vagas_noturno as int64) quantidade_vagas_noturno,
    safe_cast(quantidade_vagas_ead as int64) quantidade_vagas_ead,
    safe_cast(quantidade_vagas_novas as int64) quantidade_vagas_novas,
    safe_cast(
        quantidade_vagas_processos_seletivos as int64
    ) quantidade_vagas_processos_seletivos,
    safe_cast(quantidade_vagas_remanescentes as int64) quantidade_vagas_remanescentes,
    safe_cast(
        quantidade_vagas_programas_especiais as int64
    ) quantidade_vagas_programas_especiais,
    safe_cast(quantidade_inscritos as int64) quantidade_inscritos,
    safe_cast(quantidade_inscritos_diurno as int64) quantidade_inscritos_diurno,
    safe_cast(quantidade_inscritos_noturno as int64) quantidade_inscritos_noturno,
    safe_cast(quantidade_inscritos_ead as int64) quantidade_inscritos_ead,
    safe_cast(
        quantidade_inscritos_vagas_novas as int64
    ) quantidade_inscritos_vagas_novas,
    safe_cast(
        quantidade_inscritos_processos_seletivos as int64
    ) quantidade_inscritos_processos_seletivos,
    safe_cast(
        quantidade_inscritos_remanescentes as int64
    ) quantidade_inscritos_remanescentes,
    safe_cast(
        quantidade_inscritos_programas_especiais as int64
    ) quantidade_inscritos_programas_especiais,
    safe_cast(quantidade_ingressantes as int64) quantidade_ingressantes,
    safe_cast(
        quantidade_ingressantes_feminino as int64
    ) quantidade_ingressantes_feminino,
    safe_cast(
        quantidade_ingressantes_masculino as int64
    ) quantidade_ingressantes_masculino,
    safe_cast(quantidade_ingressantes_diurno as int64) quantidade_ingressantes_diurno,
    safe_cast(quantidade_ingressantes_noturno as int64) quantidade_ingressantes_noturno,
    safe_cast(
        quantidade_ingressantes_vagas_novas as int64
    ) quantidade_ingressantes_vagas_novas,
    safe_cast(
        quantidade_ingressantes_vestibular as int64
    ) quantidade_ingressantes_vestibular,
    safe_cast(quantidade_ingressantes_enem as int64) quantidade_ingressantes_enem,
    safe_cast(
        quantidade_ingressantes_avaliacao_seriada as int64
    ) quantidade_ingressantes_avaliacao_seriada,
    safe_cast(
        quantidade_ingressantes_selecao_simplificada as int64
    ) quantidade_ingressantes_selecao_simplificada,
    safe_cast(
        quantidade_ingressantes_egressos as int64
    ) quantidade_ingressantes_egressos,
    safe_cast(
        quantidade_ingressantes_outro_tipo_selecao as int64
    ) quantidade_ingressantes_outro_tipo_selecao,
    safe_cast(
        quantidade_ingressantes_processos_seletivos as int64
    ) quantidade_ingressantes_processos_seletivos,
    safe_cast(
        quantidade_ingressantes_remanescentes as int64
    ) quantidade_ingressantes_remanescentes,
    safe_cast(
        quantidade_ingressantes_programas_especiais as int64
    ) quantidade_ingressantes_programas_especiais,
    safe_cast(
        quantidade_ingressantes_outras_formas as int64
    ) quantidade_ingressantes_outras_formas,
    safe_cast(quantidade_ingressantes_0_17 as int64) quantidade_ingressantes_0_17,
    safe_cast(quantidade_ingressantes_18_24 as int64) quantidade_ingressantes_18_24,
    safe_cast(quantidade_ingressantes_25_29 as int64) quantidade_ingressantes_25_29,
    safe_cast(quantidade_ingressantes_30_34 as int64) quantidade_ingressantes_30_34,
    safe_cast(quantidade_ingressantes_35_39 as int64) quantidade_ingressantes_35_39,
    safe_cast(quantidade_ingressantes_40_49 as int64) quantidade_ingressantes_40_49,
    safe_cast(quantidade_ingressantes_50_59 as int64) quantidade_ingressantes_50_59,
    safe_cast(quantidade_ingressantes_60_mais as int64) quantidade_ingressantes_60_mais,
    safe_cast(quantidade_ingressantes_branca as int64) quantidade_ingressantes_branca,
    safe_cast(quantidade_ingressantes_preta as int64) quantidade_ingressantes_preta,
    safe_cast(quantidade_ingressantes_parda as int64) quantidade_ingressantes_parda,
    safe_cast(quantidade_ingressantes_amarela as int64) quantidade_ingressantes_amarela,
    safe_cast(
        quantidade_ingressantes_indigena as int64
    ) quantidade_ingressantes_indigena,
    safe_cast(
        quantidade_ingressantes_cor_nao_declarada as int64
    ) quantidade_ingressantes_cor_nao_declarada,
    safe_cast(quantidade_matriculas as int64) quantidade_matriculas,
    safe_cast(quantidade_matriculas_feminino as int64) quantidade_matriculas_feminino,
    safe_cast(quantidade_matriculas_masculino as int64) quantidade_matriculas_masculino,
    safe_cast(quantidade_matriculas_diurno as int64) quantidade_matriculas_diurno,
    safe_cast(quantidade_matriculas_noturno as int64) quantidade_matriculas_noturno,
    safe_cast(quantidade_matriculas_0_17 as int64) quantidade_matriculas_0_17,
    safe_cast(quantidade_matriculas_18_24 as int64) quantidade_matriculas_18_24,
    safe_cast(quantidade_matriculas_25_29 as int64) quantidade_matriculas_25_29,
    safe_cast(quantidade_matriculas_30_34 as int64) quantidade_matriculas_30_34,
    safe_cast(quantidade_matriculas_35_39 as int64) quantidade_matriculas_35_39,
    safe_cast(quantidade_matriculas_40_49 as int64) quantidade_matriculas_40_49,
    safe_cast(quantidade_matriculas_50_59 as int64) quantidade_matriculas_50_59,
    safe_cast(quantidade_matriculas_60_mais as int64) quantidade_matriculas_60_mais,
    safe_cast(quantidade_matriculas_branca as int64) quantidade_matriculas_branca,
    safe_cast(quantidade_matriculas_preta as int64) quantidade_matriculas_preta,
    safe_cast(quantidade_matriculas_parda as int64) quantidade_matriculas_parda,
    safe_cast(quantidade_matriculas_amarela as int64) quantidade_matriculas_amarela,
    safe_cast(quantidade_matriculas_indigena as int64) quantidade_matriculas_indigena,
    safe_cast(
        quantidade_matriculas_cor_nao_declarada as int64
    ) quantidade_matriculas_cor_nao_declarada,
    safe_cast(quantidade_concluintes as int64) quantidade_concluintes,
    safe_cast(quantidade_concluintes_feminino as int64) quantidade_concluintes_feminino,
    safe_cast(
        quantidade_concluintes_masculino as int64
    ) quantidade_concluintes_masculino,
    safe_cast(quantidade_concluintes_diurno as int64) quantidade_concluintes_diurno,
    safe_cast(quantidade_concluintes_noturno as int64) quantidade_concluintes_noturno,
    safe_cast(quantidade_concluintes_0_17 as int64) quantidade_concluintes_0_17,
    safe_cast(quantidade_concluintes_18_24 as int64) quantidade_concluintes_18_24,
    safe_cast(quantidade_concluintes_25_29 as int64) quantidade_concluintes_25_29,
    safe_cast(quantidade_concluintes_30_34 as int64) quantidade_concluintes_30_34,
    safe_cast(quantidade_concluintes_35_39 as int64) quantidade_concluintes_35_39,
    safe_cast(quantidade_concluintes_40_49 as int64) quantidade_concluintes_40_49,
    safe_cast(quantidade_concluintes_50_59 as int64) quantidade_concluintes_50_59,
    safe_cast(quantidade_concluintes_60_mais as int64) quantidade_concluintes_60_mais,
    safe_cast(quantidade_concluintes_branca as int64) quantidade_concluintes_branca,
    safe_cast(quantidade_concluintes_preta as int64) quantidade_concluintes_preta,
    safe_cast(quantidade_concluintes_parda as int64) quantidade_concluintes_parda,
    safe_cast(quantidade_concluintes_amarela as int64) quantidade_concluintes_amarela,
    safe_cast(quantidade_concluintes_indigena as int64) quantidade_concluintes_indigena,
    safe_cast(
        quantidade_concluintes_cor_nao_declarada as int64
    ) quantidade_concluintes_cor_nao_declarada,
    safe_cast(
        quantidade_ingressantes_brasileiro as int64
    ) quantidade_ingressantes_brasileiro,
    safe_cast(
        quantidade_ingressantes_estrangeiro as int64
    ) quantidade_ingressantes_estrangeiro,
    safe_cast(
        quantidade_matriculas_brasileiro as int64
    ) quantidade_matriculas_brasileiro,
    safe_cast(
        quantidade_matriculas_estrangeiro as int64
    ) quantidade_matriculas_estrangeiro,
    safe_cast(
        quantidade_concluintes_brasileiro as int64
    ) quantidade_concluintes_brasileiro,
    safe_cast(
        quantidade_concluintes_estrangeiro as int64
    ) quantidade_concluintes_estrangeiro,
    safe_cast(quantidade_alunos_deficiencia as int64) quantidade_alunos_deficiencia,
    safe_cast(
        quantidade_ingressantes_deficiencia as int64
    ) quantidade_ingressantes_deficiencia,
    safe_cast(
        quantidade_matriculas_deficiencia as int64
    ) quantidade_matriculas_deficiencia,
    safe_cast(
        quantidade_concluintes_deficiencia as int64
    ) quantidade_concluintes_deficiencia,
    safe_cast(
        quantidade_ingressantes_financiamento as int64
    ) quantidade_ingressantes_financiamento,
    safe_cast(
        quantidade_ingressantes_financiamento_reembolsavel as int64
    ) quantidade_ingressantes_financiamento_reembolsavel,
    safe_cast(
        quantidade_ingressantes_financiamento_reembolsavel_fies as int64
    ) quantidade_ingressantes_financiamento_reembolsavel_fies,
    safe_cast(
        quantidade_ingressantes_financiamento_reembolsavel_instituicao as int64
    ) quantidade_ingressantes_financiamento_reembolsavel_instituicao,
    safe_cast(
        quantidade_ingressantes_financiamento_reembolsavel_outros as int64
    ) quantidade_ingressantes_financiamento_reembolsavel_outros,
    safe_cast(
        quantidade_ingressantes_financiamento_nao_reembolsavel as int64
    ) quantidade_ingressantes_financiamento_nao_reembolsavel,
    safe_cast(
        quantidade_ingressantes_financiamento_nao_reembolsavel_prouni_integral as int64
    ) quantidade_ingressantes_financiamento_nao_reembolsavel_prouni_integral,
    safe_cast(
        quantidade_ingressantes_financiamento_nao_reembolsavel_prouni_parcial as int64
    ) quantidade_ingressantes_financiamento_nao_reembolsavel_prouni_parcial,
    safe_cast(
        quantidade_ingressantes_financiamento_nao_reembolsavel_instituicao as int64
    ) quantidade_ingressantes_financiamento_nao_reembolsavel_instituicao,
    safe_cast(
        quantidade_ingressantes_financiamento_nao_reembolsavel_outros as int64
    ) quantidade_ingressantes_financiamento_nao_reembolsavel_outros,
    safe_cast(
        quantidade_matriculas_financiamento as int64
    ) quantidade_matriculas_financiamento,
    safe_cast(
        quantidade_matriculas_financiamento_reembolsavel as int64
    ) quantidade_matriculas_financiamento_reembolsavel,
    safe_cast(
        quantidade_matriculas_financiamento_reembolsavel_fies as int64
    ) quantidade_matriculas_financiamento_reembolsavel_fies,
    safe_cast(
        quantidade_matriculas_financiamento_reembolsavel_instituicao as int64
    ) quantidade_matriculas_financiamento_reembolsavel_instituicao,
    safe_cast(
        quantidade_matriculas_financiamento_reembolsavel_outros as int64
    ) quantidade_matriculas_financiamento_reembolsavel_outros,
    safe_cast(
        quantidade_matriculas_financiamento_nao_reembolsavel as int64
    ) quantidade_matriculas_financiamento_nao_reembolsavel,
    safe_cast(
        quantidade_matriculas_financiamento_nao_reembolsavel_prouni_integral as int64
    ) quantidade_matriculas_financiamento_nao_reembolsavel_prouni_integral,
    safe_cast(
        quantidade_matriculas_financiamento_nao_reembolsavel_prouni_parcial as int64
    ) quantidade_matriculas_financiamento_nao_reembolsavel_prouni_parcial,
    safe_cast(
        quantidade_matriculas_financiamento_nao_reembolsavel_instituicao as int64
    ) quantidade_matriculas_financiamento_nao_reembolsavel_instituicao,
    safe_cast(
        quantidade_matriculas_financiamento_nao_reembolsavel_outros as int64
    ) quantidade_matriculas_financiamento_nao_reembolsavel_outros,
    safe_cast(
        quantidade_concluintes_financiamento as int64
    ) quantidade_concluintes_financiamento,
    safe_cast(
        quantidade_concluintes_financiamento_reembolsavel as int64
    ) quantidade_concluintes_financiamento_reembolsavel,
    safe_cast(
        quantidade_concluintes_financiamento_reembolsavel_fies as int64
    ) quantidade_concluintes_financiamento_reembolsavel_fies,
    safe_cast(
        quantidade_concluintes_financiamento_reembolsavel_instituicao as int64
    ) quantidade_concluintes_financiamento_reembolsavel_instituicao,
    safe_cast(
        quantidade_concluintes_financiamento_reembolsavel_outros as int64
    ) quantidade_concluintes_financiamento_reembolsavel_outros,
    safe_cast(
        quantidade_concluintes_financiamento_nao_reembolsavel as int64
    ) quantidade_concluintes_financiamento_nao_reembolsavel,
    safe_cast(
        quantidade_concluintes_financiamento_nao_reembolsavel_prouni_integral as int64
    ) quantidade_concluintes_financiamento_nao_reembolsavel_prouni_integral,
    safe_cast(
        quantidade_concluintes_financiamento_nao_reembolsavel_prouni_parcial as int64
    ) quantidade_concluintes_financiamento_nao_reembolsavel_prouni_parcial,
    safe_cast(
        quantidade_concluintes_financiamento_nao_reembolsavel_instituicao as int64
    ) quantidade_concluintes_financiamento_nao_reembolsavel_instituicao,
    safe_cast(
        quantidade_concluintes_financiamento_nao_reembolsavel_outros as int64
    ) quantidade_concluintes_financiamento_nao_reembolsavel_outros,
    safe_cast(
        quantidade_ingressantes_reserva_vaga as int64
    ) quantidade_ingressantes_reserva_vaga,
    safe_cast(
        quantidade_ingressantes_reserva_vaga_rede_publica as int64
    ) quantidade_ingressantes_reserva_vaga_rede_publica,
    safe_cast(
        quantidade_ingressantes_reserva_vaga_etnico as int64
    ) quantidade_ingressantes_reserva_vaga_etnico,
    safe_cast(
        quantidade_ingressantes_reserva_vaga_deficiencia as int64
    ) quantidade_ingressantes_reserva_vaga_deficiencia,
    safe_cast(
        quantidade_ingressantes_reserva_vaga_social_renda_familiar as int64
    ) quantidade_ingressantes_reserva_vaga_social_renda_familiar,
    safe_cast(
        quantidade_ingressantes_reserva_vaga_outros as int64
    ) quantidade_ingressantes_reserva_vaga_outros,
    safe_cast(
        quantidade_matriculas_reserva_vaga as int64
    ) quantidade_matriculas_reserva_vaga,
    safe_cast(
        quantidade_matriculas_reserva_vaga_rede_publica as int64
    ) quantidade_matriculas_reserva_vaga_rede_publica,
    safe_cast(
        quantidade_matriculas_reserva_vaga_etnico as int64
    ) quantidade_matriculas_reserva_vaga_etnico,
    safe_cast(
        quantidade_matriculas_reserva_vaga_deficiencia as int64
    ) quantidade_matriculas_reserva_vaga_deficiencia,
    safe_cast(
        quantidade_matriculas_reserva_vaga_social_renda_familiar as int64
    ) quantidade_matriculas_reserva_vaga_social_renda_familiar,
    safe_cast(
        quantidade_matriculas_reserva_vaga_outros as int64
    ) quantidade_matriculas_reserva_vaga_outros,
    safe_cast(
        quantidade_concluintes_reserva_vaga as int64
    ) quantidade_concluintes_reserva_vaga,
    safe_cast(
        quantidade_concluintes_reserva_vaga_rede_publica as int64
    ) quantidade_concluintes_reserva_vaga_rede_publica,
    safe_cast(
        quantidade_concluintes_reserva_vaga_etnico as int64
    ) quantidade_concluintes_reserva_vaga_etnico,
    safe_cast(
        quantidade_concluintes_reserva_vaga_deficiencia as int64
    ) quantidade_concluintes_reserva_vaga_deficiencia,
    safe_cast(
        quantidade_concluintes_reserva_vaga_social_renda_familiar as int64
    ) quantidade_concluintes_reserva_vaga_social_renda_familiar,
    safe_cast(
        quantidade_concluintes_reserva_vaga_outros as int64
    ) quantidade_concluintes_reserva_vaga_outros,
    safe_cast(
        quantidade_alunos_situacao_trancada as int64
    ) quantidade_alunos_situacao_trancada,
    safe_cast(
        quantidade_alunos_situacao_desvinculada as int64
    ) quantidade_alunos_situacao_desvinculada,
    safe_cast(
        quantidade_alunos_situacao_transferida as int64
    ) quantidade_alunos_situacao_transferida,
    safe_cast(
        quantidade_alunos_situacao_falecidos as int64
    ) quantidade_alunos_situacao_falecidos,
    safe_cast(
        quantidade_ingressantes_em_rede_publica as int64
    ) quantidade_ingressantes_em_rede_publica,
    safe_cast(
        quantidade_ingressantes_em_rede_privada as int64
    ) quantidade_ingressantes_em_rede_privada,
    safe_cast(
        quantidade_ingressantes_em_rede_nao_informada as int64
    ) quantidade_ingressantes_em_rede_nao_informada,
    safe_cast(
        quantidade_matriculas_em_rede_publica as int64
    ) quantidade_matriculas_em_rede_publica,
    safe_cast(
        quantidade_matriculas_em_rede_privada as int64
    ) quantidade_matriculas_em_rede_privada,
    safe_cast(
        quantidade_matriculas_em_rede_nao_informada as int64
    ) quantidade_matriculas_em_rede_nao_informada,
    safe_cast(
        quantidade_concluintes_em_rede_publica as int64
    ) quantidade_concluintes_em_rede_publica,
    safe_cast(
        quantidade_concluintes_em_rede_privada as int64
    ) quantidade_concluintes_em_rede_privada,
    safe_cast(
        quantidade_concluintes_em_rede_nao_informada as int64
    ) quantidade_concluintes_em_rede_nao_informada,
    safe_cast(quantidade_alunos_parfor as int64) quantidade_alunos_parfor,
    safe_cast(quantidade_ingressantes_parfor as int64) quantidade_ingressantes_parfor,
    safe_cast(quantidade_matriculas_parfor as int64) quantidade_matriculas_parfor,
    safe_cast(quantidade_concluintes_parfor as int64) quantidade_concluintes_parfor,
    safe_cast(quantidade_alunos_apoio_social as int64) quantidade_alunos_apoio_social,
    safe_cast(
        quantidade_ingressantes_apoio_social as int64
    ) quantidade_ingressantes_apoio_social,
    safe_cast(
        quantidade_matriculas_apoio_social as int64
    ) quantidade_matriculas_apoio_social,
    safe_cast(
        quantidade_concluintes_apoio_social as int64
    ) quantidade_concluintes_apoio_social,
    safe_cast(
        quantidade_alunos_atividade_extracurricular as int64
    ) quantidade_alunos_atividade_extracurricular,
    safe_cast(
        quantidade_ingressantes_atividade_extracurricular as int64
    ) quantidade_ingressantes_atividade_extracurricular,
    safe_cast(
        quantidade_matriculas_atividade_extracurricular as int64
    ) quantidade_matriculas_atividade_extracurricular,
    safe_cast(
        quantidade_concluintes_atividade_extracurricular as int64
    ) quantidade_concluintes_atividade_extracurricular,
    safe_cast(
        quantidade_alunos_mobilidade_academica as int64
    ) quantidade_alunos_mobilidade_academica,
    safe_cast(
        quantidade_ingressantes_mobilidade_academica as int64
    ) quantidade_ingressantes_mobilidade_academica,
    safe_cast(
        quantidade_matriculas_mobilidade_academica as int64
    ) quantidade_matriculas_mobilidade_academica,
    safe_cast(
        quantidade_concluintes_mobilidade_academica as int64
    ) quantidade_concluintes_mobilidade_academica,
from {{ set_datalake_project("br_inep_censo_educacao_superior_staging.curso") }} as t
