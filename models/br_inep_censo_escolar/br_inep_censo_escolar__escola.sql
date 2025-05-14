{{
    config(
        alias="escola",
        schema="br_inep_censo_escolar",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2007, "end": 2023, "interval": 1},
        },
        cluster_by="sigla_uf",
    )
}}
with
    censo as (
        select
            safe_cast(ano as int64) ano,
            safe_cast(sigla_uf as string) sigla_uf,
            safe_cast(id_municipio as string) id_municipio,
            safe_cast(id_escola as string) id_escola,
            safe_cast(rede as string) rede,
            safe_cast(
                tipo_categoria_escola_privada as string
            ) tipo_categoria_escola_privada,
            safe_cast(tipo_localizacao as string) tipo_localizacao,
            safe_cast(
                tipo_localizacao_diferenciada as string
            ) tipo_localizacao_diferenciada,
            safe_cast(
                tipo_situacao_funcionamento as string
            ) tipo_situacao_funcionamento,
            safe_cast(id_orgao_regional as string) id_orgao_regional,
            safe_cast(data_ano_letivo_inicio as date) data_ano_letivo_inicio,
            safe_cast(data_ano_letivo_termino as date) data_ano_letivo_termino,
            safe_cast(vinculo_secretaria_educacao as int64) vinculo_secretaria_educacao,
            safe_cast(vinculo_seguranca_publica as int64) vinculo_seguranca_publica,
            safe_cast(vinculo_secretaria_saude as int64) vinculo_secretaria_saude,
            safe_cast(vinculo_outro_orgao as int64) vinculo_outro_orgao,
            safe_cast(poder_publico_parceria as int64) poder_publico_parceria,
            safe_cast(
                tipo_poder_publico_parceria as string
            ) tipo_poder_publico_parceria,
            safe_cast(conveniada_poder_publico as int64) conveniada_poder_publico,
            safe_cast(
                tipo_convenio_poder_publico as string
            ) tipo_convenio_poder_publico,
            safe_cast(
                forma_contratacao_termo_colaboracao as int64
            ) forma_contratacao_termo_colaboracao,
            safe_cast(
                forma_contratacao_termo_fomento as int64
            ) forma_contratacao_termo_fomento,
            safe_cast(
                forma_contratacao_acordo_cooperacao as int64
            ) forma_contratacao_acordo_cooperacao,
            safe_cast(
                forma_contratacao_prestacao_servico as int64
            ) forma_contratacao_prestacao_servico,
            safe_cast(
                forma_contratacao_cooperacao_tecnica_financeira as int64
            ) forma_contratacao_cooperacao_tecnica_financeira,
            safe_cast(
                forma_contratacao_consorcio_publico as int64
            ) forma_contratacao_consorcio_publico,
            null as forma_contratacao_parceria_municipal_termo_colaboracao,
            null as forma_contratacao_parceria_municipal_termo_fomento,
            null as forma_contratacao_parceria_municipal_acordo_cooperacao,
            null as forma_contratacao_parceria_municipal_prestacao_servico,
            null as forma_contratacao_parceria_municipal_cooperacao_tecnica_financeira,
            null as forma_contratacao_parceria_municipal_consorcio_publico,
            null as forma_contratacao_parceria_estadual_termo_colaboracao,
            null as forma_contratacao_parceria_estadual_termo_fomento,
            null as forma_contratacao_parceria_estadual_acordo_cooperacao,
            null as forma_contratacao_parceria_estadual_prestacao_servico,
            null as forma_contratacao_parceria_estadual_cooperacao_tecnica_financeira,
            null as forma_contratacao_parceria_estadual_consorcio_publico,
            safe_cast(
                tipo_atendimento_escolarizacao as int64
            ) tipo_atendimento_escolarizacao,
            safe_cast(
                tipo_atendimento_atividade_complementar as int64
            ) tipo_atendimento_atividade_complementar,
            safe_cast(tipo_atendimento_aee as int64) tipo_atendimento_aee,
            safe_cast(
                mantenedora_escola_privada_empresa as int64
            ) mantenedora_escola_privada_empresa,
            safe_cast(
                mantenedora_escola_privada_ong as int64
            ) mantenedora_escola_privada_ong,
            safe_cast(
                mantenedora_escola_privada_oscip as int64
            ) mantenedora_escola_privada_oscip,
            safe_cast(
                mantenedora_escola_privada_ong_oscip as int64
            ) mantenedora_escola_privada_ong_oscip,
            safe_cast(
                mantenedora_escola_privada_sindicato as int64
            ) mantenedora_escola_privada_sindicato,
            safe_cast(
                mantenedora_escola_privada_sistema_s as int64
            ) mantenedora_escola_privada_sistema_s,
            safe_cast(
                mantenedora_escola_privada_sem_fins as int64
            ) mantenedora_escola_privada_sem_fins,
            safe_cast(cnpj_escola_privada as string) cnpj_escola_privada,
            safe_cast(cnpj_mantenedora as string) cnpj_mantenedora,
            safe_cast(tipo_regulamentacao as string) tipo_regulamentacao,
            safe_cast(
                tipo_responsavel_regulamentacao as string
            ) tipo_responsavel_regulamentacao,
            safe_cast(id_escola_sede as string) id_escola_sede,
            safe_cast(id_ies_ofertante as string) id_ies_ofertante,
            safe_cast(
                local_funcionamento_predio_escolar as int64
            ) local_funcionamento_predio_escolar,
            safe_cast(
                tipo_local_funcionamento_predio_escolar as string
            ) tipo_local_funcionamento_predio_escolar,
            safe_cast(
                local_funcionamento_sala_empresa as int64
            ) local_funcionamento_sala_empresa,
            safe_cast(
                local_funcionamento_socioeducativo as int64
            ) local_funcionamento_socioeducativo,
            safe_cast(
                local_funcionamento_unidade_prisional as int64
            ) local_funcionamento_unidade_prisional,
            safe_cast(
                local_funcionamento_prisional_socio as int64
            ) local_funcionamento_prisional_socio,
            safe_cast(
                local_funcionamento_templo_igreja as int64
            ) local_funcionamento_templo_igreja,
            safe_cast(
                local_funcionamento_casa_professor as int64
            ) local_funcionamento_casa_professor,
            safe_cast(local_funcionamento_galpao as int64) local_funcionamento_galpao,
            safe_cast(
                tipo_local_funcionamento_galpao as string
            ) tipo_local_funcionamento_galpao,
            safe_cast(
                local_funcionamento_outra_escola as int64
            ) local_funcionamento_outra_escola,
            safe_cast(local_funcionamento_outros as int64) local_funcionamento_outros,
            safe_cast(predio_compartilhado as int64) predio_compartilhado,
            safe_cast(agua_filtrada as int64) agua_filtrada,
            safe_cast(agua_potavel as int64) agua_potavel,
            safe_cast(agua_rede_publica as int64) agua_rede_publica,
            safe_cast(agua_poco_artesiano as int64) agua_poco_artesiano,
            safe_cast(agua_cacimba as int64) agua_cacimba,
            safe_cast(agua_fonte_rio as int64) agua_fonte_rio,
            safe_cast(agua_inexistente as int64) agua_inexistente,
            safe_cast(energia_rede_publica as int64) energia_rede_publica,
            safe_cast(energia_gerador as int64) energia_gerador,
            safe_cast(energia_gerador_fossil as int64) energia_gerador_fossil,
            safe_cast(energia_outros as int64) energia_outros,
            safe_cast(energia_renovavel as int64) energia_renovavel,
            safe_cast(energia_inexistente as int64) energia_inexistente,
            safe_cast(esgoto_rede_publica as int64) esgoto_rede_publica,
            safe_cast(esgoto_fossa as int64) esgoto_fossa,
            safe_cast(esgoto_fossa_septica as int64) esgoto_fossa_septica,
            safe_cast(esgoto_fossa_comum as int64) esgoto_fossa_comum,
            safe_cast(esgoto_inexistente as int64) esgoto_inexistente,
            safe_cast(lixo_servico_coleta as int64) lixo_servico_coleta,
            safe_cast(lixo_queima as int64) lixo_queima,
            safe_cast(lixo_enterrado as int64) lixo_enterrado,
            safe_cast(lixo_destino_final_publico as int64) lixo_destino_final_publico,
            safe_cast(lixo_descarta_outra_area as int64) lixo_descarta_outra_area,
            safe_cast(lixo_joga_outra_area as int64) lixo_joga_outra_area,
            safe_cast(lixo_outros as int64) lixo_outros,
            safe_cast(lixo_reciclagem as int64) lixo_reciclagem,
            safe_cast(tratamento_lixo_separacao as int64) tratamento_lixo_separacao,
            safe_cast(
                tratamento_lixo_reutilizacao as int64
            ) tratamento_lixo_reutilizacao,
            safe_cast(tratamento_lixo_reciclagem as int64) tratamento_lixo_reciclagem,
            safe_cast(tratamento_lixo_inexistente as int64) tratamento_lixo_inexistente,
            safe_cast(almoxarifado as int64) almoxarifado,
            safe_cast(area_verde as int64) area_verde,
            safe_cast(auditorio as int64) auditorio,
            safe_cast(banheiro_fora_predio as int64) banheiro_fora_predio,
            safe_cast(banheiro_dentro_predio as int64) banheiro_dentro_predio,
            safe_cast(banheiro as int64) banheiro,
            safe_cast(banheiro_educacao_infantil as int64) banheiro_educacao_infantil,
            safe_cast(banheiro_pne as int64) banheiro_pne,
            safe_cast(banheiro_funcionarios as int64) banheiro_funcionarios,
            safe_cast(banheiro_chuveiro as int64) banheiro_chuveiro,
            safe_cast(bercario as int64) bercario,
            safe_cast(biblioteca as int64) biblioteca,
            safe_cast(biblioteca_sala_leitura as int64) biblioteca_sala_leitura,
            safe_cast(cozinha as int64) cozinha,
            safe_cast(despensa as int64) despensa,
            safe_cast(dormitorio_aluno as int64) dormitorio_aluno,
            safe_cast(dormitorio_professor as int64) dormitorio_professor,
            safe_cast(laboratorio_ciencias as int64) laboratorio_ciencias,
            safe_cast(laboratorio_informatica as int64) laboratorio_informatica,
            safe_cast(
                laboratorio_educacao_profissional as int64
            ) laboratorio_educacao_profissional,
            safe_cast(patio_coberto as int64) patio_coberto,
            safe_cast(patio_descoberto as int64) patio_descoberto,
            safe_cast(parque_infantil as int64) parque_infantil,
            safe_cast(piscina as int64) piscina,
            safe_cast(quadra_esportes as int64) quadra_esportes,
            safe_cast(quadra_esportes_coberta as int64) quadra_esportes_coberta,
            safe_cast(quadra_esportes_descoberta as int64) quadra_esportes_descoberta,
            safe_cast(refeitorio as int64) refeitorio,
            safe_cast(sala_atelie_artes as int64) sala_atelie_artes,
            safe_cast(sala_musica_coral as int64) sala_musica_coral,
            safe_cast(sala_estudio_danca as int64) sala_estudio_danca,
            safe_cast(sala_multiuso as int64) sala_multiuso,
            null as sala_estudio_gravacao,
            safe_cast(
                sala_oficinas_educacao_profissional as int64
            ) sala_oficinas_educacao_profissional,
            safe_cast(sala_diretoria as int64) sala_diretoria,
            safe_cast(sala_leitura as int64) sala_leitura,
            safe_cast(sala_professor as int64) sala_professor,
            safe_cast(sala_repouso_aluno as int64) sala_repouso_aluno,
            safe_cast(secretaria as int64) secretaria,
            safe_cast(sala_atendimento_especial as int64) sala_atendimento_especial,
            safe_cast(terreirao as int64) terreirao,
            safe_cast(viveiro as int64) viveiro,
            safe_cast(dependencia_pne as int64) dependencia_pne,
            safe_cast(lavanderia as int64) lavanderia,
            safe_cast(dependencia_outras as int64) dependencia_outras,
            safe_cast(acessibilidade_corrimao as int64) acessibilidade_corrimao,
            safe_cast(acessibilidade_elevador as int64) acessibilidade_elevador,
            safe_cast(acessibilidade_pisos_tateis as int64) acessibilidade_pisos_tateis,
            safe_cast(acessibilidade_vao_livre as int64) acessibilidade_vao_livre,
            safe_cast(acessibilidade_rampas as int64) acessibilidade_rampas,
            safe_cast(acessibilidade_sinal_sonoro as int64) acessibilidade_sinal_sonoro,
            safe_cast(acessibilidade_sinal_tatil as int64) acessibilidade_sinal_tatil,
            safe_cast(acessibilidade_sinal_visual as int64) acessibilidade_sinal_visual,
            safe_cast(acessibilidade_inexistente as int64) acessibilidade_inexistente,
            safe_cast(quantidade_sala_existente as int64) quantidade_sala_existente,
            safe_cast(quantidade_sala_utilizada as int64) quantidade_sala_utilizada,
            safe_cast(
                quantidade_sala_utilizada_dentro as int64
            ) quantidade_sala_utilizada_dentro,
            safe_cast(
                quantidade_sala_utilizada_fora as int64
            ) quantidade_sala_utilizada_fora,
            safe_cast(
                quantidade_sala_utilizada_climatizada as int64
            ) quantidade_sala_utilizada_climatizada,
            safe_cast(
                quantidade_sala_utilizada_acessivel as int64
            ) quantidade_sala_utilizada_acessivel,
            safe_cast(equipamento_parabolica as int64) equipamento_parabolica,
            safe_cast(
                quantidade_equipamento_parabolica as int64
            ) quantidade_equipamento_parabolica,
            safe_cast(equipamento_computador as int64) equipamento_computador,
            safe_cast(equipamento_copiadora as int64) equipamento_copiadora,
            safe_cast(
                quantidade_equipamento_copiadora as int64
            ) quantidade_equipamento_copiadora,
            safe_cast(equipamento_impressora as int64) equipamento_impressora,
            safe_cast(
                quantidade_equipamento_impressora as int64
            ) quantidade_equipamento_impressora,
            safe_cast(
                equipamento_impressora_multifuncional as int64
            ) equipamento_impressora_multifuncional,
            safe_cast(
                quantidade_equipamento_impressora_multifuncional as int64
            ) quantidade_equipamento_impressora_multifuncional,
            safe_cast(equipamento_scanner as int64) equipamento_scanner,
            safe_cast(equipamento_nenhum as int64) equipamento_nenhum,
            safe_cast(equipamento_dvd as int64) equipamento_dvd,
            safe_cast(quantidade_equipamento_dvd as int64) quantidade_equipamento_dvd,
            safe_cast(equipamento_som as int64) equipamento_som,
            safe_cast(quantidade_equipamento_som as int64) quantidade_equipamento_som,
            safe_cast(equipamento_tv as int64) equipamento_tv,
            safe_cast(quantidade_equipamento_tv as int64) quantidade_equipamento_tv,
            safe_cast(equipamento_lousa_digital as int64) equipamento_lousa_digital,
            safe_cast(
                quantidade_equipamento_lousa_digital as int64
            ) quantidade_equipamento_lousa_digital,
            safe_cast(equipamento_multimidia as int64) equipamento_multimidia,
            safe_cast(
                quantidade_equipamento_multimidia as int64
            ) quantidade_equipamento_multimidia,
            safe_cast(equipamento_videocassete as int64) equipamento_videocassete,
            safe_cast(
                quantidade_equipamento_videocassete as int64
            ) quantidade_equipamento_videocassete,
            safe_cast(equipamento_retroprojetor as int64) equipamento_retroprojetor,
            safe_cast(
                quantidade_equipamento_retroprojetor as int64
            ) quantidade_equipamento_retroprojetor,
            safe_cast(equipamento_fax as int64) equipamento_fax,
            safe_cast(quantidade_equipamento_fax as int64) quantidade_equipamento_fax,
            safe_cast(equipamento_foto as int64) equipamento_foto,
            safe_cast(quantidade_equipamento_foto as int64) quantidade_equipamento_foto,
            safe_cast(quantidade_computador_aluno as int64) quantidade_computador_aluno,
            safe_cast(desktop_aluno as int64) desktop_aluno,
            safe_cast(quantidade_desktop_aluno as int64) quantidade_desktop_aluno,
            safe_cast(computador_portatil_aluno as int64) computador_portatil_aluno,
            safe_cast(
                quantidade_computador_portatil_aluno as int64
            ) quantidade_computador_portatil_aluno,
            safe_cast(tablet_aluno as int64) tablet_aluno,
            safe_cast(quantidade_tablet_aluno as int64) quantidade_tablet_aluno,
            safe_cast(quantidade_computador as int64) quantidade_computador,
            safe_cast(
                quantidade_computador_administrativo as int64
            ) quantidade_computador_administrativo,
            safe_cast(internet as int64) internet,
            safe_cast(internet_alunos as int64) internet_alunos,
            safe_cast(internet_administrativo as int64) internet_administrativo,
            safe_cast(internet_aprendizagem as int64) internet_aprendizagem,
            safe_cast(internet_comunidade as int64) internet_comunidade,
            safe_cast(acesso_internet_computador as int64) acesso_internet_computador,
            safe_cast(
                acesso_internet_dispositivo_pessoal as int64
            ) acesso_internet_dispositivo_pessoal,
            safe_cast(tipo_rede_local as string) tipo_rede_local,
            safe_cast(banda_larga as int64) banda_larga,
            safe_cast(quantidade_funcionario as int64) quantidade_funcionario,
            safe_cast(profissional_administrativo as int64) profissional_administrativo,
            safe_cast(
                quantidade_profissional_administrativo as int64
            ) quantidade_profissional_administrativo,
            safe_cast(profissional_servico_geral as int64) profissional_servico_geral,
            safe_cast(
                quantidade_profissional_servico_geral as int64
            ) quantidade_profissional_servico_geral,
            safe_cast(profissional_bibliotecario as int64) profissional_bibliotecario,
            safe_cast(
                quantidade_profissional_bibliotecario as int64
            ) quantidade_profissional_bibliotecario,
            safe_cast(profissional_saude as int64) profissional_saude,
            safe_cast(
                quantidade_profissional_saude as int64
            ) quantidade_profissional_saude,
            safe_cast(profissional_coordenador as int64) profissional_coordenador,
            safe_cast(
                quantidade_profissional_coordenador as int64
            ) quantidade_profissional_coordenador,
            safe_cast(profissional_fonaudiologo as int64) profissional_fonaudiologo,
            safe_cast(
                quantidade_profissional_fonaudiologo as int64
            ) quantidade_profissional_fonaudiologo,
            safe_cast(profissional_nutricionista as int64) profissional_nutricionista,
            safe_cast(
                quantidade_profissional_nutricionista as int64
            ) quantidade_profissional_nutricionista,
            safe_cast(profissional_psicologo as int64) profissional_psicologo,
            safe_cast(
                quantidade_profissional_psicologo as int64
            ) quantidade_profissional_psicologo,
            safe_cast(profissional_alimentacao as int64) profissional_alimentacao,
            safe_cast(
                quantidade_profissional_alimentacao as int64
            ) quantidade_profissional_alimentacao,
            safe_cast(profissional_pedagogia as int64) profissional_pedagogia,
            safe_cast(
                quantidade_profissional_pedagogia as int64
            ) quantidade_profissional_pedagogia,
            safe_cast(profissional_secretario as int64) profissional_secretario,
            safe_cast(
                quantidade_profissional_secretario as int64
            ) quantidade_profissional_secretario,
            safe_cast(profissional_seguranca as int64) profissional_seguranca,
            safe_cast(
                quantidade_profissional_seguranca as int64
            ) quantidade_profissional_seguranca,
            safe_cast(profissional_monitor as int64) profissional_monitor,
            safe_cast(
                quantidade_profissional_monitor as int64
            ) quantidade_profissional_monitor,
            safe_cast(profissional_gestao as int64) profissional_gestao,
            safe_cast(
                quantidade_profissional_gestao as int64
            ) quantidade_profissional_gestao,
            safe_cast(
                profissional_assistente_social as int64
            ) profissional_assistente_social,
            safe_cast(
                quantidade_profissional_assistente_social as int64
            ) quantidade_profissional_assistente_social,
            null as profissional_tradutor_libras,
            null as quantidade_profissional_tradutor_libras,
            safe_cast(alimentacao as int64) alimentacao,
            safe_cast(organizacao_serie_ano as int64) organizacao_serie_ano,
            safe_cast(organizacao_semestre as int64) organizacao_semestre,
            safe_cast(
                organizacao_fundamental_ciclo as int64
            ) organizacao_fundamental_ciclo,
            safe_cast(
                organizacao_grupo_nao_seriado as int64
            ) organizacao_grupo_nao_seriado,
            safe_cast(organizacao_modulo as int64) organizacao_modulo,
            safe_cast(organizacao_alternancia as int64) organizacao_alternancia,
            safe_cast(
                material_pedagogico_multimidia as int64
            ) material_pedagogico_multimidia,
            safe_cast(
                material_pedagogico_infantil as int64
            ) material_pedagogico_infantil,
            safe_cast(
                material_pedagogico_cientifico as int64
            ) material_pedagogico_cientifico,
            safe_cast(material_pedagogico_difusao as int64) material_pedagogico_difusao,
            safe_cast(material_pedagogico_musical as int64) material_pedagogico_musical,
            safe_cast(material_pedagogico_jogo as int64) material_pedagogico_jogo,
            safe_cast(
                material_pedagogico_artistica as int64
            ) material_pedagogico_artistica,
            safe_cast(
                material_pedagogico_profissional as int64
            ) material_pedagogico_profissional,
            safe_cast(
                material_pedagogico_desportiva as int64
            ) material_pedagogico_desportiva,
            safe_cast(
                material_pedagogico_indigena as int64
            ) material_pedagogico_indigena,
            safe_cast(material_pedagogico_etnico as int64) material_pedagogico_etnico,
            safe_cast(material_pedagogico_campo as int64) material_pedagogico_campo,
            null as material_pedagogico_surdo,
            safe_cast(material_pedagogico_nenhum as int64) material_pedagogico_nenhum,
            safe_cast(
                material_especifico_quilombola as int64
            ) material_especifico_quilombola,
            safe_cast(
                material_especifico_indigena as int64
            ) material_especifico_indigena,
            safe_cast(
                material_especifico_nao_utiliza as int64
            ) material_especifico_nao_utiliza,
            safe_cast(educacao_indigena as int64) educacao_indigena,
            safe_cast(tipo_lingua_indigena as string) tipo_lingua_indigena,
            safe_cast(id_lingua_indigena_1 as string) id_lingua_indigena_1,
            safe_cast(id_lingua_indigena_2 as string) id_lingua_indigena_2,
            safe_cast(id_lingua_indigena_3 as string) id_lingua_indigena_3,
            safe_cast(
                programa_brasil_alfabetizado as int64
            ) programa_brasil_alfabetizado,
            safe_cast(final_semana as int64) final_semana,
            safe_cast(exame_selecao as int64) exame_selecao,
            safe_cast(reserva_vaga_raca_cor as int64) reserva_vaga_raca_cor,
            safe_cast(reserva_vaga_renda as int64) reserva_vaga_renda,
            safe_cast(reserva_vaga_publica as int64) reserva_vaga_publica,
            safe_cast(reserva_vaga_pcd as int64) reserva_vaga_pcd,
            safe_cast(reserva_vaga_outros as int64) reserva_vaga_outros,
            safe_cast(reserva_vaga_nenhuma as int64) reserva_vaga_nenhuma,
            safe_cast(redes_sociais as int64) redes_sociais,
            safe_cast(espaco_atividade_comunidade as int64) espaco_atividade_comunidade,
            safe_cast(espaco_equipamento_alunos as int64) espaco_equipamento_alunos,
            safe_cast(orgao_associacao_pais as int64) orgao_associacao_pais,
            safe_cast(
                orgao_associacao_pais_mestres as int64
            ) orgao_associacao_pais_mestres,
            safe_cast(orgao_conselho_escolar as int64) orgao_conselho_escolar,
            safe_cast(orgao_gremio_estudantil as int64) orgao_gremio_estudantil,
            safe_cast(orgao_outros as int64) orgao_outros,
            safe_cast(orgao_nenhum as int64) orgao_nenhum,
            safe_cast(tipo_proposta_pedagogica as string) tipo_proposta_pedagogica,
            safe_cast(tipo_aee as string) tipo_aee,
            safe_cast(
                tipo_atividade_complementar as string
            ) tipo_atividade_complementar,
            safe_cast(escolarizacao as int64) escolarizacao,
            safe_cast(mediacao_presencial as int64) mediacao_presencial,
            safe_cast(mediacao_semipresencial as int64) mediacao_semipresencial,
            safe_cast(mediacao_ead as int64) mediacao_ead,
            safe_cast(regular as int64) regular,
            safe_cast(diurno as int64) diurno,
            safe_cast(noturno as int64) noturno,
            safe_cast(ead as int64) ead,
            safe_cast(educacao_basica as int64) educacao_basica,
            safe_cast(etapa_ensino_infantil as int64) etapa_ensino_infantil,
            safe_cast(
                etapa_ensino_infantil_creche as int64
            ) etapa_ensino_infantil_creche,
            safe_cast(
                etapa_ensino_infantil_pre_escola as int64
            ) etapa_ensino_infantil_pre_escola,
            safe_cast(etapa_ensino_fundamental as int64) etapa_ensino_fundamental,
            safe_cast(
                etapa_ensino_fundamental_anos_iniciais as int64
            ) etapa_ensino_fundamental_anos_iniciais,
            safe_cast(
                etapa_ensino_fundamental_anos_finais as int64
            ) etapa_ensino_fundamental_anos_finais,
            safe_cast(etapa_ensino_medio as int64) etapa_ensino_medio,
            safe_cast(etapa_ensino_profissional as int64) etapa_ensino_profissional,
            safe_cast(
                etapa_ensino_profissional_tecnica as int64
            ) etapa_ensino_profissional_tecnica,
            safe_cast(etapa_ensino_eja as int64) etapa_ensino_eja,
            safe_cast(
                etapa_ensino_eja_fundamental as int64
            ) etapa_ensino_eja_fundamental,
            safe_cast(etapa_ensino_eja_medio as int64) etapa_ensino_eja_medio,
            safe_cast(etapa_ensino_especial as int64) etapa_ensino_especial,
            safe_cast(etapa_ensino_especial_comum as int64) etapa_ensino_especial_comum,
            safe_cast(
                etapa_ensino_especial_exclusiva as int64
            ) etapa_ensino_especial_exclusiva,
            safe_cast(etapa_ensino_creche_comum as int64) etapa_ensino_creche_comum,
            safe_cast(
                etapa_ensino_pre_escola_comum as int64
            ) etapa_ensino_pre_escola_comum,
            safe_cast(
                etapa_ensino_fundamental_anos_iniciais_comum as int64
            ) etapa_ensino_fundamental_anos_iniciais_comum,
            safe_cast(
                etapa_ensino_fundamental_anos_finais_comum as int64
            ) etapa_ensino_fundamental_anos_finais_comum,
            safe_cast(etapa_ensino_medio_comum as int64) etapa_ensino_medio_comum,
            safe_cast(
                etapa_ensino_medio_integrado_comum as int64
            ) etapa_ensino_medio_integrado_comum,
            safe_cast(
                etapa_ensino_medio_normal_comum as int64
            ) etapa_ensino_medio_normal_comum,
            safe_cast(
                etapa_ensino_profissional_comum as int64
            ) etapa_ensino_profissional_comum,
            safe_cast(
                etapa_ensino_eja_fundamental_comum as int64
            ) etapa_ensino_eja_fundamental_comum,
            safe_cast(
                etapa_ensino_eja_medio_comum as int64
            ) etapa_ensino_eja_medio_comum,
            safe_cast(
                etapa_ensino_eja_profissional_comum as int64
            ) etapa_ensino_eja_profissional_comum,
            safe_cast(
                etapa_ensino_creche_especial_exclusiva as int64
            ) etapa_ensino_creche_especial_exclusiva,
            safe_cast(
                etapa_ensino_pre_escola_especial_exclusiva as int64
            ) etapa_ensino_pre_escola_especial_exclusiva,
            safe_cast(
                etapa_ensino_fundamental_anos_iniciais_especial_exclusiva as int64
            ) etapa_ensino_fundamental_anos_iniciais_especial_exclusiva,
            safe_cast(
                etapa_ensino_fundamental_anos_finais_especial_exclusiva as int64
            ) etapa_ensino_fundamental_anos_finais_especial_exclusiva,
            safe_cast(
                etapa_ensino_medio_especial_exclusiva as int64
            ) etapa_ensino_medio_especial_exclusiva,
            safe_cast(
                etapa_ensino_medio_integrado_especial_exclusiva as int64
            ) etapa_ensino_medio_integrado_especial_exclusiva,
            safe_cast(
                etapa_ensino_medio_normal_especial_exclusiva as int64
            ) etapa_ensino_medio_normal_especial_exclusiva,
            safe_cast(
                etapa_ensino_profissional_especial_exclusiva as int64
            ) etapa_ensino_profissional_especial_exclusiva,
            safe_cast(
                etapa_ensino_eja_fundamental_especial_exclusiva as int64
            ) etapa_ensino_eja_fundamental_especial_exclusiva,
            safe_cast(
                etapa_ensino_eja_medio_especial_exclusiva as int64
            ) etapa_ensino_eja_medio_especial_exclusiva,
            safe_cast(
                quantidade_matricula_educacao_basica as int64
            ) quantidade_matricula_educacao_basica,
            safe_cast(
                quantidade_matricula_infantil as int64
            ) quantidade_matricula_infantil,
            safe_cast(
                quantidade_matricula_infantil_creche as int64
            ) quantidade_matricula_infantil_creche,
            safe_cast(
                quantidade_matricula_infantil_pre_escola as int64
            ) quantidade_matricula_infantil_pre_escola,
            safe_cast(
                quantidade_matricula_fundamental as int64
            ) quantidade_matricula_fundamental,
            safe_cast(
                quantidade_matricula_fundamental_anos_iniciais as int64
            ) quantidade_matricula_fundamental_anos_iniciais,
            null as quantidade_matricula_fundamental_1_ano,
            null as quantidade_matricula_fundamental_2_ano,
            null as quantidade_matricula_fundamental_3_ano,
            null as quantidade_matricula_fundamental_4_ano,
            null as quantidade_matricula_fundamental_5_ano,
            safe_cast(
                quantidade_matricula_fundamental_anos_finais as int64
            ) quantidade_matricula_fundamental_anos_finais,
            null as quantidade_matricula_fundamental_6_ano,
            null as quantidade_matricula_fundamental_7_ano,
            null as quantidade_matricula_fundamental_8_ano,
            null as quantidade_matricula_fundamental_9_ano,
            safe_cast(quantidade_matricula_medio as int64) quantidade_matricula_medio,
            null as quantidade_matricula_medio_propedeutico,
            null as quantidade_matricula_medio_propedeutico_1_ano,
            null as quantidade_matricula_medio_propedeutico_2_ano,
            null as quantidade_matricula_medio_propedeutico_3_ano,
            null as quantidade_matricula_medio_propedeutico_4_ano,
            null as quantidade_matricula_medio_propedeutico_nao_seriado,
            null as quantidade_matricula_medio_tecnico,
            null as quantidade_matricula_medio_tecnico_1_ano,
            null as quantidade_matricula_medio_tecnico_2_ano,
            null as quantidade_matricula_medio_tecnico_3_ano,
            null as quantidade_matricula_medio_tecnico_4_ano,
            null as quantidade_matricula_medio_tecnico_nao_seriado,
            null as quantidade_matricula_medio_magisterio,
            null as quantidade_matricula_medio_magisterio_1_ano,
            null as quantidade_matricula_medio_magisterio_2_ano,
            null as quantidade_matricula_medio_magisterio_3_ano,
            null as quantidade_matricula_medio_magisterio_4_ano,
            safe_cast(
                quantidade_matricula_profissional as int64
            ) quantidade_matricula_profissional,
            safe_cast(
                quantidade_matricula_profissional_tecnica as int64
            ) quantidade_matricula_profissional_tecnica,
            null as quantidade_matricula_profissional_tecnica_concomitante,
            null as quantidade_matricula_profissional_tecnica_subsequente,
            null as quantidade_matricula_profissional_fic_concomitante,
            safe_cast(quantidade_matricula_eja as int64) quantidade_matricula_eja,
            safe_cast(
                quantidade_matricula_eja_fundamental as int64
            ) quantidade_matricula_eja_fundamental,
            null as quantidade_matricula_eja_fundamental_anos_iniciais,
            null as quantidade_matricula_eja_fundamental_anos_finais,
            null as quantidade_matricula_eja_fundamental_fic,
            safe_cast(
                quantidade_matricula_eja_medio as int64
            ) quantidade_matricula_eja_medio,
            null as quantidade_matricula_eja_medio_nao_profissionalizante,
            null as quantidade_matricula_eja_medio_fic,
            null as quantidade_matricula_eja_medio_tecnico,
            safe_cast(
                quantidade_matricula_especial as int64
            ) quantidade_matricula_especial,
            safe_cast(
                quantidade_matricula_especial_comum as int64
            ) quantidade_matricula_especial_comum,
            safe_cast(
                quantidade_matricula_especial_exclusiva as int64
            ) quantidade_matricula_especial_exclusiva,
            safe_cast(
                quantidade_matricula_feminino as int64
            ) quantidade_matricula_feminino,
            safe_cast(
                quantidade_matricula_masculino as int64
            ) quantidade_matricula_masculino,
            safe_cast(
                quantidade_matricula_nao_declarada as int64
            ) quantidade_matricula_nao_declarada,
            safe_cast(quantidade_matricula_branca as int64) quantidade_matricula_branca,
            safe_cast(quantidade_matricula_preta as int64) quantidade_matricula_preta,
            safe_cast(quantidade_matricula_parda as int64) quantidade_matricula_parda,
            safe_cast(
                quantidade_matricula_amarela as int64
            ) quantidade_matricula_amarela,
            safe_cast(
                quantidade_matricula_indigena as int64
            ) quantidade_matricula_indigena,
            safe_cast(
                quantidade_matricula_idade_0_3 as int64
            ) quantidade_matricula_idade_0_3,
            safe_cast(
                quantidade_matricula_idade_4_5 as int64
            ) quantidade_matricula_idade_4_5,
            safe_cast(
                quantidade_matricula_idade_6_10 as int64
            ) quantidade_matricula_idade_6_10,
            safe_cast(
                quantidade_matricula_idade_11_14 as int64
            ) quantidade_matricula_idade_11_14,
            safe_cast(
                quantidade_matricula_idade_15_17 as int64
            ) quantidade_matricula_idade_15_17,
            safe_cast(
                quantidade_matricula_idade_18 as int64
            ) quantidade_matricula_idade_18,
            safe_cast(quantidade_matricula_diurno as int64) quantidade_matricula_diurno,
            safe_cast(
                quantidade_matricula_noturno as int64
            ) quantidade_matricula_noturno,
            safe_cast(quantidade_matricula_ead as int64) quantidade_matricula_ead,
            safe_cast(
                quantidade_matricula_infantil_integral as int64
            ) quantidade_matricula_infantil_integral,
            safe_cast(
                quantidade_matricula_infantil_creche_integral as int64
            ) quantidade_matricula_infantil_creche_integral,
            safe_cast(
                quantidade_matricula_infantil_pre_escola_integral as int64
            ) quantidade_matricula_infantil_pre_escola_integral,
            safe_cast(
                quantidade_matricula_fundamental_integral as int64
            ) quantidade_matricula_fundamental_integral,
            safe_cast(
                quantidade_matricula_fundamental_anos_iniciais_integral as int64
            ) quantidade_matricula_fundamental_anos_iniciais_integral,
            safe_cast(
                quantidade_matricula_fundamental_anos_finais_integral as int64
            ) quantidade_matricula_fundamental_anos_finais_integral,
            safe_cast(
                quantidade_matricula_medio_integral as int64
            ) quantidade_matricula_medio_integral,
            null as quantidade_matricula_zona_residencia_urbana,
            null as quantidade_matricula_zona_residencia_rural,
            null as quantidade_matricula_zona_residencia_nao_aplicavel,
            null as quantidade_matricula_utiliza_transporte_publico,
            null as quantidade_matricula_transporte_estadual,
            null as quantidade_matricula_transporte_municipal,
            safe_cast(
                quantidade_docente_educacao_basica as int64
            ) quantidade_docente_educacao_basica,
            safe_cast(quantidade_docente_infantil as int64) quantidade_docente_infantil,
            safe_cast(
                quantidade_docente_infantil_creche as int64
            ) quantidade_docente_infantil_creche,
            safe_cast(
                quantidade_docente_infantil_pre_escola as int64
            ) quantidade_docente_infantil_pre_escola,
            safe_cast(
                quantidade_docente_fundamental as int64
            ) quantidade_docente_fundamental,
            safe_cast(
                quantidade_docente_fundamental_anos_iniciais as int64
            ) quantidade_docente_fundamental_anos_iniciais,
            safe_cast(
                quantidade_docente_fundamental_anos_finais as int64
            ) quantidade_docente_fundamental_anos_finais,
            safe_cast(quantidade_docente_medio as int64) quantidade_docente_medio,
            safe_cast(
                quantidade_docente_profissional as int64
            ) quantidade_docente_profissional,
            safe_cast(
                quantidade_docente_profissional_tecnica as int64
            ) quantidade_docente_profissional_tecnica,
            safe_cast(quantidade_docente_eja as int64) quantidade_docente_eja,
            safe_cast(
                quantidade_docente_eja_fundamental as int64
            ) quantidade_docente_eja_fundamental,
            safe_cast(
                quantidade_docente_eja_medio as int64
            ) quantidade_docente_eja_medio,
            safe_cast(quantidade_docente_especial as int64) quantidade_docente_especial,
            safe_cast(
                quantidade_docente_especial_comum as int64
            ) quantidade_docente_especial_comum,
            safe_cast(
                quantidade_docente_especial_exclusiva as int64
            ) quantidade_docente_especial_exclusiva,
            safe_cast(
                quantidade_turma_educacao_basica as int64
            ) quantidade_turma_educacao_basica,
            safe_cast(quantidade_turma_infantil as int64) quantidade_turma_infantil,
            null as quantidade_turma_infantil_integral,
            safe_cast(
                quantidade_turma_infantil_creche as int64
            ) quantidade_turma_infantil_creche,
            null as quantidade_turma_infantil_creche_integral,
            safe_cast(
                quantidade_turma_infantil_pre_escola as int64
            ) quantidade_turma_infantil_pre_escola,
            null as quantidade_turma_infantil_pre_escola_integral,
            safe_cast(
                quantidade_turma_fundamental as int64
            ) quantidade_turma_fundamental,
            null as quantidade_turma_fundamental_integral,
            safe_cast(
                quantidade_turma_fundamental_anos_iniciais as int64
            ) quantidade_turma_fundamental_anos_iniciais,
            null as quantidade_turma_fundamental_anos_iniciais_integral,
            safe_cast(
                quantidade_turma_fundamental_anos_finais as int64
            ) quantidade_turma_fundamental_anos_finais,
            null as quantidade_turma_fundamental_anos_finais_integral,
            safe_cast(quantidade_turma_medio as int64) quantidade_turma_medio,
            null as quantidade_turma_medio_integral,
            safe_cast(
                quantidade_turma_profissional as int64
            ) quantidade_turma_profissional,
            safe_cast(
                quantidade_turma_profissional_tecnica as int64
            ) quantidade_turma_profissional_tecnica,
            safe_cast(quantidade_turma_eja as int64) quantidade_turma_eja,
            safe_cast(
                quantidade_turma_eja_fundamental as int64
            ) quantidade_turma_eja_fundamental,
            safe_cast(quantidade_turma_eja_medio as int64) quantidade_turma_eja_medio,
            safe_cast(quantidade_turma_especial as int64) quantidade_turma_especial,
            safe_cast(
                quantidade_turma_especial_comum as int64
            ) quantidade_turma_especial_comum,
            safe_cast(
                quantidade_turma_especial_exclusiva as int64
            ) quantidade_turma_especial_exclusiva,
            null as quantidade_turma_diurno,
            null as quantidade_turma_noturno,
            null as quantidade_turma_ead,
        from `basedosdados-staging.br_inep_censo_escolar_staging.escola`
    ),

    censo_2023 as (
        select
            safe_cast(ano as int64) ano,
            safe_cast(sigla_uf as string) sigla_uf,
            safe_cast(id_municipio as string) id_municipio,
            safe_cast(id_escola as string) id_escola,
            safe_cast(rede as string) rede,
            safe_cast(
                tipo_categoria_escola_privada as string
            ) tipo_categoria_escola_privada,
            safe_cast(tipo_localizacao as string) tipo_localizacao,
            safe_cast(
                tipo_localizacao_diferenciada as string
            ) tipo_localizacao_diferenciada,
            safe_cast(
                tipo_situacao_funcionamento as string
            ) tipo_situacao_funcionamento,
            safe_cast(id_orgao_regional as string) id_orgao_regional,
            safe_cast(data_ano_letivo_inicio as date) data_ano_letivo_inicio,
            safe_cast(data_ano_letivo_termino as date) data_ano_letivo_termino,
            safe_cast(vinculo_secretaria_educacao as int64) vinculo_secretaria_educacao,
            safe_cast(vinculo_seguranca_publica as int64) vinculo_seguranca_publica,
            safe_cast(vinculo_secretaria_saude as int64) vinculo_secretaria_saude,
            safe_cast(vinculo_outro_orgao as int64) vinculo_outro_orgao,
            safe_cast(poder_publico_parceria as int64) poder_publico_parceria,
            safe_cast(
                tipo_poder_publico_parceria as string
            ) tipo_poder_publico_parceria,
            safe_cast(conveniada_poder_publico as int64) conveniada_poder_publico,
            safe_cast(
                tipo_convenio_poder_publico as string
            ) tipo_convenio_poder_publico,
            safe_cast(
                forma_contratacao_termo_colaboracao as int64
            ) forma_contratacao_termo_colaboracao,
            safe_cast(
                forma_contratacao_termo_fomento as int64
            ) forma_contratacao_termo_fomento,
            safe_cast(
                forma_contratacao_acordo_cooperacao as int64
            ) forma_contratacao_acordo_cooperacao,
            safe_cast(
                forma_contratacao_prestacao_servico as int64
            ) forma_contratacao_prestacao_servico,
            safe_cast(
                forma_contratacao_cooperacao_tecnica_financeira as int64
            ) forma_contratacao_cooperacao_tecnica_financeira,
            safe_cast(
                forma_contratacao_consorcio_publico as int64
            ) forma_contratacao_consorcio_publico,
            safe_cast(
                forma_contratacao_parceria_municipal_termo_colaboracao as int64
            ) forma_contratacao_parceria_municipal_termo_colaboracao,
            safe_cast(
                forma_contratacao_parceria_municipal_termo_fomento as int64
            ) forma_contratacao_parceria_municipal_termo_fomento,
            safe_cast(
                forma_contratacao_parceria_municipal_acordo_cooperacao as int64
            ) forma_contratacao_parceria_municipal_acordo_cooperacao,
            safe_cast(
                forma_contratacao_parceria_municipal_prestacao_servico as int64
            ) forma_contratacao_parceria_municipal_prestacao_servico,
            safe_cast(
                forma_contratacao_parceria_municipal_cooperacao_tecnica_financeira
                as int64
            ) forma_contratacao_parceria_municipal_cooperacao_tecnica_financeira,
            safe_cast(
                forma_contratacao_parceria_municipal_consorcio_publico as int64
            ) forma_contratacao_parceria_municipal_consorcio_publico,
            safe_cast(
                forma_contratacao_parceria_estadual_termo_colaboracao as int64
            ) forma_contratacao_parceria_estadual_termo_colaboracao,
            safe_cast(
                forma_contratacao_parceria_estadual_termo_fomento as int64
            ) forma_contratacao_parceria_estadual_termo_fomento,
            safe_cast(
                forma_contratacao_parceria_estadual_acordo_cooperacao as int64
            ) forma_contratacao_parceria_estadual_acordo_cooperacao,
            safe_cast(
                forma_contratacao_parceria_estadual_prestacao_servico as int64
            ) forma_contratacao_parceria_estadual_prestacao_servico,
            safe_cast(
                forma_contratacao_parceria_estadual_cooperacao_tecnica_financeira
                as int64
            ) forma_contratacao_parceria_estadual_cooperacao_tecnica_financeira,
            safe_cast(
                forma_contratacao_parceria_estadual_consorcio_publico as int64
            ) forma_contratacao_parceria_estadual_consorcio_publico,
            safe_cast(
                tipo_atendimento_escolarizacao as int64
            ) tipo_atendimento_escolarizacao,
            safe_cast(
                tipo_atendimento_atividade_complementar as int64
            ) tipo_atendimento_atividade_complementar,
            safe_cast(tipo_atendimento_aee as int64) tipo_atendimento_aee,
            safe_cast(
                mantenedora_escola_privada_empresa as int64
            ) mantenedora_escola_privada_empresa,
            safe_cast(
                mantenedora_escola_privada_ong as int64
            ) mantenedora_escola_privada_ong,
            safe_cast(
                mantenedora_escola_privada_oscip as int64
            ) mantenedora_escola_privada_oscip,
            safe_cast(
                mantenedora_escola_privada_ong_oscip as int64
            ) mantenedora_escola_privada_ong_oscip,
            safe_cast(
                mantenedora_escola_privada_sindicato as int64
            ) mantenedora_escola_privada_sindicato,
            safe_cast(
                mantenedora_escola_privada_sistema_s as int64
            ) mantenedora_escola_privada_sistema_s,
            safe_cast(
                mantenedora_escola_privada_sem_fins as int64
            ) mantenedora_escola_privada_sem_fins,
            safe_cast(cnpj_escola_privada as string) cnpj_escola_privada,
            safe_cast(cnpj_mantenedora as string) cnpj_mantenedora,
            safe_cast(tipo_regulamentacao as string) tipo_regulamentacao,
            safe_cast(
                tipo_responsavel_regulamentacao as string
            ) tipo_responsavel_regulamentacao,
            safe_cast(id_escola_sede as string) id_escola_sede,
            safe_cast(id_ies_ofertante as string) id_ies_ofertante,
            safe_cast(
                local_funcionamento_predio_escolar as int64
            ) local_funcionamento_predio_escolar,
            safe_cast(
                tipo_local_funcionamento_predio_escolar as string
            ) tipo_local_funcionamento_predio_escolar,
            safe_cast(
                local_funcionamento_sala_empresa as int64
            ) local_funcionamento_sala_empresa,
            safe_cast(
                local_funcionamento_socioeducativo as int64
            ) local_funcionamento_socioeducativo,
            safe_cast(
                local_funcionamento_unidade_prisional as int64
            ) local_funcionamento_unidade_prisional,
            safe_cast(
                local_funcionamento_prisional_socio as int64
            ) local_funcionamento_prisional_socio,
            safe_cast(
                local_funcionamento_templo_igreja as int64
            ) local_funcionamento_templo_igreja,
            safe_cast(
                local_funcionamento_casa_professor as int64
            ) local_funcionamento_casa_professor,
            safe_cast(local_funcionamento_galpao as int64) local_funcionamento_galpao,
            safe_cast(
                tipo_local_funcionamento_galpao as string
            ) tipo_local_funcionamento_galpao,
            safe_cast(
                local_funcionamento_outra_escola as int64
            ) local_funcionamento_outra_escola,
            safe_cast(local_funcionamento_outros as int64) local_funcionamento_outros,
            safe_cast(predio_compartilhado as int64) predio_compartilhado,
            safe_cast(agua_filtrada as int64) agua_filtrada,
            safe_cast(agua_potavel as int64) agua_potavel,
            safe_cast(agua_rede_publica as int64) agua_rede_publica,
            safe_cast(agua_poco_artesiano as int64) agua_poco_artesiano,
            safe_cast(agua_cacimba as int64) agua_cacimba,
            safe_cast(agua_fonte_rio as int64) agua_fonte_rio,
            safe_cast(agua_inexistente as int64) agua_inexistente,
            safe_cast(energia_rede_publica as int64) energia_rede_publica,
            safe_cast(energia_gerador as int64) energia_gerador,
            safe_cast(energia_gerador_fossil as int64) energia_gerador_fossil,
            safe_cast(energia_outros as int64) energia_outros,
            safe_cast(energia_renovavel as int64) energia_renovavel,
            safe_cast(energia_inexistente as int64) energia_inexistente,
            safe_cast(esgoto_rede_publica as int64) esgoto_rede_publica,
            safe_cast(esgoto_fossa as int64) esgoto_fossa,
            safe_cast(esgoto_fossa_septica as int64) esgoto_fossa_septica,
            safe_cast(esgoto_fossa_comum as int64) esgoto_fossa_comum,
            safe_cast(esgoto_inexistente as int64) esgoto_inexistente,
            safe_cast(lixo_servico_coleta as int64) lixo_servico_coleta,
            safe_cast(lixo_queima as int64) lixo_queima,
            safe_cast(lixo_enterrado as int64) lixo_enterrado,
            safe_cast(lixo_destino_final_publico as int64) lixo_destino_final_publico,
            safe_cast(lixo_descarta_outra_area as int64) lixo_descarta_outra_area,
            safe_cast(lixo_joga_outra_area as int64) lixo_joga_outra_area,
            safe_cast(lixo_outros as int64) lixo_outros,
            safe_cast(lixo_reciclagem as int64) lixo_reciclagem,
            safe_cast(tratamento_lixo_separacao as int64) tratamento_lixo_separacao,
            safe_cast(
                tratamento_lixo_reutilizacao as int64
            ) tratamento_lixo_reutilizacao,
            safe_cast(tratamento_lixo_reciclagem as int64) tratamento_lixo_reciclagem,
            safe_cast(tratamento_lixo_inexistente as int64) tratamento_lixo_inexistente,
            safe_cast(almoxarifado as int64) almoxarifado,
            safe_cast(area_verde as int64) area_verde,
            safe_cast(auditorio as int64) auditorio,
            safe_cast(banheiro_fora_predio as int64) banheiro_fora_predio,
            safe_cast(banheiro_dentro_predio as int64) banheiro_dentro_predio,
            safe_cast(banheiro as int64) banheiro,
            safe_cast(banheiro_educacao_infantil as int64) banheiro_educacao_infantil,
            safe_cast(banheiro_pne as int64) banheiro_pne,
            safe_cast(banheiro_funcionarios as int64) banheiro_funcionarios,
            safe_cast(banheiro_chuveiro as int64) banheiro_chuveiro,
            safe_cast(bercario as int64) bercario,
            safe_cast(biblioteca as int64) biblioteca,
            safe_cast(biblioteca_sala_leitura as int64) biblioteca_sala_leitura,
            safe_cast(cozinha as int64) cozinha,
            safe_cast(despensa as int64) despensa,
            safe_cast(dormitorio_aluno as int64) dormitorio_aluno,
            safe_cast(dormitorio_professor as int64) dormitorio_professor,
            safe_cast(laboratorio_ciencias as int64) laboratorio_ciencias,
            safe_cast(laboratorio_informatica as int64) laboratorio_informatica,
            safe_cast(
                laboratorio_educacao_profissional as int64
            ) laboratorio_educacao_profissional,
            safe_cast(patio_coberto as int64) patio_coberto,
            safe_cast(patio_descoberto as int64) patio_descoberto,
            safe_cast(parque_infantil as int64) parque_infantil,
            safe_cast(piscina as int64) piscina,
            safe_cast(quadra_esportes as int64) quadra_esportes,
            safe_cast(quadra_esportes_coberta as int64) quadra_esportes_coberta,
            safe_cast(quadra_esportes_descoberta as int64) quadra_esportes_descoberta,
            safe_cast(refeitorio as int64) refeitorio,
            safe_cast(sala_atelie_artes as int64) sala_atelie_artes,
            safe_cast(sala_musica_coral as int64) sala_musica_coral,
            safe_cast(sala_estudio_danca as int64) sala_estudio_danca,
            safe_cast(sala_multiuso as int64) sala_multiuso,
            safe_cast(sala_estudio_gravacao as int64) sala_estudio_gravacao,
            safe_cast(
                sala_oficinas_educacao_profissional as int64
            ) sala_oficinas_educacao_profissional,
            safe_cast(sala_diretoria as int64) sala_diretoria,
            safe_cast(sala_leitura as int64) sala_leitura,
            safe_cast(sala_professor as int64) sala_professor,
            safe_cast(sala_repouso_aluno as int64) sala_repouso_aluno,
            safe_cast(secretaria as int64) secretaria,
            safe_cast(sala_atendimento_especial as int64) sala_atendimento_especial,
            safe_cast(terreirao as int64) terreirao,
            safe_cast(viveiro as int64) viveiro,
            safe_cast(dependencia_pne as int64) dependencia_pne,
            safe_cast(lavanderia as int64) lavanderia,
            safe_cast(dependencia_outras as int64) dependencia_outras,
            safe_cast(acessibilidade_corrimao as int64) acessibilidade_corrimao,
            safe_cast(acessibilidade_elevador as int64) acessibilidade_elevador,
            safe_cast(acessibilidade_pisos_tateis as int64) acessibilidade_pisos_tateis,
            safe_cast(acessibilidade_vao_livre as int64) acessibilidade_vao_livre,
            safe_cast(acessibilidade_rampas as int64) acessibilidade_rampas,
            safe_cast(acessibilidade_sinal_sonoro as int64) acessibilidade_sinal_sonoro,
            safe_cast(acessibilidade_sinal_tatil as int64) acessibilidade_sinal_tatil,
            safe_cast(acessibilidade_sinal_visual as int64) acessibilidade_sinal_visual,
            safe_cast(acessibilidade_inexistente as int64) acessibilidade_inexistente,
            safe_cast(quantidade_sala_existente as int64) quantidade_sala_existente,
            safe_cast(quantidade_sala_utilizada as int64) quantidade_sala_utilizada,
            safe_cast(
                quantidade_sala_utilizada_dentro as int64
            ) quantidade_sala_utilizada_dentro,
            safe_cast(
                quantidade_sala_utilizada_fora as int64
            ) quantidade_sala_utilizada_fora,
            safe_cast(
                quantidade_sala_utilizada_climatizada as int64
            ) quantidade_sala_utilizada_climatizada,
            safe_cast(
                quantidade_sala_utilizada_acessivel as int64
            ) quantidade_sala_utilizada_acessivel,
            safe_cast(equipamento_parabolica as int64) equipamento_parabolica,
            safe_cast(
                quantidade_equipamento_parabolica as int64
            ) quantidade_equipamento_parabolica,
            safe_cast(equipamento_computador as int64) equipamento_computador,
            safe_cast(equipamento_copiadora as int64) equipamento_copiadora,
            safe_cast(
                quantidade_equipamento_copiadora as int64
            ) quantidade_equipamento_copiadora,
            safe_cast(equipamento_impressora as int64) equipamento_impressora,
            safe_cast(
                quantidade_equipamento_impressora as int64
            ) quantidade_equipamento_impressora,
            safe_cast(
                equipamento_impressora_multifuncional as int64
            ) equipamento_impressora_multifuncional,
            safe_cast(
                quantidade_equipamento_impressora_multifuncional as int64
            ) quantidade_equipamento_impressora_multifuncional,
            safe_cast(equipamento_scanner as int64) equipamento_scanner,
            safe_cast(equipamento_nenhum as int64) equipamento_nenhum,
            safe_cast(equipamento_dvd as int64) equipamento_dvd,
            safe_cast(quantidade_equipamento_dvd as int64) quantidade_equipamento_dvd,
            safe_cast(equipamento_som as int64) equipamento_som,
            safe_cast(quantidade_equipamento_som as int64) quantidade_equipamento_som,
            safe_cast(equipamento_tv as int64) equipamento_tv,
            safe_cast(quantidade_equipamento_tv as int64) quantidade_equipamento_tv,
            safe_cast(equipamento_lousa_digital as int64) equipamento_lousa_digital,
            safe_cast(
                quantidade_equipamento_lousa_digital as int64
            ) quantidade_equipamento_lousa_digital,
            safe_cast(equipamento_multimidia as int64) equipamento_multimidia,
            safe_cast(
                quantidade_equipamento_multimidia as int64
            ) quantidade_equipamento_multimidia,
            safe_cast(equipamento_videocassete as int64) equipamento_videocassete,
            safe_cast(
                quantidade_equipamento_videocassete as int64
            ) quantidade_equipamento_videocassete,
            safe_cast(equipamento_retroprojetor as int64) equipamento_retroprojetor,
            safe_cast(
                quantidade_equipamento_retroprojetor as int64
            ) quantidade_equipamento_retroprojetor,
            safe_cast(equipamento_fax as int64) equipamento_fax,
            safe_cast(quantidade_equipamento_fax as int64) quantidade_equipamento_fax,
            safe_cast(equipamento_foto as int64) equipamento_foto,
            safe_cast(quantidade_equipamento_foto as int64) quantidade_equipamento_foto,
            safe_cast(quantidade_computador_aluno as int64) quantidade_computador_aluno,
            safe_cast(desktop_aluno as int64) desktop_aluno,
            safe_cast(quantidade_desktop_aluno as int64) quantidade_desktop_aluno,
            safe_cast(computador_portatil_aluno as int64) computador_portatil_aluno,
            safe_cast(
                quantidade_computador_portatil_aluno as int64
            ) quantidade_computador_portatil_aluno,
            safe_cast(tablet_aluno as int64) tablet_aluno,
            safe_cast(quantidade_tablet_aluno as int64) quantidade_tablet_aluno,
            safe_cast(quantidade_computador as int64) quantidade_computador,
            safe_cast(
                quantidade_computador_administrativo as int64
            ) quantidade_computador_administrativo,
            safe_cast(internet as int64) internet,
            safe_cast(internet_alunos as int64) internet_alunos,
            safe_cast(internet_administrativo as int64) internet_administrativo,
            safe_cast(internet_aprendizagem as int64) internet_aprendizagem,
            safe_cast(internet_comunidade as int64) internet_comunidade,
            safe_cast(acesso_internet_computador as int64) acesso_internet_computador,
            safe_cast(
                acesso_internet_dispositivo_pessoal as int64
            ) acesso_internet_dispositivo_pessoal,
            safe_cast(tipo_rede_local as string) tipo_rede_local,
            safe_cast(banda_larga as int64) banda_larga,
            safe_cast(quantidade_funcionario as int64) quantidade_funcionario,
            safe_cast(profissional_administrativo as int64) profissional_administrativo,
            safe_cast(
                quantidade_profissional_administrativo as int64
            ) quantidade_profissional_administrativo,
            safe_cast(profissional_servico_geral as int64) profissional_servico_geral,
            safe_cast(
                quantidade_profissional_servico_geral as int64
            ) quantidade_profissional_servico_geral,
            safe_cast(profissional_bibliotecario as int64) profissional_bibliotecario,
            safe_cast(
                quantidade_profissional_bibliotecario as int64
            ) quantidade_profissional_bibliotecario,
            safe_cast(profissional_saude as int64) profissional_saude,
            safe_cast(
                quantidade_profissional_saude as int64
            ) quantidade_profissional_saude,
            safe_cast(profissional_coordenador as int64) profissional_coordenador,
            safe_cast(
                quantidade_profissional_coordenador as int64
            ) quantidade_profissional_coordenador,
            safe_cast(profissional_fonaudiologo as int64) profissional_fonaudiologo,
            safe_cast(
                quantidade_profissional_fonaudiologo as int64
            ) quantidade_profissional_fonaudiologo,
            safe_cast(profissional_nutricionista as int64) profissional_nutricionista,
            safe_cast(
                quantidade_profissional_nutricionista as int64
            ) quantidade_profissional_nutricionista,
            safe_cast(profissional_psicologo as int64) profissional_psicologo,
            safe_cast(
                quantidade_profissional_psicologo as int64
            ) quantidade_profissional_psicologo,
            safe_cast(profissional_alimentacao as int64) profissional_alimentacao,
            safe_cast(
                quantidade_profissional_alimentacao as int64
            ) quantidade_profissional_alimentacao,
            safe_cast(profissional_pedagogia as int64) profissional_pedagogia,
            safe_cast(
                quantidade_profissional_pedagogia as int64
            ) quantidade_profissional_pedagogia,
            safe_cast(profissional_secretario as int64) profissional_secretario,
            safe_cast(
                quantidade_profissional_secretario as int64
            ) quantidade_profissional_secretario,
            safe_cast(profissional_seguranca as int64) profissional_seguranca,
            safe_cast(
                quantidade_profissional_seguranca as int64
            ) quantidade_profissional_seguranca,
            safe_cast(profissional_monitor as int64) profissional_monitor,
            safe_cast(
                quantidade_profissional_monitor as int64
            ) quantidade_profissional_monitor,
            safe_cast(profissional_gestao as int64) profissional_gestao,
            safe_cast(
                quantidade_profissional_gestao as int64
            ) quantidade_profissional_gestao,
            safe_cast(
                profissional_assistente_social as int64
            ) profissional_assistente_social,
            safe_cast(
                quantidade_profissional_assistente_social as int64
            ) quantidade_profissional_assistente_social,
            safe_cast(
                profissional_tradutor_libras as int64
            ) profissional_tradutor_libras,
            safe_cast(
                quantidade_profissional_tradutor_libras as int64
            ) quantidade_profissional_tradutor_libras,
            safe_cast(alimentacao as int64) alimentacao,
            safe_cast(organizacao_serie_ano as int64) organizacao_serie_ano,
            safe_cast(organizacao_semestre as int64) organizacao_semestre,
            safe_cast(
                organizacao_fundamental_ciclo as int64
            ) organizacao_fundamental_ciclo,
            safe_cast(
                organizacao_grupo_nao_seriado as int64
            ) organizacao_grupo_nao_seriado,
            safe_cast(organizacao_modulo as int64) organizacao_modulo,
            safe_cast(organizacao_alternancia as int64) organizacao_alternancia,
            safe_cast(
                material_pedagogico_multimidia as int64
            ) material_pedagogico_multimidia,
            safe_cast(
                material_pedagogico_infantil as int64
            ) material_pedagogico_infantil,
            safe_cast(
                material_pedagogico_cientifico as int64
            ) material_pedagogico_cientifico,
            safe_cast(material_pedagogico_difusao as int64) material_pedagogico_difusao,
            safe_cast(material_pedagogico_musical as int64) material_pedagogico_musical,
            safe_cast(material_pedagogico_jogo as int64) material_pedagogico_jogo,
            safe_cast(
                material_pedagogico_artistica as int64
            ) material_pedagogico_artistica,
            safe_cast(
                material_pedagogico_profissional as int64
            ) material_pedagogico_profissional,
            safe_cast(
                material_pedagogico_desportiva as int64
            ) material_pedagogico_desportiva,
            safe_cast(
                material_pedagogico_indigena as int64
            ) material_pedagogico_indigena,
            safe_cast(material_pedagogico_etnico as int64) material_pedagogico_etnico,
            safe_cast(material_pedagogico_campo as int64) material_pedagogico_campo,
            safe_cast(material_pedagogico_surdo as int64) material_pedagogico_surdo,
            safe_cast(material_pedagogico_nenhum as int64) material_pedagogico_nenhum,
            safe_cast(
                material_especifico_quilombola as int64
            ) material_especifico_quilombola,
            safe_cast(
                material_especifico_indigena as int64
            ) material_especifico_indigena,
            safe_cast(
                material_especifico_nao_utiliza as int64
            ) material_especifico_nao_utiliza,
            safe_cast(educacao_indigena as int64) educacao_indigena,
            safe_cast(tipo_lingua_indigena as string) tipo_lingua_indigena,
            safe_cast(id_lingua_indigena_1 as string) id_lingua_indigena_1,
            safe_cast(id_lingua_indigena_2 as string) id_lingua_indigena_2,
            safe_cast(id_lingua_indigena_3 as string) id_lingua_indigena_3,
            safe_cast(
                programa_brasil_alfabetizado as int64
            ) programa_brasil_alfabetizado,
            safe_cast(final_semana as int64) final_semana,
            safe_cast(exame_selecao as int64) exame_selecao,
            safe_cast(reserva_vaga_raca_cor as int64) reserva_vaga_raca_cor,
            safe_cast(reserva_vaga_renda as int64) reserva_vaga_renda,
            safe_cast(reserva_vaga_publica as int64) reserva_vaga_publica,
            safe_cast(reserva_vaga_pcd as int64) reserva_vaga_pcd,
            safe_cast(reserva_vaga_outros as int64) reserva_vaga_outros,
            safe_cast(reserva_vaga_nenhuma as int64) reserva_vaga_nenhuma,
            safe_cast(redes_sociais as int64) redes_sociais,
            safe_cast(espaco_atividade_comunidade as int64) espaco_atividade_comunidade,
            safe_cast(espaco_equipamento_alunos as int64) espaco_equipamento_alunos,
            safe_cast(orgao_associacao_pais as int64) orgao_associacao_pais,
            safe_cast(
                orgao_associacao_pais_mestres as int64
            ) orgao_associacao_pais_mestres,
            safe_cast(orgao_conselho_escolar as int64) orgao_conselho_escolar,
            safe_cast(orgao_gremio_estudantil as int64) orgao_gremio_estudantil,
            safe_cast(orgao_outros as int64) orgao_outros,
            safe_cast(orgao_nenhum as int64) orgao_nenhum,
            safe_cast(tipo_proposta_pedagogica as string) tipo_proposta_pedagogica,
            safe_cast(tipo_aee as string) tipo_aee,
            safe_cast(
                tipo_atividade_complementar as string
            ) tipo_atividade_complementar,
            safe_cast(escolarizacao as int64) escolarizacao,
            safe_cast(mediacao_presencial as int64) mediacao_presencial,
            safe_cast(mediacao_semipresencial as int64) mediacao_semipresencial,
            safe_cast(mediacao_ead as int64) mediacao_ead,
            safe_cast(regular as int64) regular,
            safe_cast(diurno as int64) diurno,
            safe_cast(noturno as int64) noturno,
            safe_cast(ead as int64) ead,
            safe_cast(educacao_basica as int64) educacao_basica,
            safe_cast(etapa_ensino_infantil as int64) etapa_ensino_infantil,
            safe_cast(
                etapa_ensino_infantil_creche as int64
            ) etapa_ensino_infantil_creche,
            safe_cast(
                etapa_ensino_infantil_pre_escola as int64
            ) etapa_ensino_infantil_pre_escola,
            safe_cast(etapa_ensino_fundamental as int64) etapa_ensino_fundamental,
            safe_cast(
                etapa_ensino_fundamental_anos_iniciais as int64
            ) etapa_ensino_fundamental_anos_iniciais,
            safe_cast(
                etapa_ensino_fundamental_anos_finais as int64
            ) etapa_ensino_fundamental_anos_finais,
            safe_cast(etapa_ensino_medio as int64) etapa_ensino_medio,
            safe_cast(etapa_ensino_profissional as int64) etapa_ensino_profissional,
            safe_cast(
                etapa_ensino_profissional_tecnica as int64
            ) etapa_ensino_profissional_tecnica,
            safe_cast(etapa_ensino_eja as int64) etapa_ensino_eja,
            safe_cast(
                etapa_ensino_eja_fundamental as int64
            ) etapa_ensino_eja_fundamental,
            safe_cast(etapa_ensino_eja_medio as int64) etapa_ensino_eja_medio,
            safe_cast(etapa_ensino_especial as int64) etapa_ensino_especial,
            safe_cast(etapa_ensino_especial_comum as int64) etapa_ensino_especial_comum,
            safe_cast(
                etapa_ensino_especial_exclusiva as int64
            ) etapa_ensino_especial_exclusiva,
            safe_cast(etapa_ensino_creche_comum as int64) etapa_ensino_creche_comum,
            safe_cast(
                etapa_ensino_pre_escola_comum as int64
            ) etapa_ensino_pre_escola_comum,
            safe_cast(
                etapa_ensino_fundamental_anos_iniciais_comum as int64
            ) etapa_ensino_fundamental_anos_iniciais_comum,
            safe_cast(
                etapa_ensino_fundamental_anos_finais_comum as int64
            ) etapa_ensino_fundamental_anos_finais_comum,
            safe_cast(etapa_ensino_medio_comum as int64) etapa_ensino_medio_comum,
            safe_cast(
                etapa_ensino_medio_integrado_comum as int64
            ) etapa_ensino_medio_integrado_comum,
            safe_cast(
                etapa_ensino_medio_normal_comum as int64
            ) etapa_ensino_medio_normal_comum,
            safe_cast(
                etapa_ensino_profissional_comum as int64
            ) etapa_ensino_profissional_comum,
            safe_cast(
                etapa_ensino_eja_fundamental_comum as int64
            ) etapa_ensino_eja_fundamental_comum,
            safe_cast(
                etapa_ensino_eja_medio_comum as int64
            ) etapa_ensino_eja_medio_comum,
            safe_cast(
                etapa_ensino_eja_profissional_comum as int64
            ) etapa_ensino_eja_profissional_comum,
            safe_cast(
                etapa_ensino_creche_especial_exclusiva as int64
            ) etapa_ensino_creche_especial_exclusiva,
            safe_cast(
                etapa_ensino_pre_escola_especial_exclusiva as int64
            ) etapa_ensino_pre_escola_especial_exclusiva,
            safe_cast(
                etapa_ensino_fundamental_anos_iniciais_especial_exclusiva as int64
            ) etapa_ensino_fundamental_anos_iniciais_especial_exclusiva,
            safe_cast(
                etapa_ensino_fundamental_anos_finais_especial_exclusiva as int64
            ) etapa_ensino_fundamental_anos_finais_especial_exclusiva,
            safe_cast(
                etapa_ensino_medio_especial_exclusiva as int64
            ) etapa_ensino_medio_especial_exclusiva,
            safe_cast(
                etapa_ensino_medio_integrado_especial_exclusiva as int64
            ) etapa_ensino_medio_integrado_especial_exclusiva,
            safe_cast(
                etapa_ensino_medio_normal_especial_exclusiva as int64
            ) etapa_ensino_medio_normal_especial_exclusiva,
            safe_cast(
                etapa_ensino_profissional_especial_exclusiva as int64
            ) etapa_ensino_profissional_especial_exclusiva,
            safe_cast(
                etapa_ensino_eja_fundamental_especial_exclusiva as int64
            ) etapa_ensino_eja_fundamental_especial_exclusiva,
            safe_cast(
                etapa_ensino_eja_medio_especial_exclusiva as int64
            ) etapa_ensino_eja_medio_especial_exclusiva,
            safe_cast(
                quantidade_matricula_educacao_basica as int64
            ) quantidade_matricula_educacao_basica,
            safe_cast(
                quantidade_matricula_infantil as int64
            ) quantidade_matricula_infantil,
            safe_cast(
                quantidade_matricula_infantil_creche as int64
            ) quantidade_matricula_infantil_creche,
            safe_cast(
                quantidade_matricula_infantil_pre_escola as int64
            ) quantidade_matricula_infantil_pre_escola,
            safe_cast(
                quantidade_matricula_fundamental as int64
            ) quantidade_matricula_fundamental,
            safe_cast(
                quantidade_matricula_fundamental_anos_iniciais as int64
            ) quantidade_matricula_fundamental_anos_iniciais,
            safe_cast(
                quantidade_matricula_fundamental_1_ano as int64
            ) quantidade_matricula_fundamental_1_ano,
            safe_cast(
                quantidade_matricula_fundamental_2_ano as int64
            ) quantidade_matricula_fundamental_2_ano,
            safe_cast(
                quantidade_matricula_fundamental_3_ano as int64
            ) quantidade_matricula_fundamental_3_ano,
            safe_cast(
                quantidade_matricula_fundamental_4_ano as int64
            ) quantidade_matricula_fundamental_4_ano,
            safe_cast(
                quantidade_matricula_fundamental_5_ano as int64
            ) quantidade_matricula_fundamental_5_ano,
            safe_cast(
                quantidade_matricula_fundamental_anos_finais as int64
            ) quantidade_matricula_fundamental_anos_finais,
            safe_cast(
                quantidade_matricula_fundamental_6_ano as int64
            ) quantidade_matricula_fundamental_6_ano,
            safe_cast(
                quantidade_matricula_fundamental_7_ano as int64
            ) quantidade_matricula_fundamental_7_ano,
            safe_cast(
                quantidade_matricula_fundamental_8_ano as int64
            ) quantidade_matricula_fundamental_8_ano,
            safe_cast(
                quantidade_matricula_fundamental_9_ano as int64
            ) quantidade_matricula_fundamental_9_ano,
            safe_cast(quantidade_matricula_medio as int64) quantidade_matricula_medio,
            safe_cast(
                quantidade_matricula_medio_propedeutico as int64
            ) quantidade_matricula_medio_propedeutico,
            safe_cast(
                quantidade_matricula_medio_propedeutico_1_ano as int64
            ) quantidade_matricula_medio_propedeutico_1_ano,
            safe_cast(
                quantidade_matricula_medio_propedeutico_2_ano as int64
            ) quantidade_matricula_medio_propedeutico_2_ano,
            safe_cast(
                quantidade_matricula_medio_propedeutico_3_ano as int64
            ) quantidade_matricula_medio_propedeutico_3_ano,
            safe_cast(
                quantidade_matricula_medio_propedeutico_4_ano as int64
            ) quantidade_matricula_medio_propedeutico_4_ano,
            safe_cast(
                quantidade_matricula_medio_propedeutico_nao_seriado as int64
            ) quantidade_matricula_medio_propedeutico_nao_seriado,
            safe_cast(
                quantidade_matricula_medio_tecnico as int64
            ) quantidade_matricula_medio_tecnico,
            safe_cast(
                quantidade_matricula_medio_tecnico_1_ano as int64
            ) quantidade_matricula_medio_tecnico_1_ano,
            safe_cast(
                quantidade_matricula_medio_tecnico_2_ano as int64
            ) quantidade_matricula_medio_tecnico_2_ano,
            safe_cast(
                quantidade_matricula_medio_tecnico_3_ano as int64
            ) quantidade_matricula_medio_tecnico_3_ano,
            safe_cast(
                quantidade_matricula_medio_tecnico_4_ano as int64
            ) quantidade_matricula_medio_tecnico_4_ano,
            safe_cast(
                quantidade_matricula_medio_tecnico_nao_seriado as int64
            ) quantidade_matricula_medio_tecnico_nao_seriado,
            safe_cast(
                quantidade_matricula_medio_magisterio as int64
            ) quantidade_matricula_medio_magisterio,
            safe_cast(
                quantidade_matricula_medio_magisterio_1_ano as int64
            ) quantidade_matricula_medio_magisterio_1_ano,
            safe_cast(
                quantidade_matricula_medio_magisterio_2_ano as int64
            ) quantidade_matricula_medio_magisterio_2_ano,
            safe_cast(
                quantidade_matricula_medio_magisterio_3_ano as int64
            ) quantidade_matricula_medio_magisterio_3_ano,
            safe_cast(
                quantidade_matricula_medio_magisterio_4_ano as int64
            ) quantidade_matricula_medio_magisterio_4_ano,
            safe_cast(
                quantidade_matricula_profissional as int64
            ) quantidade_matricula_profissional,
            safe_cast(
                quantidade_matricula_profissional_tecnica as int64
            ) quantidade_matricula_profissional_tecnica,
            safe_cast(
                quantidade_matricula_profissional_tecnica_concomitante as int64
            ) quantidade_matricula_profissional_tecnica_concomitante,
            safe_cast(
                quantidade_matricula_profissional_tecnica_subsequente as int64
            ) quantidade_matricula_profissional_tecnica_subsequente,
            safe_cast(
                quantidade_matricula_profissional_fic_concomitante as int64
            ) quantidade_matricula_profissional_fic_concomitante,
            safe_cast(quantidade_matricula_eja as int64) quantidade_matricula_eja,
            safe_cast(
                quantidade_matricula_eja_fundamental as int64
            ) quantidade_matricula_eja_fundamental,
            safe_cast(
                quantidade_matricula_eja_fundamental_anos_iniciais as int64
            ) quantidade_matricula_eja_fundamental_anos_iniciais,
            safe_cast(
                quantidade_matricula_eja_fundamental_anos_finais as int64
            ) quantidade_matricula_eja_fundamental_anos_finais,
            safe_cast(
                quantidade_matricula_eja_fundamental_fic as int64
            ) quantidade_matricula_eja_fundamental_fic,
            safe_cast(
                quantidade_matricula_eja_medio as int64
            ) quantidade_matricula_eja_medio,
            safe_cast(
                quantidade_matricula_eja_medio_nao_profissionalizante as int64
            ) quantidade_matricula_eja_medio_nao_profissionalizante,
            safe_cast(
                quantidade_matricula_eja_medio_fic as int64
            ) quantidade_matricula_eja_medio_fic,
            safe_cast(
                quantidade_matricula_eja_medio_tecnico as int64
            ) quantidade_matricula_eja_medio_tecnico,
            safe_cast(
                quantidade_matricula_especial as int64
            ) quantidade_matricula_especial,
            safe_cast(
                quantidade_matricula_especial_comum as int64
            ) quantidade_matricula_especial_comum,
            safe_cast(
                quantidade_matricula_especial_exclusiva as int64
            ) quantidade_matricula_especial_exclusiva,
            safe_cast(
                quantidade_matricula_feminino as int64
            ) quantidade_matricula_feminino,
            safe_cast(
                quantidade_matricula_masculino as int64
            ) quantidade_matricula_masculino,
            safe_cast(
                quantidade_matricula_nao_declarada as int64
            ) quantidade_matricula_nao_declarada,
            safe_cast(quantidade_matricula_branca as int64) quantidade_matricula_branca,
            safe_cast(quantidade_matricula_preta as int64) quantidade_matricula_preta,
            safe_cast(quantidade_matricula_parda as int64) quantidade_matricula_parda,
            safe_cast(
                quantidade_matricula_amarela as int64
            ) quantidade_matricula_amarela,
            safe_cast(
                quantidade_matricula_indigena as int64
            ) quantidade_matricula_indigena,
            safe_cast(
                quantidade_matricula_idade_0_3 as int64
            ) quantidade_matricula_idade_0_3,
            safe_cast(
                quantidade_matricula_idade_4_5 as int64
            ) quantidade_matricula_idade_4_5,
            safe_cast(
                quantidade_matricula_idade_6_10 as int64
            ) quantidade_matricula_idade_6_10,
            safe_cast(
                quantidade_matricula_idade_11_14 as int64
            ) quantidade_matricula_idade_11_14,
            safe_cast(
                quantidade_matricula_idade_15_17 as int64
            ) quantidade_matricula_idade_15_17,
            safe_cast(
                quantidade_matricula_idade_18 as int64
            ) quantidade_matricula_idade_18,
            safe_cast(quantidade_matricula_diurno as int64) quantidade_matricula_diurno,
            safe_cast(
                quantidade_matricula_noturno as int64
            ) quantidade_matricula_noturno,
            safe_cast(quantidade_matricula_ead as int64) quantidade_matricula_ead,
            safe_cast(
                quantidade_matricula_infantil_integral as int64
            ) quantidade_matricula_infantil_integral,
            safe_cast(
                quantidade_matricula_infantil_creche_integral as int64
            ) quantidade_matricula_infantil_creche_integral,
            safe_cast(
                quantidade_matricula_infantil_pre_escola_integral as int64
            ) quantidade_matricula_infantil_pre_escola_integral,
            safe_cast(
                quantidade_matricula_fundamental_integral as int64
            ) quantidade_matricula_fundamental_integral,
            safe_cast(
                quantidade_matricula_fundamental_anos_iniciais_integral as int64
            ) quantidade_matricula_fundamental_anos_iniciais_integral,
            safe_cast(
                quantidade_matricula_fundamental_anos_finais_integral as int64
            ) quantidade_matricula_fundamental_anos_finais_integral,
            safe_cast(
                quantidade_matricula_medio_integral as int64
            ) quantidade_matricula_medio_integral,
            safe_cast(
                quantidade_matricula_zona_residencia_urbana as int64
            ) quantidade_matricula_zona_residencia_urbana,
            safe_cast(
                quantidade_matricula_zona_residencia_rural as int64
            ) quantidade_matricula_zona_residencia_rural,
            safe_cast(
                quantidade_matricula_zona_residencia_nao_aplicavel as int64
            ) quantidade_matricula_zona_residencia_nao_aplicavel,
            safe_cast(
                quantidade_matricula_utiliza_transporte_publico as int64
            ) quantidade_matricula_utiliza_transporte_publico,
            safe_cast(
                quantidade_matricula_transporte_estadual as int64
            ) quantidade_matricula_transporte_estadual,
            safe_cast(
                quantidade_matricula_transporte_municipal as int64
            ) quantidade_matricula_transporte_municipal,
            safe_cast(
                quantidade_docente_educacao_basica as int64
            ) quantidade_docente_educacao_basica,
            safe_cast(quantidade_docente_infantil as int64) quantidade_docente_infantil,
            safe_cast(
                quantidade_docente_infantil_creche as int64
            ) quantidade_docente_infantil_creche,
            safe_cast(
                quantidade_docente_infantil_pre_escola as int64
            ) quantidade_docente_infantil_pre_escola,
            safe_cast(
                quantidade_docente_fundamental as int64
            ) quantidade_docente_fundamental,
            safe_cast(
                quantidade_docente_fundamental_anos_iniciais as int64
            ) quantidade_docente_fundamental_anos_iniciais,
            safe_cast(
                quantidade_docente_fundamental_anos_finais as int64
            ) quantidade_docente_fundamental_anos_finais,
            safe_cast(quantidade_docente_medio as int64) quantidade_docente_medio,
            safe_cast(
                quantidade_docente_profissional as int64
            ) quantidade_docente_profissional,
            safe_cast(
                quantidade_docente_profissional_tecnica as int64
            ) quantidade_docente_profissional_tecnica,
            safe_cast(quantidade_docente_eja as int64) quantidade_docente_eja,
            safe_cast(
                quantidade_docente_eja_fundamental as int64
            ) quantidade_docente_eja_fundamental,
            safe_cast(
                quantidade_docente_eja_medio as int64
            ) quantidade_docente_eja_medio,
            safe_cast(quantidade_docente_especial as int64) quantidade_docente_especial,
            safe_cast(
                quantidade_docente_especial_comum as int64
            ) quantidade_docente_especial_comum,
            safe_cast(
                quantidade_docente_especial_exclusiva as int64
            ) quantidade_docente_especial_exclusiva,
            safe_cast(
                quantidade_turma_educacao_basica as int64
            ) quantidade_turma_educacao_basica,
            safe_cast(quantidade_turma_infantil as int64) quantidade_turma_infantil,
            safe_cast(
                quantidade_turma_infantil_integral as int64
            ) quantidade_turma_infantil_integral,
            safe_cast(
                quantidade_turma_infantil_creche as int64
            ) quantidade_turma_infantil_creche,
            safe_cast(
                quantidade_turma_infantil_creche_integral as int64
            ) quantidade_turma_infantil_creche_integral,
            safe_cast(
                quantidade_turma_infantil_pre_escola as int64
            ) quantidade_turma_infantil_pre_escola,
            safe_cast(
                quantidade_turma_infantil_pre_escola_integral as int64
            ) quantidade_turma_infantil_pre_escola_integral,
            safe_cast(
                quantidade_turma_fundamental as int64
            ) quantidade_turma_fundamental,
            safe_cast(
                quantidade_turma_fundamental_integral as int64
            ) quantidade_turma_fundamental_integral,
            safe_cast(
                quantidade_turma_fundamental_anos_iniciais as int64
            ) quantidade_turma_fundamental_anos_iniciais,
            safe_cast(
                quantidade_turma_fundamental_anos_iniciais_integral as int64
            ) quantidade_turma_fundamental_anos_iniciais_integral,
            safe_cast(
                quantidade_turma_fundamental_anos_finais as int64
            ) quantidade_turma_fundamental_anos_finais,
            safe_cast(
                quantidade_turma_fundamental_anos_finais_integral as int64
            ) quantidade_turma_fundamental_anos_finais_integral,
            safe_cast(quantidade_turma_medio as int64) quantidade_turma_medio,
            safe_cast(
                quantidade_turma_medio_integral as int64
            ) quantidade_turma_medio_integral,
            safe_cast(
                quantidade_turma_profissional as int64
            ) quantidade_turma_profissional,
            safe_cast(
                quantidade_turma_profissional_tecnica as int64
            ) quantidade_turma_profissional_tecnica,
            safe_cast(quantidade_turma_eja as int64) quantidade_turma_eja,
            safe_cast(
                quantidade_turma_eja_fundamental as int64
            ) quantidade_turma_eja_fundamental,
            safe_cast(quantidade_turma_eja_medio as int64) quantidade_turma_eja_medio,
            safe_cast(quantidade_turma_especial as int64) quantidade_turma_especial,
            safe_cast(
                quantidade_turma_especial_comum as int64
            ) quantidade_turma_especial_comum,
            safe_cast(
                quantidade_turma_especial_exclusiva as int64
            ) quantidade_turma_especial_exclusiva,
            safe_cast(quantidade_turma_diurno as int64) quantidade_turma_diurno,
            safe_cast(quantidade_turma_noturno as int64) quantidade_turma_noturno,
            safe_cast(quantidade_turma_ead as int64) quantidade_turma_ead,
        from `basedosdados-staging.br_inep_censo_escolar_staging.escola_2023`
    ),
    censo_2024 as (
        select
            safe_cast(ano as int64) ano,
            safe_cast(sigla_uf as string) sigla_uf,
            safe_cast(id_municipio as string) id_municipio,
            safe_cast(id_escola as string) id_escola,
            safe_cast(rede as string) rede,
            safe_cast(
                tipo_categoria_escola_privada as string
            ) tipo_categoria_escola_privada,
            safe_cast(tipo_localizacao as string) tipo_localizacao,
            safe_cast(
                tipo_localizacao_diferenciada as string
            ) tipo_localizacao_diferenciada,
            safe_cast(
                tipo_situacao_funcionamento as string
            ) tipo_situacao_funcionamento,
            safe_cast(id_orgao_regional as string) id_orgao_regional,
            safe_cast(data_ano_letivo_inicio as date) data_ano_letivo_inicio,
            safe_cast(data_ano_letivo_termino as date) data_ano_letivo_termino,
            safe_cast(vinculo_secretaria_educacao as int64) vinculo_secretaria_educacao,
            safe_cast(vinculo_seguranca_publica as int64) vinculo_seguranca_publica,
            safe_cast(vinculo_secretaria_saude as int64) vinculo_secretaria_saude,
            safe_cast(vinculo_outro_orgao as int64) vinculo_outro_orgao,
            safe_cast(poder_publico_parceria as int64) poder_publico_parceria,
            safe_cast(
                tipo_poder_publico_parceria as string
            ) tipo_poder_publico_parceria,
            safe_cast(conveniada_poder_publico as int64) conveniada_poder_publico,
            safe_cast(
                tipo_convenio_poder_publico as string
            ) tipo_convenio_poder_publico,
            safe_cast(
                forma_contratacao_termo_colaboracao as int64
            ) forma_contratacao_termo_colaboracao,
            safe_cast(
                forma_contratacao_termo_fomento as int64
            ) forma_contratacao_termo_fomento,
            safe_cast(
                forma_contratacao_acordo_cooperacao as int64
            ) forma_contratacao_acordo_cooperacao,
            safe_cast(
                forma_contratacao_prestacao_servico as int64
            ) forma_contratacao_prestacao_servico,
            safe_cast(
                forma_contratacao_cooperacao_tecnica_financeira as int64
            ) forma_contratacao_cooperacao_tecnica_financeira,
            safe_cast(
                forma_contratacao_consorcio_publico as int64
            ) forma_contratacao_consorcio_publico,
            safe_cast(
                forma_contratacao_parceria_municipal_termo_colaboracao as int64
            ) forma_contratacao_parceria_municipal_termo_colaboracao,
            safe_cast(
                forma_contratacao_parceria_municipal_termo_fomento as int64
            ) forma_contratacao_parceria_municipal_termo_fomento,
            safe_cast(
                forma_contratacao_parceria_municipal_acordo_cooperacao as int64
            ) forma_contratacao_parceria_municipal_acordo_cooperacao,
            safe_cast(
                forma_contratacao_parceria_municipal_prestacao_servico as int64
            ) forma_contratacao_parceria_municipal_prestacao_servico,
            safe_cast(
                forma_contratacao_parceria_municipal_cooperacao_tecnica_financeira
                as int64
            ) forma_contratacao_parceria_municipal_cooperacao_tecnica_financeira,
            safe_cast(
                forma_contratacao_parceria_municipal_consorcio_publico as int64
            ) forma_contratacao_parceria_municipal_consorcio_publico,
            safe_cast(
                forma_contratacao_parceria_estadual_termo_colaboracao as int64
            ) forma_contratacao_parceria_estadual_termo_colaboracao,
            safe_cast(
                forma_contratacao_parceria_estadual_termo_fomento as int64
            ) forma_contratacao_parceria_estadual_termo_fomento,
            safe_cast(
                forma_contratacao_parceria_estadual_acordo_cooperacao as int64
            ) forma_contratacao_parceria_estadual_acordo_cooperacao,
            safe_cast(
                forma_contratacao_parceria_estadual_prestacao_servico as int64
            ) forma_contratacao_parceria_estadual_prestacao_servico,
            safe_cast(
                forma_contratacao_parceria_estadual_cooperacao_tecnica_financeira
                as int64
            ) forma_contratacao_parceria_estadual_cooperacao_tecnica_financeira,
            safe_cast(
                forma_contratacao_parceria_estadual_consorcio_publico as int64
            ) forma_contratacao_parceria_estadual_consorcio_publico,
            safe_cast(
                tipo_atendimento_escolarizacao as int64
            ) tipo_atendimento_escolarizacao,
            safe_cast(
                tipo_atendimento_atividade_complementar as int64
            ) tipo_atendimento_atividade_complementar,
            safe_cast(tipo_atendimento_aee as int64) tipo_atendimento_aee,
            safe_cast(
                mantenedora_escola_privada_empresa as int64
            ) mantenedora_escola_privada_empresa,
            safe_cast(
                mantenedora_escola_privada_ong as int64
            ) mantenedora_escola_privada_ong,
            safe_cast(
                mantenedora_escola_privada_oscip as int64
            ) mantenedora_escola_privada_oscip,
            safe_cast(
                mantenedora_escola_privada_ong_oscip as int64
            ) mantenedora_escola_privada_ong_oscip,
            safe_cast(
                mantenedora_escola_privada_sindicato as int64
            ) mantenedora_escola_privada_sindicato,
            safe_cast(
                mantenedora_escola_privada_sistema_s as int64
            ) mantenedora_escola_privada_sistema_s,
            safe_cast(
                mantenedora_escola_privada_sem_fins as int64
            ) mantenedora_escola_privada_sem_fins,
            safe_cast(cnpj_escola_privada as string) cnpj_escola_privada,
            safe_cast(cnpj_mantenedora as string) cnpj_mantenedora,
            safe_cast(tipo_regulamentacao as string) tipo_regulamentacao,
            safe_cast(
                tipo_responsavel_regulamentacao as string
            ) tipo_responsavel_regulamentacao,
            safe_cast(id_escola_sede as string) id_escola_sede,
            safe_cast(id_ies_ofertante as string) id_ies_ofertante,
            safe_cast(
                local_funcionamento_predio_escolar as int64
            ) local_funcionamento_predio_escolar,
            safe_cast(
                tipo_local_funcionamento_predio_escolar as string
            ) tipo_local_funcionamento_predio_escolar,
            safe_cast(
                local_funcionamento_sala_empresa as int64
            ) local_funcionamento_sala_empresa,
            safe_cast(
                local_funcionamento_socioeducativo as int64
            ) local_funcionamento_socioeducativo,
            safe_cast(
                local_funcionamento_unidade_prisional as int64
            ) local_funcionamento_unidade_prisional,
            safe_cast(
                local_funcionamento_prisional_socio as int64
            ) local_funcionamento_prisional_socio,
            safe_cast(
                local_funcionamento_templo_igreja as int64
            ) local_funcionamento_templo_igreja,
            safe_cast(
                local_funcionamento_casa_professor as int64
            ) local_funcionamento_casa_professor,
            safe_cast(local_funcionamento_galpao as int64) local_funcionamento_galpao,
            safe_cast(
                tipo_local_funcionamento_galpao as string
            ) tipo_local_funcionamento_galpao,
            safe_cast(
                local_funcionamento_outra_escola as int64
            ) local_funcionamento_outra_escola,
            safe_cast(local_funcionamento_outros as int64) local_funcionamento_outros,
            safe_cast(predio_compartilhado as int64) predio_compartilhado,
            safe_cast(agua_filtrada as int64) agua_filtrada,
            safe_cast(agua_potavel as int64) agua_potavel,
            safe_cast(agua_rede_publica as int64) agua_rede_publica,
            safe_cast(agua_poco_artesiano as int64) agua_poco_artesiano,
            safe_cast(agua_cacimba as int64) agua_cacimba,
            safe_cast(agua_fonte_rio as int64) agua_fonte_rio,
            safe_cast(agua_inexistente as int64) agua_inexistente,
            safe_cast(energia_rede_publica as int64) energia_rede_publica,
            safe_cast(energia_gerador as int64) energia_gerador,
            safe_cast(energia_gerador_fossil as int64) energia_gerador_fossil,
            safe_cast(energia_outros as int64) energia_outros,
            safe_cast(energia_renovavel as int64) energia_renovavel,
            safe_cast(energia_inexistente as int64) energia_inexistente,
            safe_cast(esgoto_rede_publica as int64) esgoto_rede_publica,
            safe_cast(esgoto_fossa as int64) esgoto_fossa,
            safe_cast(esgoto_fossa_septica as int64) esgoto_fossa_septica,
            safe_cast(esgoto_fossa_comum as int64) esgoto_fossa_comum,
            safe_cast(esgoto_inexistente as int64) esgoto_inexistente,
            safe_cast(lixo_servico_coleta as int64) lixo_servico_coleta,
            safe_cast(lixo_queima as int64) lixo_queima,
            safe_cast(lixo_enterrado as int64) lixo_enterrado,
            safe_cast(lixo_destino_final_publico as int64) lixo_destino_final_publico,
            safe_cast(lixo_descarta_outra_area as int64) lixo_descarta_outra_area,
            safe_cast(lixo_joga_outra_area as int64) lixo_joga_outra_area,
            safe_cast(lixo_outros as int64) lixo_outros,
            safe_cast(lixo_reciclagem as int64) lixo_reciclagem,
            safe_cast(tratamento_lixo_separacao as int64) tratamento_lixo_separacao,
            safe_cast(
                tratamento_lixo_reutilizacao as int64
            ) tratamento_lixo_reutilizacao,
            safe_cast(tratamento_lixo_reciclagem as int64) tratamento_lixo_reciclagem,
            safe_cast(tratamento_lixo_inexistente as int64) tratamento_lixo_inexistente,
            safe_cast(almoxarifado as int64) almoxarifado,
            safe_cast(area_verde as int64) area_verde,
            safe_cast(auditorio as int64) auditorio,
            safe_cast(banheiro_fora_predio as int64) banheiro_fora_predio,
            safe_cast(banheiro_dentro_predio as int64) banheiro_dentro_predio,
            safe_cast(banheiro as int64) banheiro,
            safe_cast(banheiro_educacao_infantil as int64) banheiro_educacao_infantil,
            safe_cast(banheiro_pne as int64) banheiro_pne,
            safe_cast(banheiro_funcionarios as int64) banheiro_funcionarios,
            safe_cast(banheiro_chuveiro as int64) banheiro_chuveiro,
            safe_cast(bercario as int64) bercario,
            safe_cast(biblioteca as int64) biblioteca,
            safe_cast(biblioteca_sala_leitura as int64) biblioteca_sala_leitura,
            safe_cast(cozinha as int64) cozinha,
            safe_cast(despensa as int64) despensa,
            safe_cast(dormitorio_aluno as int64) dormitorio_aluno,
            safe_cast(dormitorio_professor as int64) dormitorio_professor,
            safe_cast(laboratorio_ciencias as int64) laboratorio_ciencias,
            safe_cast(laboratorio_informatica as int64) laboratorio_informatica,
            safe_cast(
                laboratorio_educacao_profissional as int64
            ) laboratorio_educacao_profissional,
            safe_cast(patio_coberto as int64) patio_coberto,
            safe_cast(patio_descoberto as int64) patio_descoberto,
            safe_cast(parque_infantil as int64) parque_infantil,
            safe_cast(piscina as int64) piscina,
            safe_cast(quadra_esportes as int64) quadra_esportes,
            safe_cast(quadra_esportes_coberta as int64) quadra_esportes_coberta,
            safe_cast(quadra_esportes_descoberta as int64) quadra_esportes_descoberta,
            safe_cast(refeitorio as int64) refeitorio,
            safe_cast(sala_atelie_artes as int64) sala_atelie_artes,
            safe_cast(sala_musica_coral as int64) sala_musica_coral,
            safe_cast(sala_estudio_danca as int64) sala_estudio_danca,
            safe_cast(sala_multiuso as int64) sala_multiuso,
            safe_cast(sala_estudio_gravacao as int64) sala_estudio_gravacao,
            safe_cast(
                sala_oficinas_educacao_profissional as int64
            ) sala_oficinas_educacao_profissional,
            safe_cast(sala_diretoria as int64) sala_diretoria,
            safe_cast(sala_leitura as int64) sala_leitura,
            safe_cast(sala_professor as int64) sala_professor,
            safe_cast(sala_repouso_aluno as int64) sala_repouso_aluno,
            safe_cast(secretaria as int64) secretaria,
            safe_cast(sala_atendimento_especial as int64) sala_atendimento_especial,
            safe_cast(terreirao as int64) terreirao,
            safe_cast(viveiro as int64) viveiro,
            safe_cast(dependencia_pne as int64) dependencia_pne,
            safe_cast(lavanderia as int64) lavanderia,
            safe_cast(dependencia_outras as int64) dependencia_outras,
            safe_cast(acessibilidade_corrimao as int64) acessibilidade_corrimao,
            safe_cast(acessibilidade_elevador as int64) acessibilidade_elevador,
            safe_cast(acessibilidade_pisos_tateis as int64) acessibilidade_pisos_tateis,
            safe_cast(acessibilidade_vao_livre as int64) acessibilidade_vao_livre,
            safe_cast(acessibilidade_rampas as int64) acessibilidade_rampas,
            safe_cast(acessibilidade_sinal_sonoro as int64) acessibilidade_sinal_sonoro,
            safe_cast(acessibilidade_sinal_tatil as int64) acessibilidade_sinal_tatil,
            safe_cast(acessibilidade_sinal_visual as int64) acessibilidade_sinal_visual,
            safe_cast(acessibilidade_inexistente as int64) acessibilidade_inexistente,
            safe_cast(quantidade_sala_existente as int64) quantidade_sala_existente,
            safe_cast(quantidade_sala_utilizada as int64) quantidade_sala_utilizada,
            safe_cast(
                quantidade_sala_utilizada_dentro as int64
            ) quantidade_sala_utilizada_dentro,
            safe_cast(
                quantidade_sala_utilizada_fora as int64
            ) quantidade_sala_utilizada_fora,
            safe_cast(
                quantidade_sala_utilizada_climatizada as int64
            ) quantidade_sala_utilizada_climatizada,
            safe_cast(
                quantidade_sala_utilizada_acessivel as int64
            ) quantidade_sala_utilizada_acessivel,
            safe_cast(equipamento_parabolica as int64) equipamento_parabolica,
            safe_cast(
                quantidade_equipamento_parabolica as int64
            ) quantidade_equipamento_parabolica,
            safe_cast(equipamento_computador as int64) equipamento_computador,
            safe_cast(equipamento_copiadora as int64) equipamento_copiadora,
            safe_cast(
                quantidade_equipamento_copiadora as int64
            ) quantidade_equipamento_copiadora,
            safe_cast(equipamento_impressora as int64) equipamento_impressora,
            safe_cast(
                quantidade_equipamento_impressora as int64
            ) quantidade_equipamento_impressora,
            safe_cast(
                equipamento_impressora_multifuncional as int64
            ) equipamento_impressora_multifuncional,
            safe_cast(
                quantidade_equipamento_impressora_multifuncional as int64
            ) quantidade_equipamento_impressora_multifuncional,
            safe_cast(equipamento_scanner as int64) equipamento_scanner,
            safe_cast(equipamento_nenhum as int64) equipamento_nenhum,
            safe_cast(equipamento_dvd as int64) equipamento_dvd,
            safe_cast(quantidade_equipamento_dvd as int64) quantidade_equipamento_dvd,
            safe_cast(equipamento_som as int64) equipamento_som,
            safe_cast(quantidade_equipamento_som as int64) quantidade_equipamento_som,
            safe_cast(equipamento_tv as int64) equipamento_tv,
            safe_cast(quantidade_equipamento_tv as int64) quantidade_equipamento_tv,
            safe_cast(equipamento_lousa_digital as int64) equipamento_lousa_digital,
            safe_cast(
                quantidade_equipamento_lousa_digital as int64
            ) quantidade_equipamento_lousa_digital,
            safe_cast(equipamento_multimidia as int64) equipamento_multimidia,
            safe_cast(
                quantidade_equipamento_multimidia as int64
            ) quantidade_equipamento_multimidia,
            safe_cast(equipamento_videocassete as int64) equipamento_videocassete,
            safe_cast(
                quantidade_equipamento_videocassete as int64
            ) quantidade_equipamento_videocassete,
            safe_cast(equipamento_retroprojetor as int64) equipamento_retroprojetor,
            safe_cast(
                quantidade_equipamento_retroprojetor as int64
            ) quantidade_equipamento_retroprojetor,
            safe_cast(equipamento_fax as int64) equipamento_fax,
            safe_cast(quantidade_equipamento_fax as int64) quantidade_equipamento_fax,
            safe_cast(equipamento_foto as int64) equipamento_foto,
            safe_cast(quantidade_equipamento_foto as int64) quantidade_equipamento_foto,
            safe_cast(quantidade_computador_aluno as int64) quantidade_computador_aluno,
            safe_cast(desktop_aluno as int64) desktop_aluno,
            safe_cast(quantidade_desktop_aluno as int64) quantidade_desktop_aluno,
            safe_cast(computador_portatil_aluno as int64) computador_portatil_aluno,
            safe_cast(
                quantidade_computador_portatil_aluno as int64
            ) quantidade_computador_portatil_aluno,
            safe_cast(tablet_aluno as int64) tablet_aluno,
            safe_cast(quantidade_tablet_aluno as int64) quantidade_tablet_aluno,
            safe_cast(quantidade_computador as int64) quantidade_computador,
            safe_cast(
                quantidade_computador_administrativo as int64
            ) quantidade_computador_administrativo,
            safe_cast(internet as int64) internet,
            safe_cast(internet_alunos as int64) internet_alunos,
            safe_cast(internet_administrativo as int64) internet_administrativo,
            safe_cast(internet_aprendizagem as int64) internet_aprendizagem,
            safe_cast(internet_comunidade as int64) internet_comunidade,
            safe_cast(acesso_internet_computador as int64) acesso_internet_computador,
            safe_cast(
                acesso_internet_dispositivo_pessoal as int64
            ) acesso_internet_dispositivo_pessoal,
            safe_cast(tipo_rede_local as string) tipo_rede_local,
            safe_cast(banda_larga as int64) banda_larga,
            safe_cast(quantidade_funcionario as int64) quantidade_funcionario,
            safe_cast(profissional_administrativo as int64) profissional_administrativo,
            safe_cast(
                quantidade_profissional_administrativo as int64
            ) quantidade_profissional_administrativo,
            safe_cast(profissional_servico_geral as int64) profissional_servico_geral,
            safe_cast(
                quantidade_profissional_servico_geral as int64
            ) quantidade_profissional_servico_geral,
            safe_cast(profissional_bibliotecario as int64) profissional_bibliotecario,
            safe_cast(
                quantidade_profissional_bibliotecario as int64
            ) quantidade_profissional_bibliotecario,
            safe_cast(profissional_saude as int64) profissional_saude,
            safe_cast(
                quantidade_profissional_saude as int64
            ) quantidade_profissional_saude,
            safe_cast(profissional_coordenador as int64) profissional_coordenador,
            safe_cast(
                quantidade_profissional_coordenador as int64
            ) quantidade_profissional_coordenador,
            safe_cast(profissional_fonaudiologo as int64) profissional_fonaudiologo,
            safe_cast(
                quantidade_profissional_fonaudiologo as int64
            ) quantidade_profissional_fonaudiologo,
            safe_cast(profissional_nutricionista as int64) profissional_nutricionista,
            safe_cast(
                quantidade_profissional_nutricionista as int64
            ) quantidade_profissional_nutricionista,
            safe_cast(profissional_psicologo as int64) profissional_psicologo,
            safe_cast(
                quantidade_profissional_psicologo as int64
            ) quantidade_profissional_psicologo,
            safe_cast(profissional_alimentacao as int64) profissional_alimentacao,
            safe_cast(
                quantidade_profissional_alimentacao as int64
            ) quantidade_profissional_alimentacao,
            safe_cast(profissional_pedagogia as int64) profissional_pedagogia,
            safe_cast(
                quantidade_profissional_pedagogia as int64
            ) quantidade_profissional_pedagogia,
            safe_cast(profissional_secretario as int64) profissional_secretario,
            safe_cast(
                quantidade_profissional_secretario as int64
            ) quantidade_profissional_secretario,
            safe_cast(profissional_seguranca as int64) profissional_seguranca,
            safe_cast(
                quantidade_profissional_seguranca as int64
            ) quantidade_profissional_seguranca,
            safe_cast(profissional_monitor as int64) profissional_monitor,
            safe_cast(
                quantidade_profissional_monitor as int64
            ) quantidade_profissional_monitor,
            safe_cast(profissional_gestao as int64) profissional_gestao,
            safe_cast(
                quantidade_profissional_gestao as int64
            ) quantidade_profissional_gestao,
            safe_cast(
                profissional_assistente_social as int64
            ) profissional_assistente_social,
            safe_cast(
                quantidade_profissional_assistente_social as int64
            ) quantidade_profissional_assistente_social,
            safe_cast(
                profissional_tradutor_libras as int64
            ) profissional_tradutor_libras,
            safe_cast(
                quantidade_profissional_tradutor_libras as int64
            ) quantidade_profissional_tradutor_libras,
            safe_cast(alimentacao as int64) alimentacao,
            safe_cast(organizacao_serie_ano as int64) organizacao_serie_ano,
            safe_cast(organizacao_semestre as int64) organizacao_semestre,
            safe_cast(
                organizacao_fundamental_ciclo as int64
            ) organizacao_fundamental_ciclo,
            safe_cast(
                organizacao_grupo_nao_seriado as int64
            ) organizacao_grupo_nao_seriado,
            safe_cast(organizacao_modulo as int64) organizacao_modulo,
            safe_cast(organizacao_alternancia as int64) organizacao_alternancia,
            safe_cast(
                material_pedagogico_multimidia as int64
            ) material_pedagogico_multimidia,
            safe_cast(
                material_pedagogico_infantil as int64
            ) material_pedagogico_infantil,
            safe_cast(
                material_pedagogico_cientifico as int64
            ) material_pedagogico_cientifico,
            safe_cast(material_pedagogico_difusao as int64) material_pedagogico_difusao,
            safe_cast(material_pedagogico_musical as int64) material_pedagogico_musical,
            safe_cast(material_pedagogico_jogo as int64) material_pedagogico_jogo,
            safe_cast(
                material_pedagogico_artistica as int64
            ) material_pedagogico_artistica,
            safe_cast(
                material_pedagogico_profissional as int64
            ) material_pedagogico_profissional,
            safe_cast(
                material_pedagogico_desportiva as int64
            ) material_pedagogico_desportiva,
            safe_cast(
                material_pedagogico_indigena as int64
            ) material_pedagogico_indigena,
            safe_cast(material_pedagogico_etnico as int64) material_pedagogico_etnico,
            safe_cast(material_pedagogico_campo as int64) material_pedagogico_campo,
            safe_cast(material_pedagogico_surdo as int64) material_pedagogico_surdo,
            safe_cast(material_pedagogico_nenhum as int64) material_pedagogico_nenhum,
            safe_cast(
                material_especifico_quilombola as int64
            ) material_especifico_quilombola,
            safe_cast(
                material_especifico_indigena as int64
            ) material_especifico_indigena,
            safe_cast(
                material_especifico_nao_utiliza as int64
            ) material_especifico_nao_utiliza,
            safe_cast(educacao_indigena as int64) educacao_indigena,
            safe_cast(tipo_lingua_indigena as string) tipo_lingua_indigena,
            safe_cast(id_lingua_indigena_1 as string) id_lingua_indigena_1,
            safe_cast(id_lingua_indigena_2 as string) id_lingua_indigena_2,
            safe_cast(id_lingua_indigena_3 as string) id_lingua_indigena_3,
            safe_cast(
                programa_brasil_alfabetizado as int64
            ) programa_brasil_alfabetizado,
            safe_cast(final_semana as int64) final_semana,
            safe_cast(exame_selecao as int64) exame_selecao,
            safe_cast(reserva_vaga_raca_cor as int64) reserva_vaga_raca_cor,
            safe_cast(reserva_vaga_renda as int64) reserva_vaga_renda,
            safe_cast(reserva_vaga_publica as int64) reserva_vaga_publica,
            safe_cast(reserva_vaga_pcd as int64) reserva_vaga_pcd,
            safe_cast(reserva_vaga_outros as int64) reserva_vaga_outros,
            safe_cast(reserva_vaga_nenhuma as int64) reserva_vaga_nenhuma,
            safe_cast(redes_sociais as int64) redes_sociais,
            safe_cast(espaco_atividade_comunidade as int64) espaco_atividade_comunidade,
            safe_cast(espaco_equipamento_alunos as int64) espaco_equipamento_alunos,
            safe_cast(orgao_associacao_pais as int64) orgao_associacao_pais,
            safe_cast(
                orgao_associacao_pais_mestres as int64
            ) orgao_associacao_pais_mestres,
            safe_cast(orgao_conselho_escolar as int64) orgao_conselho_escolar,
            safe_cast(orgao_gremio_estudantil as int64) orgao_gremio_estudantil,
            safe_cast(orgao_outros as int64) orgao_outros,
            safe_cast(orgao_nenhum as int64) orgao_nenhum,
            safe_cast(tipo_proposta_pedagogica as string) tipo_proposta_pedagogica,
            safe_cast(tipo_aee as string) tipo_aee,
            safe_cast(
                tipo_atividade_complementar as string
            ) tipo_atividade_complementar,
            safe_cast(escolarizacao as int64) escolarizacao,
            safe_cast(mediacao_presencial as int64) mediacao_presencial,
            safe_cast(mediacao_semipresencial as int64) mediacao_semipresencial,
            safe_cast(mediacao_ead as int64) mediacao_ead,
            safe_cast(regular as int64) regular,
            safe_cast(diurno as int64) diurno,
            safe_cast(noturno as int64) noturno,
            safe_cast(ead as int64) ead,
            safe_cast(educacao_basica as int64) educacao_basica,
            safe_cast(etapa_ensino_infantil as int64) etapa_ensino_infantil,
            safe_cast(
                etapa_ensino_infantil_creche as int64
            ) etapa_ensino_infantil_creche,
            safe_cast(
                etapa_ensino_infantil_pre_escola as int64
            ) etapa_ensino_infantil_pre_escola,
            safe_cast(etapa_ensino_fundamental as int64) etapa_ensino_fundamental,
            safe_cast(
                etapa_ensino_fundamental_anos_iniciais as int64
            ) etapa_ensino_fundamental_anos_iniciais,
            safe_cast(
                etapa_ensino_fundamental_anos_finais as int64
            ) etapa_ensino_fundamental_anos_finais,
            safe_cast(etapa_ensino_medio as int64) etapa_ensino_medio,
            safe_cast(etapa_ensino_profissional as int64) etapa_ensino_profissional,
            safe_cast(
                etapa_ensino_profissional_tecnica as int64
            ) etapa_ensino_profissional_tecnica,
            safe_cast(etapa_ensino_eja as int64) etapa_ensino_eja,
            safe_cast(
                etapa_ensino_eja_fundamental as int64
            ) etapa_ensino_eja_fundamental,
            safe_cast(etapa_ensino_eja_medio as int64) etapa_ensino_eja_medio,
            safe_cast(etapa_ensino_especial as int64) etapa_ensino_especial,
            safe_cast(etapa_ensino_especial_comum as int64) etapa_ensino_especial_comum,
            safe_cast(
                etapa_ensino_especial_exclusiva as int64
            ) etapa_ensino_especial_exclusiva,
            safe_cast(etapa_ensino_creche_comum as int64) etapa_ensino_creche_comum,
            safe_cast(
                etapa_ensino_pre_escola_comum as int64
            ) etapa_ensino_pre_escola_comum,
            safe_cast(
                etapa_ensino_fundamental_anos_iniciais_comum as int64
            ) etapa_ensino_fundamental_anos_iniciais_comum,
            safe_cast(
                etapa_ensino_fundamental_anos_finais_comum as int64
            ) etapa_ensino_fundamental_anos_finais_comum,
            safe_cast(etapa_ensino_medio_comum as int64) etapa_ensino_medio_comum,
            safe_cast(
                etapa_ensino_medio_integrado_comum as int64
            ) etapa_ensino_medio_integrado_comum,
            safe_cast(
                etapa_ensino_medio_normal_comum as int64
            ) etapa_ensino_medio_normal_comum,
            safe_cast(
                etapa_ensino_profissional_comum as int64
            ) etapa_ensino_profissional_comum,
            safe_cast(
                etapa_ensino_eja_fundamental_comum as int64
            ) etapa_ensino_eja_fundamental_comum,
            safe_cast(
                etapa_ensino_eja_medio_comum as int64
            ) etapa_ensino_eja_medio_comum,
            safe_cast(
                etapa_ensino_eja_profissional_comum as int64
            ) etapa_ensino_eja_profissional_comum,
            safe_cast(
                etapa_ensino_creche_especial_exclusiva as int64
            ) etapa_ensino_creche_especial_exclusiva,
            safe_cast(
                etapa_ensino_pre_escola_especial_exclusiva as int64
            ) etapa_ensino_pre_escola_especial_exclusiva,
            safe_cast(
                etapa_ensino_fundamental_anos_iniciais_especial_exclusiva as int64
            ) etapa_ensino_fundamental_anos_iniciais_especial_exclusiva,
            safe_cast(
                etapa_ensino_fundamental_anos_finais_especial_exclusiva as int64
            ) etapa_ensino_fundamental_anos_finais_especial_exclusiva,
            safe_cast(
                etapa_ensino_medio_especial_exclusiva as int64
            ) etapa_ensino_medio_especial_exclusiva,
            safe_cast(
                etapa_ensino_medio_integrado_especial_exclusiva as int64
            ) etapa_ensino_medio_integrado_especial_exclusiva,
            safe_cast(
                etapa_ensino_medio_normal_especial_exclusiva as int64
            ) etapa_ensino_medio_normal_especial_exclusiva,
            safe_cast(
                etapa_ensino_profissional_especial_exclusiva as int64
            ) etapa_ensino_profissional_especial_exclusiva,
            safe_cast(
                etapa_ensino_eja_fundamental_especial_exclusiva as int64
            ) etapa_ensino_eja_fundamental_especial_exclusiva,
            safe_cast(
                etapa_ensino_eja_medio_especial_exclusiva as int64
            ) etapa_ensino_eja_medio_especial_exclusiva,
            safe_cast(
                quantidade_matricula_educacao_basica as int64
            ) quantidade_matricula_educacao_basica,
            safe_cast(
                quantidade_matricula_infantil as int64
            ) quantidade_matricula_infantil,
            safe_cast(
                quantidade_matricula_infantil_creche as int64
            ) quantidade_matricula_infantil_creche,
            safe_cast(
                quantidade_matricula_infantil_pre_escola as int64
            ) quantidade_matricula_infantil_pre_escola,
            safe_cast(
                quantidade_matricula_fundamental as int64
            ) quantidade_matricula_fundamental,
            safe_cast(
                quantidade_matricula_fundamental_anos_iniciais as int64
            ) quantidade_matricula_fundamental_anos_iniciais,
            safe_cast(
                quantidade_matricula_fundamental_1_ano as int64
            ) quantidade_matricula_fundamental_1_ano,
            safe_cast(
                quantidade_matricula_fundamental_2_ano as int64
            ) quantidade_matricula_fundamental_2_ano,
            safe_cast(
                quantidade_matricula_fundamental_3_ano as int64
            ) quantidade_matricula_fundamental_3_ano,
            safe_cast(
                quantidade_matricula_fundamental_4_ano as int64
            ) quantidade_matricula_fundamental_4_ano,
            safe_cast(
                quantidade_matricula_fundamental_5_ano as int64
            ) quantidade_matricula_fundamental_5_ano,
            safe_cast(
                quantidade_matricula_fundamental_anos_finais as int64
            ) quantidade_matricula_fundamental_anos_finais,
            safe_cast(
                quantidade_matricula_fundamental_6_ano as int64
            ) quantidade_matricula_fundamental_6_ano,
            safe_cast(
                quantidade_matricula_fundamental_7_ano as int64
            ) quantidade_matricula_fundamental_7_ano,
            safe_cast(
                quantidade_matricula_fundamental_8_ano as int64
            ) quantidade_matricula_fundamental_8_ano,
            safe_cast(
                quantidade_matricula_fundamental_9_ano as int64
            ) quantidade_matricula_fundamental_9_ano,
            safe_cast(quantidade_matricula_medio as int64) quantidade_matricula_medio,
            safe_cast(
                quantidade_matricula_medio_propedeutico as int64
            ) quantidade_matricula_medio_propedeutico,
            safe_cast(
                quantidade_matricula_medio_propedeutico_1_ano as int64
            ) quantidade_matricula_medio_propedeutico_1_ano,
            safe_cast(
                quantidade_matricula_medio_propedeutico_2_ano as int64
            ) quantidade_matricula_medio_propedeutico_2_ano,
            safe_cast(
                quantidade_matricula_medio_propedeutico_3_ano as int64
            ) quantidade_matricula_medio_propedeutico_3_ano,
            safe_cast(
                quantidade_matricula_medio_propedeutico_4_ano as int64
            ) quantidade_matricula_medio_propedeutico_4_ano,
            safe_cast(
                quantidade_matricula_medio_propedeutico_nao_seriado as int64
            ) quantidade_matricula_medio_propedeutico_nao_seriado,
            safe_cast(
                quantidade_matricula_medio_tecnico as int64
            ) quantidade_matricula_medio_tecnico,
            safe_cast(
                quantidade_matricula_medio_tecnico_1_ano as int64
            ) quantidade_matricula_medio_tecnico_1_ano,
            safe_cast(
                quantidade_matricula_medio_tecnico_2_ano as int64
            ) quantidade_matricula_medio_tecnico_2_ano,
            safe_cast(
                quantidade_matricula_medio_tecnico_3_ano as int64
            ) quantidade_matricula_medio_tecnico_3_ano,
            safe_cast(
                quantidade_matricula_medio_tecnico_4_ano as int64
            ) quantidade_matricula_medio_tecnico_4_ano,
            safe_cast(
                quantidade_matricula_medio_tecnico_nao_seriado as int64
            ) quantidade_matricula_medio_tecnico_nao_seriado,
            safe_cast(
                quantidade_matricula_medio_magisterio as int64
            ) quantidade_matricula_medio_magisterio,
            safe_cast(
                quantidade_matricula_medio_magisterio_1_ano as int64
            ) quantidade_matricula_medio_magisterio_1_ano,
            safe_cast(
                quantidade_matricula_medio_magisterio_2_ano as int64
            ) quantidade_matricula_medio_magisterio_2_ano,
            safe_cast(
                quantidade_matricula_medio_magisterio_3_ano as int64
            ) quantidade_matricula_medio_magisterio_3_ano,
            safe_cast(
                quantidade_matricula_medio_magisterio_4_ano as int64
            ) quantidade_matricula_medio_magisterio_4_ano,
            safe_cast(
                quantidade_matricula_profissional as int64
            ) quantidade_matricula_profissional,
            safe_cast(
                quantidade_matricula_profissional_tecnica as int64
            ) quantidade_matricula_profissional_tecnica,
            safe_cast(
                quantidade_matricula_profissional_tecnica_concomitante as int64
            ) quantidade_matricula_profissional_tecnica_concomitante,
            safe_cast(
                quantidade_matricula_profissional_tecnica_subsequente as int64
            ) quantidade_matricula_profissional_tecnica_subsequente,
            safe_cast(
                quantidade_matricula_profissional_fic_concomitante as int64
            ) quantidade_matricula_profissional_fic_concomitante,
            safe_cast(quantidade_matricula_eja as int64) quantidade_matricula_eja,
            safe_cast(
                quantidade_matricula_eja_fundamental as int64
            ) quantidade_matricula_eja_fundamental,
            safe_cast(
                quantidade_matricula_eja_fundamental_anos_iniciais as int64
            ) quantidade_matricula_eja_fundamental_anos_iniciais,
            safe_cast(
                quantidade_matricula_eja_fundamental_anos_finais as int64
            ) quantidade_matricula_eja_fundamental_anos_finais,
            safe_cast(
                quantidade_matricula_eja_fundamental_fic as int64
            ) quantidade_matricula_eja_fundamental_fic,
            safe_cast(
                quantidade_matricula_eja_medio as int64
            ) quantidade_matricula_eja_medio,
            safe_cast(
                quantidade_matricula_eja_medio_nao_profissionalizante as int64
            ) quantidade_matricula_eja_medio_nao_profissionalizante,
            safe_cast(
                quantidade_matricula_eja_medio_fic as int64
            ) quantidade_matricula_eja_medio_fic,
            safe_cast(
                quantidade_matricula_eja_medio_tecnico as int64
            ) quantidade_matricula_eja_medio_tecnico,
            safe_cast(
                quantidade_matricula_especial as int64
            ) quantidade_matricula_especial,
            safe_cast(
                quantidade_matricula_especial_comum as int64
            ) quantidade_matricula_especial_comum,
            safe_cast(
                quantidade_matricula_especial_exclusiva as int64
            ) quantidade_matricula_especial_exclusiva,
            safe_cast(
                quantidade_matricula_feminino as int64
            ) quantidade_matricula_feminino,
            safe_cast(
                quantidade_matricula_masculino as int64
            ) quantidade_matricula_masculino,
            safe_cast(
                quantidade_matricula_nao_declarada as int64
            ) quantidade_matricula_nao_declarada,
            safe_cast(quantidade_matricula_branca as int64) quantidade_matricula_branca,
            safe_cast(quantidade_matricula_preta as int64) quantidade_matricula_preta,
            safe_cast(quantidade_matricula_parda as int64) quantidade_matricula_parda,
            safe_cast(
                quantidade_matricula_amarela as int64
            ) quantidade_matricula_amarela,
            safe_cast(
                quantidade_matricula_indigena as int64
            ) quantidade_matricula_indigena,
            safe_cast(
                quantidade_matricula_idade_0_3 as int64
            ) quantidade_matricula_idade_0_3,
            safe_cast(
                quantidade_matricula_idade_4_5 as int64
            ) quantidade_matricula_idade_4_5,
            safe_cast(
                quantidade_matricula_idade_6_10 as int64
            ) quantidade_matricula_idade_6_10,
            safe_cast(
                quantidade_matricula_idade_11_14 as int64
            ) quantidade_matricula_idade_11_14,
            safe_cast(
                quantidade_matricula_idade_15_17 as int64
            ) quantidade_matricula_idade_15_17,
            safe_cast(
                quantidade_matricula_idade_18 as int64
            ) quantidade_matricula_idade_18,
            safe_cast(quantidade_matricula_diurno as int64) quantidade_matricula_diurno,
            safe_cast(
                quantidade_matricula_noturno as int64
            ) quantidade_matricula_noturno,
            safe_cast(quantidade_matricula_ead as int64) quantidade_matricula_ead,
            safe_cast(
                quantidade_matricula_infantil_integral as int64
            ) quantidade_matricula_infantil_integral,
            safe_cast(
                quantidade_matricula_infantil_creche_integral as int64
            ) quantidade_matricula_infantil_creche_integral,
            safe_cast(
                quantidade_matricula_infantil_pre_escola_integral as int64
            ) quantidade_matricula_infantil_pre_escola_integral,
            safe_cast(
                quantidade_matricula_fundamental_integral as int64
            ) quantidade_matricula_fundamental_integral,
            safe_cast(
                quantidade_matricula_fundamental_anos_iniciais_integral as int64
            ) quantidade_matricula_fundamental_anos_iniciais_integral,
            safe_cast(
                quantidade_matricula_fundamental_anos_finais_integral as int64
            ) quantidade_matricula_fundamental_anos_finais_integral,
            safe_cast(
                quantidade_matricula_medio_integral as int64
            ) quantidade_matricula_medio_integral,
            safe_cast(
                quantidade_matricula_zona_residencia_urbana as int64
            ) quantidade_matricula_zona_residencia_urbana,
            safe_cast(
                quantidade_matricula_zona_residencia_rural as int64
            ) quantidade_matricula_zona_residencia_rural,
            safe_cast(
                quantidade_matricula_zona_residencia_nao_aplicavel as int64
            ) quantidade_matricula_zona_residencia_nao_aplicavel,
            safe_cast(
                quantidade_matricula_utiliza_transporte_publico as int64
            ) quantidade_matricula_utiliza_transporte_publico,
            safe_cast(
                quantidade_matricula_transporte_estadual as int64
            ) quantidade_matricula_transporte_estadual,
            safe_cast(
                quantidade_matricula_transporte_municipal as int64
            ) quantidade_matricula_transporte_municipal,
            safe_cast(
                quantidade_docente_educacao_basica as int64
            ) quantidade_docente_educacao_basica,
            safe_cast(quantidade_docente_infantil as int64) quantidade_docente_infantil,
            safe_cast(
                quantidade_docente_infantil_creche as int64
            ) quantidade_docente_infantil_creche,
            safe_cast(
                quantidade_docente_infantil_pre_escola as int64
            ) quantidade_docente_infantil_pre_escola,
            safe_cast(
                quantidade_docente_fundamental as int64
            ) quantidade_docente_fundamental,
            safe_cast(
                quantidade_docente_fundamental_anos_iniciais as int64
            ) quantidade_docente_fundamental_anos_iniciais,
            safe_cast(
                quantidade_docente_fundamental_anos_finais as int64
            ) quantidade_docente_fundamental_anos_finais,
            safe_cast(quantidade_docente_medio as int64) quantidade_docente_medio,
            safe_cast(
                quantidade_docente_profissional as int64
            ) quantidade_docente_profissional,
            safe_cast(
                quantidade_docente_profissional_tecnica as int64
            ) quantidade_docente_profissional_tecnica,
            safe_cast(quantidade_docente_eja as int64) quantidade_docente_eja,
            safe_cast(
                quantidade_docente_eja_fundamental as int64
            ) quantidade_docente_eja_fundamental,
            safe_cast(
                quantidade_docente_eja_medio as int64
            ) quantidade_docente_eja_medio,
            safe_cast(quantidade_docente_especial as int64) quantidade_docente_especial,
            safe_cast(
                quantidade_docente_especial_comum as int64
            ) quantidade_docente_especial_comum,
            safe_cast(
                quantidade_docente_especial_exclusiva as int64
            ) quantidade_docente_especial_exclusiva,
            safe_cast(
                quantidade_turma_educacao_basica as int64
            ) quantidade_turma_educacao_basica,
            safe_cast(quantidade_turma_infantil as int64) quantidade_turma_infantil,
            safe_cast(
                quantidade_turma_infantil_integral as int64
            ) quantidade_turma_infantil_integral,
            safe_cast(
                quantidade_turma_infantil_creche as int64
            ) quantidade_turma_infantil_creche,
            safe_cast(
                quantidade_turma_infantil_creche_integral as int64
            ) quantidade_turma_infantil_creche_integral,
            safe_cast(
                quantidade_turma_infantil_pre_escola as int64
            ) quantidade_turma_infantil_pre_escola,
            safe_cast(
                quantidade_turma_infantil_pre_escola_integral as int64
            ) quantidade_turma_infantil_pre_escola_integral,
            safe_cast(
                quantidade_turma_fundamental as int64
            ) quantidade_turma_fundamental,
            safe_cast(
                quantidade_turma_fundamental_integral as int64
            ) quantidade_turma_fundamental_integral,
            safe_cast(
                quantidade_turma_fundamental_anos_iniciais as int64
            ) quantidade_turma_fundamental_anos_iniciais,
            safe_cast(
                quantidade_turma_fundamental_anos_iniciais_integral as int64
            ) quantidade_turma_fundamental_anos_iniciais_integral,
            safe_cast(
                quantidade_turma_fundamental_anos_finais as int64
            ) quantidade_turma_fundamental_anos_finais,
            safe_cast(
                quantidade_turma_fundamental_anos_finais_integral as int64
            ) quantidade_turma_fundamental_anos_finais_integral,
            safe_cast(quantidade_turma_medio as int64) quantidade_turma_medio,
            safe_cast(
                quantidade_turma_medio_integral as int64
            ) quantidade_turma_medio_integral,
            safe_cast(
                quantidade_turma_profissional as int64
            ) quantidade_turma_profissional,
            safe_cast(
                quantidade_turma_profissional_tecnica as int64
            ) quantidade_turma_profissional_tecnica,
            safe_cast(quantidade_turma_eja as int64) quantidade_turma_eja,
            safe_cast(
                quantidade_turma_eja_fundamental as int64
            ) quantidade_turma_eja_fundamental,
            safe_cast(quantidade_turma_eja_medio as int64) quantidade_turma_eja_medio,
            safe_cast(quantidade_turma_especial as int64) quantidade_turma_especial,
            safe_cast(
                quantidade_turma_especial_comum as int64
            ) quantidade_turma_especial_comum,
            safe_cast(
                quantidade_turma_especial_exclusiva as int64
            ) quantidade_turma_especial_exclusiva,
            safe_cast(quantidade_turma_diurno as int64) quantidade_turma_diurno,
            safe_cast(quantidade_turma_noturno as int64) quantidade_turma_noturno,
            safe_cast(quantidade_turma_ead as int64) quantidade_turma_ead
        from `basedosdados-staging.br_inep_censo_escolar_staging.escola_2024`
    )

select *
from censo
union all
select *
from censo_2023
union all
select *
from censo_2024
