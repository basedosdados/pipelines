{{ config(alias="meio_ambiente", schema="br_ibge_munic", materialized="table") }}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(caracterizacao_orgao_gestor as string) caracterizacao_orgao_gestor,
    safe_cast(
        informacoes_gestor_escolaridade as string
    ) informacoes_gestor_escolaridade,
    safe_cast(informacoes_gestor_sexo as string) informacoes_gestor_sexo,
    safe_cast(informacoes_gestor_idade as int64) informacoes_gestor_idade,
    safe_cast(informacoes_gestor_cor_raca as string) informacoes_gestor_cor_raca,
    safe_cast(
        informacoes_gestor_cor_raca_respondido_gestor as string
    ) informacoes_gestor_cor_raca_respondido_gestor,
    safe_cast(secretaria_existencia as string) secretaria_existencia,
    safe_cast(
        secretaria_trata_unicamiente_meio_ambiente as string
    ) secretaria_trata_unicamiente_meio_ambiente,
    safe_cast(
        departamento_similar_existencia as string
    ) departamento_similar_existencia,
    safe_cast(
        secretaria_associada_subordinada_agricultura as string
    ) secretaria_associada_subordinada_agricultura,
    safe_cast(
        secretaria_associada_subordinada_defesa_civil as string
    ) secretaria_associada_subordinada_defesa_civil,
    safe_cast(
        secretaria_associada_subordinada_educacao_cultura as string
    ) secretaria_associada_subordinada_educacao_cultura,
    safe_cast(
        secretaria_associada_subordinada_industria as string
    ) secretaria_associada_subordinada_industria,
    safe_cast(
        secretaria_associada_subordinada_obras as string
    ) secretaria_associada_subordinada_obras,
    safe_cast(
        secretaria_associada_subordinada_pesca as string
    ) secretaria_associada_subordinada_pesca,
    safe_cast(
        secretaria_associada_subordinada_planejamento as string
    ) secretaria_associada_subordinada_planejamento,
    safe_cast(
        secretaria_associada_subordinada_saude as string
    ) secretaria_associada_subordinada_saude,
    safe_cast(
        secretaria_associada_subordinada_turismo as string
    ) secretaria_associada_subordinada_turismo,
    safe_cast(
        secretaria_associada_subordinada_outras as string
    ) secretaria_associada_subordinada_outras,
    safe_cast(pessoal_ocupado_total as int64) pessoal_ocupado_total,
    safe_cast(pessoal_ocupado_estatutario as int64) pessoal_ocupado_estatutario,
    safe_cast(pessoal_ocupado_celetista as int64) pessoal_ocupado_celetista,
    safe_cast(pessoal_ocupado_estagiario as int64) pessoal_ocupado_estagiario,
    safe_cast(
        pessoal_ocupado_somente_comissionado as int64
    ) pessoal_ocupado_somente_comissionado,
    safe_cast(pessoal_ocupado_outros as int64) pessoal_ocupado_outros,
    safe_cast(
        pessoal_ocupado_sem_vinculo_permanente as int64
    ) pessoal_ocupado_sem_vinculo_permanente,
    safe_cast(
        capacitacao_uniao_ultimos_4anos as string
    ) capacitacao_uniao_ultimos_4anos,
    safe_cast(
        capacitacao_area_estruturacao_gestao as string
    ) capacitacao_area_estruturacao_gestao,
    safe_cast(capacitacao_area_licensiamento as string) capacitacao_area_licensiamento,
    safe_cast(
        capacitacao_area_educacao_ambiental as string
    ) capacitacao_area_educacao_ambiental,
    safe_cast(
        capacitacao_area_educacao_ambiental_agricultura_familiar as string
    ) capacitacao_area_educacao_ambiental_agricultura_familiar,
    safe_cast(capacitacao_area_car as string) capacitacao_area_car,
    safe_cast(
        capacitacao_area_residuos_solidos as string
    ) capacitacao_area_residuos_solidos,
    safe_cast(
        capacitacao_area_producao_consumo_sustentaveis as string
    ) capacitacao_area_producao_consumo_sustentaveis,
    safe_cast(capacitacao_area_mudanca_clima as string) capacitacao_area_mudanca_clima,
    safe_cast(
        capacitacao_area_recursos_hidricos as string
    ) capacitacao_area_recursos_hidricos,
    safe_cast(
        capacitacao_area_participacao_social_foruns as string
    ) capacitacao_area_participacao_social_foruns,
    safe_cast(capacitacao_area_outras as string) capacitacao_area_outras,
    safe_cast(conselho_existencia as string) conselho_existencia,
    safe_cast(conselho_ano_criacao as int64) conselho_ano_criacao,
    safe_cast(conselho_formacao as string) conselho_formacao,
    safe_cast(conselho_paritario as string) conselho_paritario,
    safe_cast(conselho_realizacao_reunioes as string) conselho_realizacao_reunioes,
    safe_cast(
        conselho_realizou_reuniao_ultimos_12meses as string
    ) conselho_realizou_reuniao_ultimos_12meses,
    safe_cast(
        conselho_reunioes_ultimos_12meses as int64
    ) conselho_reunioes_ultimos_12meses,
    safe_cast(conselho_carater_consultivo as string) conselho_carater_consultivo,
    safe_cast(conselho_carater_deliberativo as string) conselho_carater_deliberativo,
    safe_cast(conselho_carater_normativo as string) conselho_carater_normativo,
    safe_cast(conselho_carater_fiscalizador as string) conselho_carater_fiscalizador,
    safe_cast(conselho_conselheiros as int64) conselho_conselheiros,
    safe_cast(
        conselho_capacitacao_membros_periodicamente as string
    ) conselho_capacitacao_membros_periodicamente,
    safe_cast(
        conselho_capacitacao_membros_ocasionalmente as string
    ) conselho_capacitacao_membros_ocasionalmente,
    safe_cast(
        conselho_capacitacao_membros_nao_realiza as string
    ) conselho_capacitacao_membros_nao_realiza,
    safe_cast(conselho_infraestrutura as string) conselho_infraestrutura,
    safe_cast(conselho_infraestrutura_sala as string) conselho_infraestrutura_sala,
    safe_cast(
        conselho_infraestrutura_computador as string
    ) conselho_infraestrutura_computador,
    safe_cast(
        conselho_infraestrutura_impressora as string
    ) conselho_infraestrutura_impressora,
    safe_cast(
        conselho_infraestrutura_internet as string
    ) conselho_infraestrutura_internet,
    safe_cast(
        conselho_infraestrutura_veiculo as string
    ) conselho_infraestrutura_veiculo,
    safe_cast(
        conselho_infraestrutura_telefone as string
    ) conselho_infraestrutura_telefone,
    safe_cast(
        conselho_infraestrutura_diarias as string
    ) conselho_infraestrutura_diarias,
    safe_cast(
        conselho_infraestrutura_dotacao_orcamentaria as string
    ) conselho_infraestrutura_dotacao_orcamentaria,
    safe_cast(fundo_municipal_existencia as string) fundo_municipal_existencia,
    safe_cast(
        fundo_municipal_gestao_conselho_municipal as string
    ) fundo_municipal_gestao_conselho_municipal,
    safe_cast(
        fundo_municipal_financiou_acoes_ano_anterior as string
    ) fundo_municipal_financiou_acoes_ano_anterior,
    safe_cast(
        recursos_especificos_meio_ambiente as string
    ) recursos_especificos_meio_ambiente,
    safe_cast(licensiamento_impacto_local as string) licensiamento_impacto_local,
    safe_cast(
        licensiamento_instrumentos_delegacao_outros_impactos as string
    ) licensiamento_instrumentos_delegacao_outros_impactos,
    safe_cast(
        licensiamento_concedeu_ano_anterior as string
    ) licensiamento_concedeu_ano_anterior,
    safe_cast(
        licensiamento_concedeu_ano_anterior_licencas_previas as string
    ) licensiamento_concedeu_ano_anterior_licencas_previas,
    safe_cast(
        licensiamento_concedeu_ano_anterior_licencas_instalacao as string
    ) licensiamento_concedeu_ano_anterior_licencas_instalacao,
    safe_cast(
        licensiamento_concedeu_ano_anterior_licencas_operacao as string
    ) licensiamento_concedeu_ano_anterior_licencas_operacao,
    safe_cast(
        base_cartografica_digitalizada_existencia as string
    ) base_cartografica_digitalizada_existencia,
    safe_cast(
        sistema_informacao_geografica_existencia as string
    ) sistema_informacao_geografica_existencia,
    safe_cast(
        agenda21_processo_elaboracao_local as string
    ) agenda21_processo_elaboracao_local,
    safe_cast(agenda21_estagio_atual as string) agenda21_estagio_atual,
    safe_cast(
        agenda21_forum_frequencia_ultimos_12meses as string
    ) agenda21_forum_frequencia_ultimos_12meses,
    safe_cast(cadastro_ambiental_rural as string) cadastro_ambiental_rural,
    safe_cast(legislacao_questao_ambiental as string) legislacao_questao_ambiental,
    safe_cast(
        legislacao_questao_ambiental_forma as string
    ) legislacao_questao_ambiental_forma,
    safe_cast(legislacao_coleta_seletiva as string) legislacao_coleta_seletiva,
    safe_cast(
        legislacao_coleta_seletiva_ano_criacao as int64
    ) legislacao_coleta_seletiva_ano_criacao,
    safe_cast(legislacao_saneamento_basico as string) legislacao_saneamento_basico,
    safe_cast(
        legislacao_saneamento_basico_ano_criacao as int64
    ) legislacao_saneamento_basico_ano_criacao,
    safe_cast(
        legislacao_gestao_bacias_hidrograficas as string
    ) legislacao_gestao_bacias_hidrograficas,
    safe_cast(
        legislacao_gestao_bacias_hidrograficas_ano_criacao as int64
    ) legislacao_gestao_bacias_hidrograficas_ano_criacao,
    safe_cast(
        legislacao_area_protecao_ambiental as string
    ) legislacao_area_protecao_ambiental,
    safe_cast(
        legislacao_area_protecao_ambiental_ano_criacao as int64
    ) legislacao_area_protecao_ambiental_ano_criacao,
    safe_cast(
        legislacao_destino_embalagens_agrotoxicos as string
    ) legislacao_destino_embalagens_agrotoxicos,
    safe_cast(
        legislacao_destino_embalagens_agrotoxicos_ano_criacao as int64
    ) legislacao_destino_embalagens_agrotoxicos_ano_criacao,
    safe_cast(legislacao_poluicao_ar as string) legislacao_poluicao_ar,
    safe_cast(
        legislacao_poluicao_ar_ano_criacao as int64
    ) legislacao_poluicao_ar_ano_criacao,
    safe_cast(
        legislacao_permissao_atividades_extrativas_minerais as string
    ) legislacao_permissao_atividades_extrativas_minerais,
    safe_cast(
        legislacao_permissao_atividades_extrativas_minerais_ano_criacao as int64
    ) legislacao_permissao_atividades_extrativas_minerais_ano_criacao,
    safe_cast(legislacao_fauna_silvestre as string) legislacao_fauna_silvestre,
    safe_cast(
        legislacao_fauna_silvestre_ano_criacao as int64
    ) legislacao_fauna_silvestre_ano_criacao,
    safe_cast(legislacao_florestas as string) legislacao_florestas,
    safe_cast(
        legislacao_florestas_ano_criacao as int64
    ) legislacao_florestas_ano_criacao,
    safe_cast(
        legislacao_protecao_biodiversidade as string
    ) legislacao_protecao_biodiversidade,
    safe_cast(
        legislacao_protecao_biodiversidade_ano_criacao as int64
    ) legislacao_protecao_biodiversidade_ano_criacao,
    safe_cast(
        legislacao_adaptacao_mitigacao_mudanca_clima as string
    ) legislacao_adaptacao_mitigacao_mudanca_clima,
    safe_cast(
        legislacao_adaptacao_mitigacao_mudanca_clima_ano_criacao as int64
    ) legislacao_adaptacao_mitigacao_mudanca_clima_ano_criacao,
    safe_cast(legislacao_nenhuma_citada as string) legislacao_nenhuma_citada,
    safe_cast(
        implementacao_convenio_parceria as string
    ) implementacao_convenio_parceria,
    safe_cast(
        implementacao_convenio_parceria_orgao_publico as string
    ) implementacao_convenio_parceria_orgao_publico,
    safe_cast(
        implementacao_convenio_parceria_empresa_estatal as string
    ) implementacao_convenio_parceria_empresa_estatal,
    safe_cast(
        implementacao_convenio_parceria_iniciativa_privada as string
    ) implementacao_convenio_parceria_iniciativa_privada,
    safe_cast(
        implementacao_convenio_parceria_instituicao_internacional as string
    ) implementacao_convenio_parceria_instituicao_internacional,
    safe_cast(
        implementacao_convenio_parceria_ong as string
    ) implementacao_convenio_parceria_ong,
    safe_cast(
        implementacao_convenio_parceria_universidade_orgao_ensino_pesquisa as string
    ) implementacao_convenio_parceria_universidade_orgao_ensino_pesquisa,
    safe_cast(
        implementacao_convenio_parceria_outro as string
    ) implementacao_convenio_parceria_outro,
    safe_cast(
        articulacao_intermunicipal_consorcio as string
    ) articulacao_intermunicipal_consorcio,
    safe_cast(
        articulacao_principais_temas_abordados_consorcio_1 as string
    ) articulacao_principais_temas_abordados_consorcio_1,
    safe_cast(
        articulacao_principais_temas_abordados_consorcio_2 as string
    ) articulacao_principais_temas_abordados_consorcio_2,
    safe_cast(
        articulacao_principais_temas_abordados_consorcio_3 as string
    ) articulacao_principais_temas_abordados_consorcio_3,
    safe_cast(
        articulacao_intermunicipal_comite_bacia_hidrografica as string
    ) articulacao_intermunicipal_comite_bacia_hidrografica,
    safe_cast(
        articulacao_intermunicipal_comite_bacia_hidrografica_quantidade_comites
        as string
    ) articulacao_intermunicipal_comite_bacia_hidrografica_quantidade_comites,
    safe_cast(
        articulacao_intermunicipal_outro as string
    ) articulacao_intermunicipal_outro,
    safe_cast(
        articulacao_temas_abordados_disposicao_residuos_solidos as string
    ) articulacao_temas_abordados_disposicao_residuos_solidos,
    safe_cast(
        articulacao_temas_abordados_recuperacao_recurso_hidrico as string
    ) articulacao_temas_abordados_recuperacao_recurso_hidrico,
    safe_cast(
        articulacao_temas_abordados_tratamento_esgoto_domestico as string
    ) articulacao_temas_abordados_tratamento_esgoto_domestico,
    safe_cast(
        articulacao_temas_abordados_recuperacao_areas_degradadas as string
    ) articulacao_temas_abordados_recuperacao_areas_degradadas,
    safe_cast(
        articulacao_temas_abordados_outros as string
    ) articulacao_temas_abordados_outros,
    safe_cast(
        articulacao_consorcio_publico_intermunicipal as string
    ) articulacao_consorcio_publico_intermunicipal,
    safe_cast(
        articulacao_consorcio_publico_estado as string
    ) articulacao_consorcio_publico_estado,
    safe_cast(
        articulacao_consorcio_publico_uniao as string
    ) articulacao_consorcio_publico_uniao,
    safe_cast(
        articulacao_consorcio_administrativo_intermunicipal as string
    ) articulacao_consorcio_administrativo_intermunicipal,
    safe_cast(
        articulacao_consorcio_administrativo_estado as string
    ) articulacao_consorcio_administrativo_estado,
    safe_cast(
        articulacao_consorcio_administrativo_uniao as string
    ) articulacao_consorcio_administrativo_uniao,
    safe_cast(
        articulacao_convenio_setor_privado as string
    ) articulacao_convenio_setor_privado,
    safe_cast(
        articulacao_apoio_setor_privado_comunidades as string
    ) articulacao_apoio_setor_privado_comunidades,
    safe_cast(articulacao_nao_participa as string) articulacao_nao_participa,
    safe_cast(
        parceria_governo_federal_programas as string
    ) parceria_governo_federal_programas,
    safe_cast(
        parceria_governo_federal_programas_coletivo_educador as string
    ) parceria_governo_federal_programas_coletivo_educador,
    safe_cast(
        parceria_governo_federal_programas_sala_verde as string
    ) parceria_governo_federal_programas_sala_verde,
    safe_cast(
        parceria_governo_federal_programas_circuito_tela_verde as string
    ) parceria_governo_federal_programas_circuito_tela_verde,
    safe_cast(
        parceria_governo_federal_programas_conferencia_infanto_juvenil as string
    ) parceria_governo_federal_programas_conferencia_infanto_juvenil,
    safe_cast(
        parceria_governo_federal_programas_educacao_ambiental_pgrs as string
    ) parceria_governo_federal_programas_educacao_ambiental_pgrs,
    safe_cast(
        parceria_governo_federal_programas_sustentabilidade_instituicoes_publicas
        as string
    ) parceria_governo_federal_programas_sustentabilidade_instituicoes_publicas,
    safe_cast(
        parceria_governo_federal_programas_peaaf as string
    ) parceria_governo_federal_programas_peaaf,
    safe_cast(
        parceria_governo_federal_programas_conferencia_nacional_meio_ambiente as string
    ) parceria_governo_federal_programas_conferencia_nacional_meio_ambiente,
    safe_cast(
        parceria_governo_federal_programas_nenhum as string
    ) parceria_governo_federal_programas_nenhum,
    safe_cast(ceia_estadual as string) ceia_estadual,
    safe_cast(
        ceia_estadual_reuniao_ultimos_12meses as string
    ) ceia_estadual_reuniao_ultimos_12meses,
    safe_cast(
        plano_gestao_integrada_residuos_solidos_pnrs as string
    ) plano_gestao_integrada_residuos_solidos_pnrs,
    safe_cast(
        plano_gestao_integrada_residuos_solidos_pnrs_apenas_municipio as string
    ) plano_gestao_integrada_residuos_solidos_pnrs_apenas_municipio,
    safe_cast(
        iniciativas_consumo_sustentavel as string
    ) iniciativas_consumo_sustentavel,
    safe_cast(
        iniciativas_consumo_sustentavel_reducao_sacolas_plasticas as string
    ) iniciativas_consumo_sustentavel_reducao_sacolas_plasticas,
    safe_cast(
        iniciativas_consumo_sustentavel_sustentabilidade_instituicoes_publicas as string
    ) iniciativas_consumo_sustentavel_sustentabilidade_instituicoes_publicas,
    safe_cast(
        iniciativas_consumo_sustentavel_reducao_consumo_agua_energia as string
    ) iniciativas_consumo_sustentavel_reducao_consumo_agua_energia,
    safe_cast(
        iniciativas_consumo_sustentavel_criterio_ambiental_compra_publica as string
    ) iniciativas_consumo_sustentavel_criterio_ambiental_compra_publica,
    safe_cast(
        iniciativas_consumo_sustentavel_outras as string
    ) iniciativas_consumo_sustentavel_outras,
    safe_cast(
        unidade_conservacao_municipal_existencia as string
    ) unidade_conservacao_municipal_existencia,
    safe_cast(
        recursos_especificos_meio_ambiente_licensiamento as string
    ) recursos_especificos_meio_ambiente_licensiamento,
    safe_cast(
        recursos_oriundos_institiuicao_internacional as string
    ) recursos_oriundos_institiuicao_internacional,
    safe_cast(
        recursos_oriundos_empresa_publica as string
    ) recursos_oriundos_empresa_publica,
    safe_cast(
        recursos_oriundos_entidades_ensino_pesquisa as string
    ) recursos_oriundos_entidades_ensino_pesquisa,
    safe_cast(recursos_oriundos_ong as string) recursos_oriundos_ong,
    safe_cast(
        recursos_oriundos_iniciativa_privada as string
    ) recursos_oriundos_iniciativa_privada,
    safe_cast(recursos_oriundos_outros as string) recursos_oriundos_outros,
    safe_cast(
        recursos_oriundos_orgao_publico as string
    ) recursos_oriundos_orgao_publico,
    safe_cast(
        recursos_oriundos_orgao_publico_taxa_licensiamento as string
    ) recursos_oriundos_orgao_publico_taxa_licensiamento,
    safe_cast(
        recursos_oriundos_orgao_publico_multas as string
    ) recursos_oriundos_orgao_publico_multas,
    safe_cast(
        recursos_oriundos_orgao_publico_icms_ecologico as string
    ) recursos_oriundos_orgao_publico_icms_ecologico,
    safe_cast(
        recursos_oriundos_orgao_publico_royalties as string
    ) recursos_oriundos_orgao_publico_royalties,
    safe_cast(
        recursos_oriundos_orgao_publico_outros as string
    ) recursos_oriundos_orgao_publico_outros,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses as string
    ) ocorrencia_impactante_ultimos_24meses,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_condicoes_extremas as string
    ) ocorrencia_impactante_ultimos_24meses_condicoes_extremas,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_poluicao_ar as string
    ) ocorrencia_impactante_ultimos_24meses_poluicao_ar,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_poluicao_agua as string
    ) ocorrencia_impactante_ultimos_24meses_poluicao_agua,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_escassez_agua as string
    ) ocorrencia_impactante_ultimos_24meses_escassez_agua,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_assoreamento_agua as string
    ) ocorrencia_impactante_ultimos_24meses_assoreamento_agua,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_diminuicao_vazao_agua as string
    ) ocorrencia_impactante_ultimos_24meses_diminuicao_vazao_agua,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_contaminacao_solo as string
    ) ocorrencia_impactante_ultimos_24meses_contaminacao_solo,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_perda_solo as string
    ) ocorrencia_impactante_ultimos_24meses_perda_solo,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_degradacao_areas_protegidas as string
    ) ocorrencia_impactante_ultimos_24meses_degradacao_areas_protegidas,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_desmatamento as string
    ) ocorrencia_impactante_ultimos_24meses_desmatamento,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_alteracao_paisagem as string
    ) ocorrencia_impactante_ultimos_24meses_alteracao_paisagem,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_queimadas as string
    ) ocorrencia_impactante_ultimos_24meses_queimadas,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_reducao_biodiversidade as string
    ) ocorrencia_impactante_ultimos_24meses_reducao_biodiversidade,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_reducao_quantidade_diversidade_pescado
        as string
    ) ocorrencia_impactante_ultimos_24meses_reducao_quantidade_diversidade_pescado,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_agricultura_prejudicada as string
    ) ocorrencia_impactante_ultimos_24meses_agricultura_prejudicada,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_pecuaria_prejudicada as string
    ) ocorrencia_impactante_ultimos_24meses_pecuaria_prejudicada,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_moradia_risco_ambiental as string
    ) ocorrencia_impactante_ultimos_24meses_moradia_risco_ambiental,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_falta_saneamento as string
    ) ocorrencia_impactante_ultimos_24meses_falta_saneamento,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_outras as string
    ) ocorrencia_impactante_ultimos_24meses_outras,
    safe_cast(
        ocorrencia_impactante_ultimos_24meses_nenhuma as string
    ) ocorrencia_impactante_ultimos_24meses_nenhuma,
    safe_cast(
        servicos_recebe_recursos_pagamento as string
    ) servicos_recebe_recursos_pagamento,
    safe_cast(
        servicos_recebe_recursos_pagamento_municipio as string
    ) servicos_recebe_recursos_pagamento_municipio,
    safe_cast(
        servicos_recebe_recursos_pagamento_uniao as string
    ) servicos_recebe_recursos_pagamento_uniao,
    safe_cast(
        servicos_recebe_recursos_pagamento_estado as string
    ) servicos_recebe_recursos_pagamento_estado,
    safe_cast(
        servicos_recebe_recursos_pagamento_outro_municipio as string
    ) servicos_recebe_recursos_pagamento_outro_municipio,
    safe_cast(
        servicos_recebe_recursos_pagamento_iniciativa_privada as string
    ) servicos_recebe_recursos_pagamento_iniciativa_privada,
    safe_cast(
        servicos_recebe_recursos_pagamento_ong as string
    ) servicos_recebe_recursos_pagamento_ong,
    safe_cast(
        servicos_recebe_recursos_pagamento_doacoes as string
    ) servicos_recebe_recursos_pagamento_doacoes,
    safe_cast(
        servicos_recebe_recursos_pagamento_outros as string
    ) servicos_recebe_recursos_pagamento_outros,
    safe_cast(servicos_paga as string) servicos_paga,
    safe_cast(
        servicos_paga_instrumento_conservacao_melhoria_agua as string
    ) servicos_paga_instrumento_conservacao_melhoria_agua,
    safe_cast(
        servicos_paga_instrumento_conservacao_vegetacao_vida_silvestre as string
    ) servicos_paga_instrumento_conservacao_vegetacao_vida_silvestre,
    safe_cast(
        servicos_paga_instrumento_unidades_conservacao as string
    ) servicos_paga_instrumento_unidades_conservacao,
    safe_cast(
        servicos_paga_instrumento_recuperacao_solos_areas_degradadas as string
    ) servicos_paga_instrumento_recuperacao_solos_areas_degradadas,
    safe_cast(
        servicos_paga_instrumento_conservacao_vegetacao_urbana as string
    ) servicos_paga_instrumento_conservacao_vegetacao_urbana,
    safe_cast(
        servicos_paga_instrumento_captura_carbono as string
    ) servicos_paga_instrumento_captura_carbono,
    safe_cast(
        servicos_paga_instrumento_outros as string
    ) servicos_paga_instrumento_outros,
    safe_cast(servicos_instrumento as string) servicos_instrumento,
    safe_cast(
        lei_parcelamento_solo_define_zonas_prioritarias as string
    ) lei_parcelamento_solo_define_zonas_prioritarias,
    safe_cast(
        lei_parcelamento_solo_define_zonas_prioritarias_quantas as string
    ) lei_parcelamento_solo_define_zonas_prioritarias_quantas,
    safe_cast(terceirizacao_ano_anterior as string) terceirizacao_ano_anterior,
    safe_cast(
        plano_contingencia_desastres_naturais_existencia as string
    ) plano_contingencia_desastres_naturais_existencia
from {{ set_datalake_project("br_ibge_munic_staging.meio_ambiente") }} as t
