{{ config(alias="habitacao", schema="br_ibge_munic", materialized="table") }}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(
        orgao_especifico_implementacao_politica_habitacional_existencia as string
    ) orgao_especifico_implementacao_politica_habitacional_existencia,
    safe_cast(
        caracterizacao_orgao_gestor_planejamento_urbano as string
    ) caracterizacao_orgao_gestor_planejamento_urbano,
    safe_cast(informacoes_gestor_sexo as string) informacoes_gestor_sexo,
    safe_cast(informacoes_gestor_idade as int64) informacoes_gestor_idade,
    safe_cast(informacoes_gestor_cor_raca as string) informacoes_gestor_cor_raca,
    safe_cast(
        informacoes_gestor_cor_raca_respondido_titular as string
    ) informacoes_gestor_cor_raca_respondido_titular,
    safe_cast(informacoes_gestor_escolaridade as string) informacoes_gestor_escolaridade,
    safe_cast(
        orgao_administracao_indireta_habitacao_existencia as string
    ) orgao_administracao_indireta_habitacao_existencia,
    safe_cast(plano_diretor_existencia as string) plano_diretor_existencia,
    safe_cast(plano_diretor_ano as int64) plano_diretor_ano,
    safe_cast(plano_diretor_foi_revisto as string) plano_diretor_foi_revisto,
    safe_cast(plano_diretor_ano_ultima_atualizacao as int64) plano_diretor_ano_ultima_atualizacao,
    safe_cast(plano_diretor_numero_lei as string) plano_diretor_numero_lei,
    safe_cast(plano_diretor_elaborando_revendo as string) plano_diretor_elaborando_revendo,
    safe_cast(plano_diretor_revendo as string) plano_diretor_revendo,
    safe_cast(plano_diretor_elaborando as string) plano_diretor_elaborando,
    safe_cast(
        plano_diretor_instrumentos_participacao_coordenacao_compartilhada as string
    ) plano_diretor_instrumentos_participacao_coordenacao_compartilhada,
    safe_cast(
        plano_diretor_instrumentos_participacao_conselho_politica_urbana as string
    ) plano_diretor_instrumentos_participacao_conselho_politica_urbana,
    safe_cast(
        plano_diretor_instrumentos_participacao_conferencia_cidade as string
    ) plano_diretor_instrumentos_participacao_conferencia_cidade,
    safe_cast(
        plano_diretor_instrumentos_participacao_discussao_segmentos_especificos as string
    ) plano_diretor_instrumentos_participacao_discussao_segmentos_especificos,
    safe_cast(
        plano_diretor_instrumentos_participacao_discussao_tematica as string
    ) plano_diretor_instrumentos_participacao_discussao_tematica,
    safe_cast(
        plano_diretor_instrumentos_participacao_discussao_bairros_distritos_setores as string
    ) plano_diretor_instrumentos_participacao_discussao_bairros_distritos_setores,
    safe_cast(
        plano_diretor_instrumentos_participacao_atividades_capacitacao as string
    ) plano_diretor_instrumentos_participacao_atividades_capacitacao,
    safe_cast(
        plano_diretor_instrumentos_participacao_outros as string
    ) plano_diretor_instrumentos_participacao_outros,
    safe_cast(
        plano_diretor_instrumentos_participacao_nenhum as string
    ) plano_diretor_instrumentos_participacao_nenhum,
    safe_cast(
        plano_diretor_orientacao_normas_tecnicas_acessibilidade as string
    ) plano_diretor_orientacao_normas_tecnicas_acessibilidade,
    safe_cast(
        plano_diretor_contempla_instrumentos_parcelamento_solo as string
    ) plano_diretor_contempla_instrumentos_parcelamento_solo,
    safe_cast(
        plano_diretor_contempla_instrumentos_zoneamento_ocupacao_solo as string
    ) plano_diretor_contempla_instrumentos_zoneamento_ocupacao_solo,
    safe_cast(
        plano_diretor_contempla_instrumentos_codigo_obras as string
    ) plano_diretor_contempla_instrumentos_codigo_obras,
    safe_cast(
        plano_diretor_contempla_instrumentos_contribuicao_melhorias as string
    ) plano_diretor_contempla_instrumentos_contribuicao_melhorias,
    safe_cast(
        plano_diretor_contempla_instrumentos_operacao_urbana_consorciada as string
    ) plano_diretor_contempla_instrumentos_operacao_urbana_consorciada,
    safe_cast(
        plano_diretor_contempla_instrumentos_lei_solo_criado as string
    ) plano_diretor_contempla_instrumentos_lei_solo_criado,
    safe_cast(
        plano_diretor_contempla_instrumentos_estudo_impacto_vizinhanca as string
    ) plano_diretor_contempla_instrumentos_estudo_impacto_vizinhanca,
    safe_cast(
        plano_diretor_contempla_instrumentos_codigo_posturas as string
    ) plano_diretor_contempla_instrumentos_codigo_posturas,
    safe_cast(
        plano_diretor_contempla_instrumentos_zona_area_interesse_social as string
    ) plano_diretor_contempla_instrumentos_zona_area_interesse_social,
    safe_cast(
        plano_diretor_contempla_instrumentos_zona_area_especial as string
    ) plano_diretor_contempla_instrumentos_zona_area_especial,
    safe_cast(
        plano_diretor_contempla_instrumentos_solo_criado as string
    ) plano_diretor_contempla_instrumentos_solo_criado,
    safe_cast(plano_municipal_habitacao_existencia as string) plano_municipal_habitacao_existencia,
    safe_cast(
        plano_municipal_habitacao_articulado_plano_diretor as string
    ) plano_municipal_habitacao_articulado_plano_diretor,
    safe_cast(
        plano_municipal_habitacao_integrar_acoes_demais_politicas as string
    ) plano_municipal_habitacao_integrar_acoes_demais_politicas,
    safe_cast(
        plano_municipal_habitacao_promover_urbanizacao_recuperacao as string
    ) plano_municipal_habitacao_promover_urbanizacao_recuperacao,
    safe_cast(
        plano_municipal_habitacao_coibir_novas_ocupacoes_assentamentos as string
    ) plano_municipal_habitacao_coibir_novas_ocupacoes_assentamentos,
    safe_cast(
        plano_municipal_habitacao_articular_instancias as string
    ) plano_municipal_habitacao_articular_instancias,
    safe_cast(
        plano_municipal_habitacao_garantir_melhor_aproveitamento_infraestrutura as string
    ) plano_municipal_habitacao_garantir_melhor_aproveitamento_infraestrutura,
    safe_cast(
        plano_municipal_habitacao_garantir_atendimento_habitacional_familias_removidas as string
    ) plano_municipal_habitacao_garantir_atendimento_habitacional_familias_removidas,
    safe_cast(
        plano_municipal_habitacao_producao_novas_unidades_habitacionais as string
    ) plano_municipal_habitacao_producao_novas_unidades_habitacionais,
    safe_cast(
        plano_municipal_habitacao_priorizar_acoes_areas_risco as string
    ) plano_municipal_habitacao_priorizar_acoes_areas_risco,
    safe_cast(plano_municipal_habitacao_quilombolas as string) plano_municipal_habitacao_quilombolas,
    safe_cast(
        plano_municipal_habitacao_programas_povos_ciganos as string
    ) plano_municipal_habitacao_programas_povos_ciganos,
    safe_cast(
        plano_municipal_habitacao_povos_africanos as string
    ) plano_municipal_habitacao_povos_africanos,
    safe_cast(plano_municipal_habitacao_outros as string) plano_municipal_habitacao_outros,
    safe_cast(plano_municipal_habitacao_elaborando as string) plano_municipal_habitacao_elaborando,
    safe_cast(
        conferencia_municipal_habitacao_realizada_ultimos_4anos as string
    ) conferencia_municipal_habitacao_realizada_ultimos_4anos,
    safe_cast(
        conferencia_municipal_habitacao_elementos_elaboracao as string
    ) conferencia_municipal_habitacao_elementos_elaboracao,
    safe_cast(demais_instrumentos as string) demais_instrumentos,
    safe_cast(
        demais_instrumentos_zona_area_interesse_social_existencia as string
    ) demais_instrumentos_zona_area_interesse_social_existencia,
    safe_cast(
        demais_instrumentos_zona_area_interesse_social_ano_lei as int64
    ) demais_instrumentos_zona_area_interesse_social_ano_lei,
    safe_cast(
        demais_instrumentos_zona_area_interesse_especial_existencia as string
    ) demais_instrumentos_zona_area_interesse_especial_existencia,
    safe_cast(
        demais_instrumentos_zona_area_interesse_especial_ano_lei as int64
    ) demais_instrumentos_zona_area_interesse_especial_ano_lei,
    safe_cast(
        demais_instrumentos_zona_area_interesse_especial_ambiental_existencia as string
    ) demais_instrumentos_zona_area_interesse_especial_ambiental_existencia,
    safe_cast(
        demais_instrumentos_zona_area_interesse_especial_historico_existencia as string
    ) demais_instrumentos_zona_area_interesse_especial_historico_existencia,
    safe_cast(
        demais_instrumentos_zona_area_interesse_especial_cultural_existencia as string
    ) demais_instrumentos_zona_area_interesse_especial_cultural_existencia,
    safe_cast(
        demais_instrumentos_zona_area_interesse_especial_paisagistico_existencia as string
    ) demais_instrumentos_zona_area_interesse_especial_paisagistico_existencia,
    safe_cast(
        demais_instrumentos_zona_area_interesse_especial_arquitetonico_existencia as string
    ) demais_instrumentos_zona_area_interesse_especial_arquitetonico_existencia,
    safe_cast(
        demais_instrumentos_zona_area_interesse_especial_arqueologico_existencia as string
    ) demais_instrumentos_zona_area_interesse_especial_arqueologico_existencia,
    safe_cast(
        demais_instrumentos_zona_area_interesse_especial_outra_existencia as string
    ) demais_instrumentos_zona_area_interesse_especial_outra_existencia,
    safe_cast(
        demais_instrumentos_perimetro_urbano_existencia as string
    ) demais_instrumentos_perimetro_urbano_existencia,
    safe_cast(demais_instrumentos_perimetro_urbano_ano_lei as int64) demais_instrumentos_perimetro_urbano_ano_lei,
    safe_cast(
        demais_instrumentos_parcelamento_solo_existencia as string
    ) demais_instrumentos_parcelamento_solo_existencia,
    safe_cast(demais_instrumentos_parcelamento_solo_ano_lei as int64) demais_instrumentos_parcelamento_solo_ano_lei,
    safe_cast(
        demais_instrumentos_zoneamento_ocupacao_solo_existencia as string
    ) demais_instrumentos_zoneamento_ocupacao_solo_existencia,
    safe_cast(
        demais_instrumentos_zoneamento_ocupacao_solo_ano_lei as int64
    ) demais_instrumentos_zoneamento_ocupacao_solo_ano_lei,
    safe_cast(demais_instrumentos_solo_criado_existencia as string) demais_instrumentos_solo_criado_existencia,
    safe_cast(demais_instrumentos_solo_criado_ano_lei as int64) demais_instrumentos_solo_criado_ano_lei,
    safe_cast(
        demais_instrumentos_contribuicao_melhoria_existencia as string
    ) demais_instrumentos_contribuicao_melhoria_existencia,
    safe_cast(demais_instrumentos_contribuicao_melhoria_ano_lei as int64) demais_instrumentos_contribuicao_melhoria_ano_lei,
    safe_cast(
        demais_instrumentos_operacao_urbana_consorciada_existencia as string
    ) demais_instrumentos_operacao_urbana_consorciada_existencia,
    safe_cast(
        demais_instrumentos_operacao_urbana_consorciada_ano_lei as int64
    ) demais_instrumentos_operacao_urbana_consorciada_ano_lei,
    safe_cast(
        demais_instrumentos_estudo_impacto_vizinhanca_existencia as string
    ) demais_instrumentos_estudo_impacto_vizinhanca_existencia,
    safe_cast(
        demais_instrumentos_estudo_impacto_vizinhanca_ano_lei as int64
    ) demais_instrumentos_estudo_impacto_vizinhanca_ano_lei,
    safe_cast(demais_instrumentos_codigo_obras_existencia as string) demais_instrumentos_codigo_obras_existencia,
    safe_cast(demais_instrumentos_codigo_obras_ano_lei as int64) demais_instrumentos_codigo_obras_ano_lei,
    safe_cast(
        demais_instrumentos_codigo_obras_normas_acessibilidade as string
    ) demais_instrumentos_codigo_obras_normas_acessibilidade,
    safe_cast(demais_instrumentos_codigo_posturas_existencia as string) demais_instrumentos_codigo_posturas_existencia,
    safe_cast(demais_instrumentos_codigo_posturas_ano_lei as int64) demais_instrumentos_codigo_posturas_ano_lei,
    safe_cast(
        demais_instrumentos_lei_transferencia_direito_construir_existencia as string
    ) demais_instrumentos_lei_transferencia_direito_construir_existencia,
    safe_cast(
        demais_instrumentos_lei_transferencia_direito_construir_ano_lei as int64
    ) demais_instrumentos_lei_transferencia_direito_construir_ano_lei,
    safe_cast(
        demais_instrumentos_lei_iptu_progressivo_existencia as string
    ) demais_instrumentos_lei_iptu_progressivo_existencia,
    safe_cast(demais_instrumentos_lei_iptu_progressivo_ano_lei as int64) demais_instrumentos_lei_iptu_progressivo_ano_lei,
    safe_cast(
        demais_instrumentos_lei_concessao_direito_real_uso_existencia as string
    ) demais_instrumentos_lei_concessao_direito_real_uso_existencia,
    safe_cast(
        demais_instrumentos_lei_concessao_direito_real_uso_ano_lei as int64
    ) demais_instrumentos_lei_concessao_direito_real_uso_ano_lei,
    safe_cast(
        demais_instrumentos_lei_parcelamento_edificacao_utilizacao_compulsorios_existencia as string
    ) demais_instrumentos_lei_parcelamento_edificacao_utilizacao_compulsorios_existencia,
    safe_cast(
        demais_instrumentos_lei_parcelamento_edificacao_utilizacao_compulsorios_ano_lei as int64
    ) demais_instrumentos_lei_parcelamento_edificacao_utilizacao_compulsorios_ano_lei,
    safe_cast(
        demais_instrumentos_lei_desapropriacao_pagamento_titulos_existencia as string
    ) demais_instrumentos_lei_desapropriacao_pagamento_titulos_existencia,
    safe_cast(
        demais_instrumentos_lei_desapropriacao_pagamento_titulos_ano_lei as int64
    ) demais_instrumentos_lei_desapropriacao_pagamento_titulos_ano_lei,
    safe_cast(
        demais_instrumentos_lei_direito_preempcao_existencia as string
    ) demais_instrumentos_lei_direito_preempcao_existencia,
    safe_cast(demais_instrumentos_lei_direito_preempcao_ano_lei as int64) demais_instrumentos_lei_direito_preempcao_ano_lei,
    safe_cast(
        demais_instrumentos_zoneamento_ambiental_existencia as string
    ) demais_instrumentos_zoneamento_ambiental_existencia,
    safe_cast(demais_instrumentos_zoneamento_ambiental_ano_lei as int64) demais_instrumentos_zoneamento_ambiental_ano_lei,
    safe_cast(
        demais_instrumentos_servidao_administrativa_existencia as string
    ) demais_instrumentos_servidao_administrativa_existencia,
    safe_cast(demais_instrumentos_servidao_administrativa_ano_lei as int64) demais_instrumentos_servidao_administrativa_ano_lei,
    safe_cast(demais_instrumentos_tombamento_existencia as string) demais_instrumentos_tombamento_existencia,
    safe_cast(demais_instrumentos_tombamento_ano_lei as int64) demais_instrumentos_tombamento_ano_lei,
    safe_cast(
        demais_instrumentos_unidade_conservacao_existencia as string
    ) demais_instrumentos_unidade_conservacao_existencia,
    safe_cast(demais_instrumentos_unidade_conservacao_ano_lei as int64) demais_instrumentos_unidade_conservacao_ano_lei,
    safe_cast(
        demais_instrumentos_concessao_uso_especial_fins_moradia_existencia as string
    ) demais_instrumentos_concessao_uso_especial_fins_moradia_existencia,
    safe_cast(
        demais_instrumentos_concessao_uso_especial_fins_moradia_ano_lei as int64
    ) demais_instrumentos_concessao_uso_especial_fins_moradia_ano_lei,
    safe_cast(
        demais_instrumentos_usucapiao_especial_imovel_urbano_existencia as string
    ) demais_instrumentos_usucapiao_especial_imovel_urbano_existencia,
    safe_cast(
        demais_instrumentos_usucapiao_especial_imovel_urbano_ano_lei as int64
    ) demais_instrumentos_usucapiao_especial_imovel_urbano_ano_lei,
    safe_cast(demais_instrumentos_direito_superficie_existencia as string) demais_instrumentos_direito_superficie_existencia,
    safe_cast(demais_instrumentos_direito_superficie_ano_lei as int64) demais_instrumentos_direito_superficie_ano_lei,
    safe_cast(
        demais_instrumentos_regularizacao_fundiaria_existencia as string
    ) demais_instrumentos_regularizacao_fundiaria_existencia,
    safe_cast(demais_instrumentos_regularizacao_fundiaria_ano_lei as int64) demais_instrumentos_regularizacao_fundiaria_ano_lei,
    safe_cast(demais_instrumentos_legitimacao_posse_existencia as string) demais_instrumentos_legitimacao_posse_existencia,
    safe_cast(demais_instrumentos_egitimacao_posse_ano_lei as int64) demais_instrumentos_egitimacao_posse_ano_lei,
    safe_cast(
        demais_instrumentos_estudo_previo_impacto_ambiental_existencia as string
    ) demais_instrumentos_estudo_previo_impacto_ambiental_existencia,
    safe_cast(
        demais_instrumentos_estudo_previo_impacto_ambiental_ano_lei as int64
    ) demais_instrumentos_estudo_previo_impacto_ambiental_ano_lei,
    safe_cast(lei_organica_ano_lei as int64) lei_organica_ano_lei,
    safe_cast(lei_diretrizes_orcamentarias as string) lei_diretrizes_orcamentarias,
    safe_cast(lei_orcamento_anual as string) lei_orcamento_anual,
    safe_cast(
        debates_audiencias_consultas_publicas_ppa_ldo_loa as string
    ) debates_audiencias_consultas_publicas_ppa_ldo_loa,
    safe_cast(
        utiliza_instrumentos_politica_urbana_estatuto_cidades as string
    ) utiliza_instrumentos_politica_urbana_estatuto_cidades,
    safe_cast(
        conselho_municipal_politica_urbana_existencia as string
    ) conselho_municipal_politica_urbana_existencia,
    safe_cast(conselho_municipal_politica_urbana_lei_criacao as string) conselho_municipal_politica_urbana_lei_criacao,
    safe_cast(conselho_municipal_politica_urbana_ano_criacao as int64) conselho_municipal_politica_urbana_ano_criacao,
    safe_cast(conselho_municipal_politica_urbana_situacao as string) conselho_municipal_politica_urbana_situacao,
    safe_cast(conselho_municipal_politica_urbana_paridade as string) conselho_municipal_politica_urbana_paridade,
    safe_cast(conselho_municipal_politica_urbana_formacao as string) conselho_municipal_politica_urbana_formacao,
    safe_cast(
        conselho_municipal_politica_urbana_periodicidade_reunioes as string
    ) conselho_municipal_politica_urbana_periodicidade_reunioes,
    safe_cast(
        conselho_municipal_politica_urbana_reuniao_ultimos_12meses as string
    ) conselho_municipal_politica_urbana_reuniao_ultimos_12meses,
    safe_cast(
        conselho_municipal_politica_urbana_reunioes_ultimos_12meses as int64
    ) conselho_municipal_politica_urbana_reunioes_ultimos_12meses,
    safe_cast(
        conselho_municipal_politica_urbana_carater_consultivo as string
    ) conselho_municipal_politica_urbana_carater_consultivo,
    safe_cast(
        conselho_municipal_politica_urbana_carater_deliberativo as string
    ) conselho_municipal_politica_urbana_carater_deliberativo,
    safe_cast(
        conselho_municipal_politica_urbana_carater_normativo as string
    ) conselho_municipal_politica_urbana_carater_normativo,
    safe_cast(
        conselho_municipal_politica_urbana_carater_fiscalizador as string
    ) conselho_municipal_politica_urbana_carater_fiscalizador,
    safe_cast(conselho_municipal_politica_urbana_conselheiros as int64) conselho_municipal_politica_urbana_conselheiros,
    safe_cast(
        conselho_municipal_politica_urbana_capacitacao_membros_periodicamente as string
    ) conselho_municipal_politica_urbana_capacitacao_membros_periodicamente,
    safe_cast(
        conselho_municipal_politica_urbana_capacitacao_membros_ocasionalmente as string
    ) conselho_municipal_politica_urbana_capacitacao_membros_ocasionalmente,
    safe_cast(
        conselho_municipal_politica_urbana_capacitacao_membros_nao_realiza as string
    ) conselho_municipal_politica_urbana_capacitacao_membros_nao_realiza,
    safe_cast(
        conselho_municipal_politica_urbana_infraestrutura as string
    ) conselho_municipal_politica_urbana_infraestrutura,
    safe_cast(
        conselho_municipal_politica_urbana_infraestrutura_sala as string
    ) conselho_municipal_politica_urbana_infraestrutura_sala,
    safe_cast(
        conselho_municipal_politica_urbana_infraestrutura_computador as string
    ) conselho_municipal_politica_urbana_infraestrutura_computador,
    safe_cast(
        conselho_municipal_politica_urbana_infraestrutura_impressora as string
    ) conselho_municipal_politica_urbana_infraestrutura_impressora,
    safe_cast(
        conselho_municipal_politica_urbana_infraestrutura_internet as string
    ) conselho_municipal_politica_urbana_infraestrutura_internet,
    safe_cast(
        conselho_municipal_politica_urbana_infraestrutura_veiculo as string
    ) conselho_municipal_politica_urbana_infraestrutura_veiculo,
    safe_cast(
        conselho_municipal_politica_urbana_infraestrutura_telefone as string
    ) conselho_municipal_politica_urbana_infraestrutura_telefone,
    safe_cast(
        conselho_municipal_politica_urbana_infraestrutura_diarias as string
    ) conselho_municipal_politica_urbana_infraestrutura_diarias,
    safe_cast(
        conselho_municipal_politica_urbana_infraestrutura_dotacao_orcamentaria as string
    ) conselho_municipal_politica_urbana_infraestrutura_dotacao_orcamentaria,
    safe_cast(
        conselho_municipal_politica_urbana_infraestrutura_transporte as string
    ) conselho_municipal_politica_urbana_infraestrutura_transporte,
    safe_cast(fundo_municipal_habitacao_existencia as string) fundo_municipal_habitacao_existencia,
    safe_cast(
        fundo_municipal_habitacao_financiou_acoes_ultimos_12meses as string
    ) fundo_municipal_habitacao_financiou_acoes_ultimos_12meses,
    safe_cast(
        fundo_municipal_habitacao_conselho_gestor_existencia as string
    ) fundo_municipal_habitacao_conselho_gestor_existencia,
    safe_cast(
        fundo_municipal_habitacao_conselho_gestor_conselho_municipal as string
    ) fundo_municipal_habitacao_conselho_gestor_conselho_municipal,
    safe_cast(
        fundo_municipal_habitacao_reune_todos_recursos_orcamentarios as string
    ) fundo_municipal_habitacao_reune_todos_recursos_orcamentarios,
    safe_cast(cadastro_familias_interessadas_existencia as string) cadastro_familias_interessadas_existencia,
    safe_cast(cadastro_familias_interessadas_ano as int64) cadastro_familias_interessadas_ano,
    safe_cast(cadastro_familias_interessadas_informatizado as string) cadastro_familias_interessadas_informatizado,
    safe_cast(
        cadastro_familias_interessadas_inclui_natureza_beneficio as string
    ) cadastro_familias_interessadas_inclui_natureza_beneficio,
    safe_cast(
        cadastro_familias_interessadas_identificacao_idosos as string
    ) cadastro_familias_interessadas_identificacao_idosos,
    safe_cast(
        cadastro_familias_interessadas_identificacao_mulheres_chefes_familia as string
    ) cadastro_familias_interessadas_identificacao_mulheres_chefes_familia,
    safe_cast(
        cadastro_familias_interessadas_identificacao_renda_pc_familia as string
    ) cadastro_familias_interessadas_identificacao_renda_pc_familia,
    safe_cast(
        cadastro_familias_interessadas_identificacao_negros_indigenas as string
    ) cadastro_familias_interessadas_identificacao_negros_indigenas,
    safe_cast(
        cadastro_familias_interessadas_identificacao_deficientes as string
    ) cadastro_familias_interessadas_identificacao_deficientes,
    safe_cast(
        cadastro_familias_interessadas_identificacao_dependentes_familia as string
    ) cadastro_familias_interessadas_identificacao_dependentes_familia,
    safe_cast(
        cadastro_familias_interessadas_identificacao_nenhuma as string
    ) cadastro_familias_interessadas_identificacao_nenhuma,
    safe_cast(
        cadastro_familias_interessadas_criterio_preferencia_existencia as string
    ) cadastro_familias_interessadas_criterio_preferencia_existencia,
    safe_cast(favelas_mocambos_palafitas_existencia as string) favelas_mocambos_palafitas_existencia,
    safe_cast(
        favelas_mocambos_palafitas_criterios_ocupacao_alheia as string
    ) favelas_mocambos_palafitas_criterios_ocupacao_alheia,
    safe_cast(
        favelas_mocambos_palafitas_criterios_maioria_nao_possui_titulo_propriedade as string
    ) favelas_mocambos_palafitas_criterios_maioria_nao_possui_titulo_propriedade,
    safe_cast(
        favelas_mocambos_palafitas_criterios_vias_circulacao_estreitas as string
    ) favelas_mocambos_palafitas_criterios_vias_circulacao_estreitas,
    safe_cast(
        favelas_mocambos_palafitas_criterios_lotes_tamanho_forma_desiguais as string
    ) favelas_mocambos_palafitas_criterios_lotes_tamanho_forma_desiguais,
    safe_cast(
        favelas_mocambos_palafitas_criterios_ocupacao_densa as string
    ) favelas_mocambos_palafitas_criterios_ocupacao_densa,
    safe_cast(
        favelas_mocambos_palafitas_criterios_construcoes_nao_regularizadas as string
    ) favelas_mocambos_palafitas_criterios_construcoes_nao_regularizadas,
    safe_cast(
        favelas_mocambos_palafitas_criterios_precariedade_servicos_publicos as string
    ) favelas_mocambos_palafitas_criterios_precariedade_servicos_publicos,
    safe_cast(
        favelas_mocambos_palafitas_criterios_outros as string
    ) favelas_mocambos_palafitas_criterios_outros,
    safe_cast(corticos_casas_comodos_existencia as string) corticos_casas_comodos_existencia,
    safe_cast(
        corticos_casas_comodos_criterios_varias_familias as string
    ) corticos_casas_comodos_criterios_varias_familias,
    safe_cast(
        corticos_casas_comodos_criterios_uso_comum_agua as string
    ) corticos_casas_comodos_criterios_uso_comum_agua,
    safe_cast(
        corticos_casas_comodos_criterios_comodos_varias_funcoes as string
    ) corticos_casas_comodos_criterios_comodos_varias_funcoes,
    safe_cast(
        corticos_casas_comodos_criterios_construcoes_lotes_urbanos as string
    ) corticos_casas_comodos_criterios_construcoes_lotes_urbanos,
    safe_cast(
        corticos_casas_comodos_criterios_subdivisoes_habitacoes as string
    ) corticos_casas_comodos_criterios_subdivisoes_habitacoes,
    safe_cast(
        corticos_casas_comodos_criterios_unidades_sem_contrato_formal as string
    ) corticos_casas_comodos_criterios_unidades_sem_contrato_formal,
    safe_cast(corticos_casas_comodos_criterios_outros as string) corticos_casas_comodos_criterios_outros,
    safe_cast(loteamentos_irregulares_existencia as string) loteamentos_irregulares_existencia,
    safe_cast(
        loteamentos_irregulares_criterios_sem_aprovacao_previa as string
    ) loteamentos_irregulares_criterios_sem_aprovacao_previa,
    safe_cast(
        loteamentos_irregulares_criterios_descumprimento_normas_legais as string
    ) loteamentos_irregulares_criterios_descumprimento_normas_legais,
    safe_cast(
        loteamentos_irregulares_criterios_falta_titulacao_correta as string
    ) loteamentos_irregulares_criterios_falta_titulacao_correta,
    safe_cast(
        loteamentos_irregulares_criterios_falta_correspondencia_projeto_executado as string
    ) loteamentos_irregulares_criterios_falta_correspondencia_projeto_executado,
    safe_cast(loteamentos_irregulares_criterios_outros as string) loteamentos_irregulares_criterios_outros,
    safe_cast(occupacoes_terreno_predios_existencia as string) occupacoes_terreno_predios_existencia,
    safe_cast(nenhum_acima as string) nenhum_acima,
    safe_cast(programas as string) programas,
    safe_cast(
        programas_construcao_unidades_habitacionais as string
    ) programas_construcao_unidades_habitacionais,
    safe_cast(
        programas_construcao_unidades_habitacionais_iniciativa_prefeitura as string
    ) programas_construcao_unidades_habitacionais_iniciativa_prefeitura,
    safe_cast(
        programas_construcao_unidades_habitacionais_convenio as string
    ) programas_construcao_unidades_habitacionais_convenio,
    safe_cast(
        programas_construcao_unidades_habitacionais_convenio_governo_federal as string
    ) programas_construcao_unidades_habitacionais_convenio_governo_federal,
    safe_cast(
        programas_construcao_unidades_habitacionais_convenio_governo_estadual as string
    ) programas_construcao_unidades_habitacionais_convenio_governo_estadual,
    safe_cast(
        programas_construcao_unidades_habitacionais_convenio_outro_municipio as string
    ) programas_construcao_unidades_habitacionais_convenio_outro_municipio,
    safe_cast(
        programas_construcao_unidades_habitacionais_convenio_iniciativa_privada as string
    ) programas_construcao_unidades_habitacionais_convenio_iniciativa_privada,
    safe_cast(
        programas_construcao_unidades_habitacionais_convenio_outros as string
    ) programas_construcao_unidades_habitacionais_convenio_outros,
    safe_cast(
        programas_construcao_unidades_habitacionais_areas_beneficiadas_urbana as string
    ) programas_construcao_unidades_habitacionais_areas_beneficiadas_urbana,
    safe_cast(
        programas_construcao_unidades_habitacionais_areas_beneficiadas_rural as string
    ) programas_construcao_unidades_habitacionais_areas_beneficiadas_rural,
    safe_cast(
        programas_aquisicao_unidades_habitacionais as string
    ) programas_aquisicao_unidades_habitacionais,
    safe_cast(
        programas_aquisicao_unidades_habitacionais_iniciativa_prefeitura as string
    ) programas_aquisicao_unidades_habitacionais_iniciativa_prefeitura,
    safe_cast(
        programas_aquisicao_unidades_habitacionais_convenio_governo_federal as string
    ) programas_aquisicao_unidades_habitacionais_convenio_governo_federal,
    safe_cast(
        programas_aquisicao_unidades_habitacionais_convenio_governo_estadual as string
    ) programas_aquisicao_unidades_habitacionais_convenio_governo_estadual,
    safe_cast(
        programas_aquisicao_unidades_habitacionais_convenio_outro_municipio as string
    ) programas_aquisicao_unidades_habitacionais_convenio_outro_municipio,
    safe_cast(
        programas_aquisicao_unidades_habitacionais_convenio_iniciativa_privada as string
    ) programas_aquisicao_unidades_habitacionais_convenio_iniciativa_privada,
    safe_cast(
        programas_aquisicao_unidades_habitacionais_convenio_outros as string
    ) programas_aquisicao_unidades_habitacionais_convenio_outros,
    safe_cast(
        programas_aquisicao_unidades_habitacionais_areas_beneficiadas_urbana as string
    ) programas_aquisicao_unidades_habitacionais_areas_beneficiadas_urbana,
    safe_cast(
        programas_aquisicao_unidades_habitacionais_areas_beneficiadas_rural as string
    ) programas_aquisicao_unidades_habitacionais_areas_beneficiadas_rural,
    safe_cast(
        programas_melhorias_unidades_habitacionais as string
    ) programas_melhorias_unidades_habitacionais,
    safe_cast(
        programas_melhorias_unidades_habitacionais_iniciativa_prefeitura as string
    ) programas_melhorias_unidades_habitacionais_iniciativa_prefeitura,
    safe_cast(
        programas_melhorias_unidades_habitacionais_convenio_governo_federal as string
    ) programas_melhorias_unidades_habitacionais_convenio_governo_federal,
    safe_cast(
        programas_melhorias_unidades_habitacionais_convenio_governo_estadual as string
    ) programas_melhorias_unidades_habitacionais_convenio_governo_estadual,
    safe_cast(
        programas_melhorias_unidades_habitacionais_convenio_outro_municipio as string
    ) programas_melhorias_unidades_habitacionais_convenio_outro_municipio,
    safe_cast(
        programas_melhorias_unidades_habitacionais_convenio_iniciativa_privada as string
    ) programas_melhorias_unidades_habitacionais_convenio_iniciativa_privada,
    safe_cast(
        programas_melhorias_unidades_habitacionais_convenio_outros as string
    ) programas_melhorias_unidades_habitacionais_convenio_outros,
    safe_cast(
        programas_melhorias_unidades_habitacionais_areas_beneficiadas_urbana as string
    ) programas_melhorias_unidades_habitacionais_areas_beneficiadas_urbana,
    safe_cast(
        programas_melhorias_unidades_habitacionais_areas_beneficiadas_rural as string
    ) programas_melhorias_unidades_habitacionais_areas_beneficiadas_rural,
    safe_cast(programas_oferta_materiais_construcao as string) programas_oferta_materiais_construcao,
    safe_cast(
        programas_oferta_materiais_construcao_iniciativa_prefeitura as string
    ) programas_oferta_materiais_construcao_iniciativa_prefeitura,
    safe_cast(
        programas_oferta_materiais_construcao_convenio as string
    ) programas_oferta_materiais_construcao_convenio,
    safe_cast(
        programas_oferta_materiais_construcao_convenio_governo_federal as string
    ) programas_oferta_materiais_construcao_convenio_governo_federal,
    safe_cast(
        programas_oferta_materiais_construcao_convenio_governo_estadual as string
    ) programas_oferta_materiais_construcao_convenio_governo_estadual,
    safe_cast(
        programas_oferta_materiais_construcao_convenio_outro_municipio as string
    ) programas_oferta_materiais_construcao_convenio_outro_municipio,
    safe_cast(
        programas_oferta_materiais_construcao_convenio_iniciativa_privada as string
    ) programas_oferta_materiais_construcao_convenio_iniciativa_privada,
    safe_cast(
        programas_oferta_materiais_construcao_convenio_outros as string
    ) programas_oferta_materiais_construcao_convenio_outros,
    safe_cast(
        programas_oferta_materiais_construcao_areas_beneficiadas_urbana as string
    ) programas_oferta_materiais_construcao_areas_beneficiadas_urbana,
    safe_cast(
        programas_oferta_materiais_construcao_areas_beneficiadas_rural as string
    ) programas_oferta_materiais_construcao_areas_beneficiadas_rural,
    safe_cast(programas_oferta_lotes as string) programas_oferta_lotes,
    safe_cast(programas_oferta_lotes_iniciativa_prefeitura as string) programas_oferta_lotes_iniciativa_prefeitura,
    safe_cast(programas_oferta_lotes_convenio as string) programas_oferta_lotes_convenio,
    safe_cast(
        programas_oferta_lotes_convenio_governo_federal as string
    ) programas_oferta_lotes_convenio_governo_federal,
    safe_cast(
        programas_oferta_lotes_convenio_governo_estadual as string
    ) programas_oferta_lotes_convenio_governo_estadual,
    safe_cast(
        programas_oferta_lotes_convenio_outro_municipio as string
    ) programas_oferta_lotes_convenio_outro_municipio,
    safe_cast(
        programas_oferta_lotes_convenio_iniciativa_privada as string
    ) programas_oferta_lotes_convenio_iniciativa_privada,
    safe_cast(programas_oferta_lotes_convenio_outros as string) programas_oferta_lotes_convenio_outros,
    safe_cast(
        programas_oferta_lotes_areas_beneficiadas_urbana as string
    ) programas_oferta_lotes_areas_beneficiadas_urbana,
    safe_cast(
        programas_oferta_lotes_areas_beneficiadas_rural as string
    ) programas_oferta_lotes_areas_beneficiadas_rural,
    safe_cast(programas_oferta_lotes_lotes_urbanizados as string) programas_oferta_lotes_lotes_urbanizados,
    safe_cast(programas_oferta_lotes_lotes_nao_urbanizados as string) programas_oferta_lotes_lotes_nao_urbanizados,
    safe_cast(programas_regularizacao_fundiaria as string) programas_regularizacao_fundiaria,
    safe_cast(
        programas_regularizacao_fundiaria_iniciativa_prefeitura as string
    ) programas_regularizacao_fundiaria_iniciativa_prefeitura,
    safe_cast(programas_regularizacao_fundiaria_convenio as string) programas_regularizacao_fundiaria_convenio,
    safe_cast(
        programas_regularizacao_fundiaria_convenio_governo_federal as string
    ) programas_regularizacao_fundiaria_convenio_governo_federal,
    safe_cast(
        programas_regularizacao_fundiaria_convenio_governo_estadual as string
    ) programas_regularizacao_fundiaria_convenio_governo_estadual,
    safe_cast(
        programas_regularizacao_fundiaria_convenio_outro_municipio as string
    ) programas_regularizacao_fundiaria_convenio_outro_municipio,
    safe_cast(
        programas_regularizacao_fundiaria_convenio_iniciativa_privada as string
    ) programas_regularizacao_fundiaria_convenio_iniciativa_privada,
    safe_cast(
        programas_regularizacao_fundiaria_convenio_outros as string
    ) programas_regularizacao_fundiaria_convenio_outros,
    safe_cast(
        programas_regularizacao_fundiaria_areas_beneficiadas_urbana as string
    ) programas_regularizacao_fundiaria_areas_beneficiadas_urbana,
    safe_cast(
        programas_regularizacao_fundiaria_areas_beneficiadas_rural as string
    ) programas_regularizacao_fundiaria_areas_beneficiadas_rural,
    safe_cast(
        programas_regularizacao_fundiaria_foram_beneficiados_loteamentos_irregulares as string
    ) programas_regularizacao_fundiaria_foram_beneficiados_loteamentos_irregulares,
    safe_cast(
        programas_regularizacao_fundiaria_foram_beneficiados_favelas_mocambos_palafitas as string
    ) programas_regularizacao_fundiaria_foram_beneficiados_favelas_mocambos_palafitas,
    safe_cast(
        programas_regularizacao_fundiaria_foram_beneficiados_conjuntos_habitacionais as string
    ) programas_regularizacao_fundiaria_foram_beneficiados_conjuntos_habitacionais,
    safe_cast(
        programas_regularizacao_fundiaria_foram_beneficiados_bairros_consolidados as string
    ) programas_regularizacao_fundiaria_foram_beneficiados_bairros_consolidados,
    safe_cast(
        programas_regularizacao_fundiaria_foram_beneficiados_corticos as string
    ) programas_regularizacao_fundiaria_foram_beneficiados_corticos,
    safe_cast(
        programas_regularizacao_fundiaria_foram_beneficiados_outros as string
    ) programas_regularizacao_fundiaria_foram_beneficiados_outros,
    safe_cast(programas_urbanizacao_assentamentos as string) programas_urbanizacao_assentamentos,
    safe_cast(
        programas_urbanizacao_assentamentos_iniciativa_prefeitura as string
    ) programas_urbanizacao_assentamentos_iniciativa_prefeitura,
    safe_cast(programas_urbanizacao_assentamentos_convenio as string) programas_urbanizacao_assentamentos_convenio,
    safe_cast(
        programas_urbanizacao_assentamentos_convenio_governo_federal as string
    ) programas_urbanizacao_assentamentos_convenio_governo_federal,
    safe_cast(
        programas_urbanizacao_assentamentos_convenio_governo_estadual as string
    ) programas_urbanizacao_assentamentos_convenio_governo_estadual,
    safe_cast(
        programas_urbanizacao_assentamentos_convenio_outro_municipio as string
    ) programas_urbanizacao_assentamentos_convenio_outro_municipio,
    safe_cast(
        programas_urbanizacao_assentamentos_convenio_iniciativa_privada as string
    ) programas_urbanizacao_assentamentos_convenio_iniciativa_privada,
    safe_cast(
        programas_urbanizacao_assentamentos_convenio_outros as string
    ) programas_urbanizacao_assentamentos_convenio_outros,
    safe_cast(
        programas_urbanizacao_assentamentos_areas_beneficiadas_urbana as string
    ) programas_urbanizacao_assentamentos_areas_beneficiadas_urbana,
    safe_cast(
        programas_urbanizacao_assentamentos_areas_beneficiadas_rural as string
    ) programas_urbanizacao_assentamentos_areas_beneficiadas_rural,
    safe_cast(programas_regularizacao_loteamento as string) programas_regularizacao_loteamento,
    safe_cast(programas_outros as string) programas_outros,
    safe_cast(programas_outros_iniciativa_prefeitura as string) programas_outros_iniciativa_prefeitura,
    safe_cast(programas_outros_convenio as string) programas_outros_convenio,
    safe_cast(programas_outros_areas_beneficiadas_urbana as string) programas_outros_areas_beneficiadas_urbana,
    safe_cast(programas_outros_areas_beneficiadas_rural as string) programas_outros_areas_beneficiadas_rural,
    safe_cast(programas_nenhum as string) programas_nenhum,
    safe_cast(programas_concede_beneficio_aluguel_social as string) programas_concede_beneficio_aluguel_social,
    safe_cast(
        programas_recursos_fora_fundo_municipal_habitacao as string
    ) programas_recursos_fora_fundo_municipal_habitacao,
    safe_cast(emitiu_licencas_novos_loteamentos as string) emitiu_licencas_novos_loteamentos,
    safe_cast(emitiu_licencas_construcao as string) emitiu_licencas_construcao,
    safe_cast(emitiu_alvaras_habitacao as string) emitiu_alvaras_habitacao,
    safe_cast(emitiu_nada as string) emitiu_nada,
    safe_cast(articulacao_consorcio_publico_intermunicipal as string) articulacao_consorcio_publico_intermunicipal,
    safe_cast(articulacao_consorcio_publico_estado as string) articulacao_consorcio_publico_estado,
    safe_cast(articulacao_consorcio_publico_uniao as string) articulacao_consorcio_publico_uniao,
    safe_cast(articulacao_convenio_setor_privado as string) articulacao_convenio_setor_privado,
    safe_cast(articulacao_apoio_setor_privado as string) articulacao_apoio_setor_privado,
    safe_cast(integra_aglomeracao_urbana as string) integra_aglomeracao_urbana,
    safe_cast(integra_area_interesse_turistico as string) integra_area_interesse_turistico,
    safe_cast(integra_area_influencia_empreendimentos as string) integra_area_influencia_empreendimentos,
    safe_cast(integra_nada as string) integra_nada,
    safe_cast(
        regularizacao_fundiaria_responsabilidade_gestor_habitacao as string
    ) regularizacao_fundiaria_responsabilidade_gestor_habitacao,
    safe_cast(
        regularizacao_fundiaria_instrumentos_usucapiao_urbano_individual as string
    ) regularizacao_fundiaria_instrumentos_usucapiao_urbano_individual,
    safe_cast(
        regularizacao_fundiaria_instrumentos_usucapiao_urbano_coletivo as string
    ) regularizacao_fundiaria_instrumentos_usucapiao_urbano_coletivo,
    safe_cast(
        regularizacao_fundiaria_instrumentos_direito_superficie as string
    ) regularizacao_fundiaria_instrumentos_direito_superficie,
    safe_cast(
        regularizacao_fundiaria_instrumentos_concessao_direito_real_uso_gratuito_individual as string
    ) regularizacao_fundiaria_instrumentos_concessao_direito_real_uso_gratuito_individual,
    safe_cast(
        regularizacao_fundiaria_instrumentos_concessao_direito_real_uso_gratuito_coletivo as string
    ) regularizacao_fundiaria_instrumentos_concessao_direito_real_uso_gratuito_coletivo,
    safe_cast(
        regularizacao_fundiaria_instrumentos_concessao_direito_real_uso_oneroso_individual as string
    ) regularizacao_fundiaria_instrumentos_concessao_direito_real_uso_oneroso_individual,
    safe_cast(
        regularizacao_fundiaria_instrumentos_concessao_direito_real_uso_oneroso_coletivo as string
    ) regularizacao_fundiaria_instrumentos_concessao_direito_real_uso_oneroso_coletivo,
    safe_cast(
        regularizacao_fundiaria_instrumentos_concessao_especial_uso_moradia_individual as string
    ) regularizacao_fundiaria_instrumentos_concessao_especial_uso_moradia_individual,
    safe_cast(
        regularizacao_fundiaria_instrumentos_concessao_especial_uso_moradia_coletiva as string
    ) regularizacao_fundiaria_instrumentos_concessao_especial_uso_moradia_coletiva,
    safe_cast(
        regularizacao_fundiaria_instrumentos_autorizacao_uso as string
    ) regularizacao_fundiaria_instrumentos_autorizacao_uso,
    safe_cast(
        regularizacao_fundiaria_instrumentos_contrato_compra_venda as string
    ) regularizacao_fundiaria_instrumentos_contrato_compra_venda,
    safe_cast(regularizacao_fundiaria_instrumentos_outros as string) regularizacao_fundiaria_instrumentos_outros
from {{ set_datalake_project("br_ibge_munic_staging.habitacao") }} as t
