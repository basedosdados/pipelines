{{ config(alias="recursos_gestao", schema="br_ibge_munic", materialized="table") }}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(
        iptu_cadastro_imobiliario_existencia as string
    ) iptu_cadastro_imobiliario_existencia,
    safe_cast(
        iptu_cadastro_imobiliario_informatizado_existencia as string
    ) iptu_cadastro_imobiliario_informatizado_existencia,
    safe_cast(
        iptu_cadastro_imobiliario_informatizado_unidades_prediais_territoriais as string
    ) iptu_cadastro_imobiliario_informatizado_unidades_prediais_territoriais,
    safe_cast(
        iptu_cadastro_imobiliario_informatizado_total_unidades_prediais as string
    ) iptu_cadastro_imobiliario_informatizado_total_unidades_prediais,
    safe_cast(
        iptu_cadastro_imobiliario_informatizado_total_unidades_territoriais as string
    ) iptu_cadastro_imobiliario_informatizado_total_unidades_territoriais,
    safe_cast(
        iptu_cadastro_imobiliario_georrefenciado_existencia as string
    ) iptu_cadastro_imobiliario_georrefenciado_existencia,
    safe_cast(
        iptu_cadastro_imobiliario_publico as string
    ) iptu_cadastro_imobiliario_publico,
    safe_cast(
        iptu_cadastro_imobiliario_atualizacao_anual_menor as string
    ) iptu_cadastro_imobiliario_atualizacao_anual_menor,
    safe_cast(
        iptu_cadastro_imobiliario_atualizacao_maior as string
    ) iptu_cadastro_imobiliario_atualizacao_maior,
    safe_cast(
        iptu_cadastro_imobiliario_atualizacao_sob_demanda as string
    ) iptu_cadastro_imobiliario_atualizacao_sob_demanda,
    safe_cast(
        iptu_planta_valores_ano_ultima_atualizacao as int64
    ) iptu_planta_valores_ano_ultima_atualizacao,
    safe_cast(iptu_ultimo_recadastramento_ano as int64) iptu_ultimo_recadastramento_ano,
    safe_cast(iptu_municipio_cobra as string) iptu_municipio_cobra,
    safe_cast(iptu_municipio_cobra_ano_lei as int64) iptu_municipio_cobra_ano_lei,
    safe_cast(
        iptu_municipio_cobra_numero_lei as string
    ) iptu_municipio_cobra_numero_lei,
    safe_cast(
        iptu_planta_generica_valores_existencia as string
    ) iptu_planta_generica_valores_existencia,
    safe_cast(
        iptu_planta_generica_valores_informatizada_existencia as string
    ) iptu_planta_generica_valores_informatizada_existencia,
    safe_cast(
        iptu_planta_generica_valores_ano_atualizada_ultimos_10anos as string
    ) iptu_planta_generica_valores_ano_atualizada_ultimos_10anos,
    safe_cast(
        iptu_planta_generica_valores_ano_ultima_atualizacao as int64
    ) iptu_planta_generica_valores_ano_ultima_atualizacao,
    safe_cast(
        iss_cadastro_prestadores_servico_existencia as string
    ) iss_cadastro_prestadores_servico_existencia,
    safe_cast(
        iss_cadastro_prestadores_servico_informatizado as string
    ) iss_cadastro_prestadores_servico_informatizado,
    safe_cast(
        iss_cadastro_prestadores_servico_ano_ultimo_recadastramento as int64
    ) iss_cadastro_prestadores_servico_ano_ultimo_recadastramento,
    safe_cast(
        iss_contribuintes_inscritos_empresas as string
    ) iss_contribuintes_inscritos_empresas,
    safe_cast(
        iss_contribuintes_inscritos_outros as string
    ) iss_contribuintes_inscritos_outros,
    safe_cast(issqn_cadastro_existencia as string) issqn_cadastro_existencia,
    safe_cast(
        issqn_cadastro_informatizado_existencia as string
    ) issqn_cadastro_informatizado_existencia,
    safe_cast(
        issqn_cadastro_ano_ultima_atualizacao as int64
    ) issqn_cadastro_ano_ultima_atualizacao,
    safe_cast(taxas_existencia as string) taxas_existencia,
    safe_cast(taxas_iluminacao_existencia as string) taxas_iluminacao_existencia,
    safe_cast(taxas_coleta_lixo_existencia as string) taxas_coleta_lixo_existencia,
    safe_cast(
        taxas_limpeza_urbana_existencia as string
    ) taxas_limpeza_urbana_existencia,
    safe_cast(
        taxas_incendio_combate_sinistros_existencia as string
    ) taxas_incendio_combate_sinistros_existencia,
    safe_cast(taxas_poder_policia_existencia as string) taxas_poder_policia_existencia,
    safe_cast(taxas_outras_existencia as string) taxas_outras_existencia,
    safe_cast(taxas_nao_cobra as string) taxas_nao_cobra,
    safe_cast(
        mecanismos_incentivos_existencia as string
    ) mecanismos_incentivos_existencia,
    safe_cast(
        mecanismos_incentivos_ultimos_24meses_reducao_iptu as string
    ) mecanismos_incentivos_ultimos_24meses_reducao_iptu,
    safe_cast(
        mecanismos_incentivos_ultimos_24meses_isencao_iptu as string
    ) mecanismos_incentivos_ultimos_24meses_isencao_iptu,
    safe_cast(
        mecanismos_incentivos_ultimos_24meses_reducao_issqn as string
    ) mecanismos_incentivos_ultimos_24meses_reducao_issqn,
    safe_cast(
        mecanismos_incentivos_ultimos_24meses_isencao_issqn as string
    ) mecanismos_incentivos_ultimos_24meses_isencao_issqn,
    safe_cast(
        mecanismos_incentivos_ultimos_24meses_isencao_taxas as string
    ) mecanismos_incentivos_ultimos_24meses_isencao_taxas,
    safe_cast(
        mecanismos_incentivos_ultimos_24meses_cessao_terrenos as string
    ) mecanismos_incentivos_ultimos_24meses_cessao_terrenos,
    safe_cast(
        mecanismos_incentivos_ultimos_24meses_doacao_terrenos as string
    ) mecanismos_incentivos_ultimos_24meses_doacao_terrenos,
    safe_cast(
        mecanismos_incentivos_ultimos_24meses_outros as string
    ) mecanismos_incentivos_ultimos_24meses_outros,
    safe_cast(
        mecanismos_incentivos_ultimos_24meses_nenhum as string
    ) mecanismos_incentivos_ultimos_24meses_nenhum,
    safe_cast(
        mecanismos_incentivos_tipo_beneficiado_ultimos_24meses_industrial as string
    ) mecanismos_incentivos_tipo_beneficiado_ultimos_24meses_industrial,
    safe_cast(
        mecanismos_incentivos_tipo_beneficiado_ultimos_24meses_comercial_servicos
        as string
    ) mecanismos_incentivos_tipo_beneficiado_ultimos_24meses_comercial_servicos,
    safe_cast(
        mecanismos_incentivos_tipo_beneficiado_ultimos_24meses_turismo_esporte_lazer
        as string
    ) mecanismos_incentivos_tipo_beneficiado_ultimos_24meses_turismo_esporte_lazer,
    safe_cast(
        mecanismos_incentivos_tipo_beneficiado_ultimos_24meses_agropecuario as string
    ) mecanismos_incentivos_tipo_beneficiado_ultimos_24meses_agropecuario,
    safe_cast(
        mecanismos_incentivos_tipo_beneficiado_ultimos_24meses_outros as string
    ) mecanismos_incentivos_tipo_beneficiado_ultimos_24meses_outros,
    safe_cast(
        mecanismos_incentivos_tipo_beneficiado_ano_anterior_industrial as string
    ) mecanismos_incentivos_tipo_beneficiado_ano_anterior_industrial,
    safe_cast(
        mecanismos_incentivos_tipo_beneficiado_ano_anterior_comercial_servicos as string
    ) mecanismos_incentivos_tipo_beneficiado_ano_anterior_comercial_servicos,
    safe_cast(
        mecanismos_incentivos_tipo_beneficiado_ano_anterior_turismo_esporte_lazer
        as string
    ) mecanismos_incentivos_tipo_beneficiado_ano_anterior_turismo_esporte_lazer,
    safe_cast(
        mecanismos_incentivos_tipo_beneficiado_ano_anterior_agropecuario as string
    ) mecanismos_incentivos_tipo_beneficiado_ano_anterior_agropecuario,
    safe_cast(
        mecanismos_incentivos_tipo_beneficiado_ano_anterior_outros as string
    ) mecanismos_incentivos_tipo_beneficiado_ano_anterior_outros,
    safe_cast(
        mecanismos_incentivos_tipo_beneficiado_ano_anterior_nenhum as string
    ) mecanismos_incentivos_tipo_beneficiado_ano_anterior_nenhum,
    safe_cast(
        mecanismos_restricao_existencia as string
    ) mecanismos_restricao_existencia,
    safe_cast(
        mecanismos_restricao_ultimos_24meses_legislacao as string
    ) mecanismos_restricao_ultimos_24meses_legislacao,
    safe_cast(
        mecanismos_restricao_ultimos_24meses_tributacao as string
    ) mecanismos_restricao_ultimos_24meses_tributacao,
    safe_cast(
        mecanismos_restricao_ultimos_24meses_outros as string
    ) mecanismos_restricao_ultimos_24meses_outros,
    safe_cast(
        mecanismos_restricao_ultimos_24meses_nenhum as string
    ) mecanismos_restricao_ultimos_24meses_nenhum,
    safe_cast(
        mecanismos_restricao_tipo_aplicado_ultimos_24meses_industria_poluidora as string
    ) mecanismos_restricao_tipo_aplicado_ultimos_24meses_industria_poluidora,
    safe_cast(
        mecanismos_restricao_tipo_aplicado_ultimos_24meses_industria_extrativa as string
    ) mecanismos_restricao_tipo_aplicado_ultimos_24meses_industria_extrativa,
    safe_cast(
        mecanismos_restricao_tipo_aplicado_ultimos_24meses_comercial_servicos as string
    ) mecanismos_restricao_tipo_aplicado_ultimos_24meses_comercial_servicos,
    safe_cast(
        mecanismos_restricao_tipo_aplicado_ultimos_24meses_turismo_esporte_lazer
        as string
    ) mecanismos_restricao_tipo_aplicado_ultimos_24meses_turismo_esporte_lazer,
    safe_cast(
        mecanismos_restricao_tipo_aplicado_ultimos_24meses_empreendimentos_poluidores
        as string
    ) mecanismos_restricao_tipo_aplicado_ultimos_24meses_empreendimentos_poluidores,
    safe_cast(
        mecanismos_restricao_tipo_aplicado_ultimos_24meses_outros as string
    ) mecanismos_restricao_tipo_aplicado_ultimos_24meses_outros,
    safe_cast(
        mecanismos_restricao_tipo_aplicado_ano_anterior_industria_poluidora as string
    ) mecanismos_restricao_tipo_aplicado_ano_anterior_industria_poluidora,
    safe_cast(
        mecanismos_restricao_tipo_aplicado_ano_anterior_industria_extrativa as string
    ) mecanismos_restricao_tipo_aplicado_ano_anterior_industria_extrativa,
    safe_cast(
        mecanismos_restricao_tipo_aplicado_ano_anterior_comercial_servicos as string
    ) mecanismos_restricao_tipo_aplicado_ano_anterior_comercial_servicos,
    safe_cast(
        mecanismos_restricao_tipo_aplicado_ano_anterior_turismo_esporte_lazer as string
    ) mecanismos_restricao_tipo_aplicado_ano_anterior_turismo_esporte_lazer,
    safe_cast(
        mecanismos_restricao_tipo_aplicado_ano_anterior_empreendimentos_poluidores
        as string
    ) mecanismos_restricao_tipo_aplicado_ano_anterior_empreendimentos_poluidores,
    safe_cast(
        mecanismos_restricao_tipo_aplicado_ano_anterior_outros as string
    ) mecanismos_restricao_tipo_aplicado_ano_anterior_outros,
    safe_cast(
        mecanismos_restricao_tipo_aplicado_ano_anterior_nenhum as string
    ) mecanismos_restricao_tipo_aplicado_ano_anterior_nenhum,
    safe_cast(
        distrito_industrial_regulamentado_existencia as string
    ) distrito_industrial_regulamentado_existencia,
    safe_cast(
        distrito_industrial_regulamentado_distritos_implantados as string
    ) distrito_industrial_regulamentado_distritos_implantados,
    safe_cast(
        distrito_industrial_regulamentado_distritos_obras as string
    ) distrito_industrial_regulamentado_distritos_obras,
    safe_cast(
        programas_geracao_trabalho_renda_existencia as string
    ) programas_geracao_trabalho_renda_existencia,
    safe_cast(
        programas_geracao_trabalho_renda_publico_alvo_adolescentes as string
    ) programas_geracao_trabalho_renda_publico_alvo_adolescentes,
    safe_cast(
        programas_geracao_trabalho_renda_publico_alvo_jovens as string
    ) programas_geracao_trabalho_renda_publico_alvo_jovens,
    safe_cast(
        programas_geracao_trabalho_renda_publico_alvo_indigenas as string
    ) programas_geracao_trabalho_renda_publico_alvo_indigenas,
    safe_cast(
        programas_geracao_trabalho_renda_publico_alvo_deficientes as string
    ) programas_geracao_trabalho_renda_publico_alvo_deficientes,
    safe_cast(
        programas_geracao_trabalho_renda_publico_alvo_idosos as string
    ) programas_geracao_trabalho_renda_publico_alvo_idosos,
    safe_cast(
        programas_geracao_trabalho_renda_publico_alvo_populacao_baixa_renda as string
    ) programas_geracao_trabalho_renda_publico_alvo_populacao_baixa_renda,
    safe_cast(
        programas_geracao_trabalho_renda_publico_alvo_populacao_residente as string
    ) programas_geracao_trabalho_renda_publico_alvo_populacao_residente,
    safe_cast(
        programas_geracao_trabalho_renda_publico_alvo_outros as string
    ) programas_geracao_trabalho_renda_publico_alvo_outros,
    safe_cast(
        politica_primeiro_emprego_jovens_adolescentes_existencia as string
    ) politica_primeiro_emprego_jovens_adolescentes_existencia,
    safe_cast(
        arranjo_produtivo_local_existencia as string
    ) arranjo_produtivo_local_existencia,
    safe_cast(
        arranjo_produtivo_local_outros_municipios as string
    ) arranjo_produtivo_local_outros_municipios,
    safe_cast(territorios_cidadania as string) territorios_cidadania,
    safe_cast(desestatizacao_ultimos_24meses as string) desestatizacao_ultimos_24meses,
    safe_cast(
        desestatizacao_ultimos_24meses_forma_venda_ativos_imobiliarios as string
    ) desestatizacao_ultimos_24meses_forma_venda_ativos_imobiliarios,
    safe_cast(
        desestatizacao_ultimos_24meses_forma_privatizacao as string
    ) desestatizacao_ultimos_24meses_forma_privatizacao,
    safe_cast(
        desestatizacao_ultimos_24meses_forma_concessao as string
    ) desestatizacao_ultimos_24meses_forma_concessao,
    safe_cast(
        desestatizacao_ultimos_24meses_forma_concessao_comum as string
    ) desestatizacao_ultimos_24meses_forma_concessao_comum,
    safe_cast(
        desestatizacao_ultimos_24meses_forma_concessao_ppp as string
    ) desestatizacao_ultimos_24meses_forma_concessao_ppp,
    safe_cast(
        desestatizacao_ultimos_24meses_forma_concessao_ppp_administrativa as string
    ) desestatizacao_ultimos_24meses_forma_concessao_ppp_administrativa,
    safe_cast(
        desestatizacao_ultimos_24meses_forma_concessao_ppp_patrocinada as string
    ) desestatizacao_ultimos_24meses_forma_concessao_ppp_patrocinada,
    safe_cast(
        desestatizacao_ultimos_24meses_area_cultura_esporte_lazer_turismo as string
    ) desestatizacao_ultimos_24meses_area_cultura_esporte_lazer_turismo,
    safe_cast(
        desestatizacao_ultimos_24meses_area_educacao as string
    ) desestatizacao_ultimos_24meses_area_educacao,
    safe_cast(
        desestatizacao_ultimos_24meses_area_manutencao_reparo_logradouros as string
    ) desestatizacao_ultimos_24meses_area_manutencao_reparo_logradouros,
    safe_cast(
        desestatizacao_ultimos_24meses_area_saneamento_basico as string
    ) desestatizacao_ultimos_24meses_area_saneamento_basico,
    safe_cast(
        desestatizacao_ultimos_24meses_area_saude as string
    ) desestatizacao_ultimos_24meses_area_saude,
    safe_cast(
        desestatizacao_ultimos_24meses_area_servicos_funerarios as string
    ) desestatizacao_ultimos_24meses_area_servicos_funerarios,
    safe_cast(
        desestatizacao_ultimos_24meses_area_transporte as string
    ) desestatizacao_ultimos_24meses_area_transporte,
    safe_cast(
        desestatizacao_ultimos_24meses_area_outros as string
    ) desestatizacao_ultimos_24meses_area_outros,
    safe_cast(
        articulacao_desenvolvimento_urbano_consorcio_publico_intermunicipal as string
    ) articulacao_desenvolvimento_urbano_consorcio_publico_intermunicipal,
    safe_cast(
        articulacao_desenvolvimento_urbano_consorcio_publico_estado as string
    ) articulacao_desenvolvimento_urbano_consorcio_publico_estado,
    safe_cast(
        articulacao_desenvolvimento_urbano_consorcio_publico_uniao as string
    ) articulacao_desenvolvimento_urbano_consorcio_publico_uniao,
    safe_cast(
        articulacao_desenvolvimento_urbano_consorcio_administrativo_intermunicipal
        as string
    ) articulacao_desenvolvimento_urbano_consorcio_administrativo_intermunicipal,
    safe_cast(
        articulacao_desenvolvimento_urbano_consorcio_administrativo_estado as string
    ) articulacao_desenvolvimento_urbano_consorcio_administrativo_estado,
    safe_cast(
        articulacao_desenvolvimento_urbano_consorcio_administrativo_uniao as string
    ) articulacao_desenvolvimento_urbano_consorcio_administrativo_uniao,
    safe_cast(
        articulacao_desenvolvimento_urbano_convenio_setor_privado as string
    ) articulacao_desenvolvimento_urbano_convenio_setor_privado,
    safe_cast(
        articulacao_desenvolvimento_urbano_apoio_setor_privado_comunidades as string
    ) articulacao_desenvolvimento_urbano_apoio_setor_privado_comunidades,
    safe_cast(
        articulacao_politica_emprego_consorcio_publico_intermunicipal as string
    ) articulacao_politica_emprego_consorcio_publico_intermunicipal,
    safe_cast(
        articulacao_politica_emprego_consorcio_publico_estado as string
    ) articulacao_politica_emprego_consorcio_publico_estado,
    safe_cast(
        articulacao_politica_emprego_consorcio_publico_uniao as string
    ) articulacao_politica_emprego_consorcio_publico_uniao,
    safe_cast(
        articulacao_politica_emprego_consorcio_administrativo_intermunicipal as string
    ) articulacao_politica_emprego_consorcio_administrativo_intermunicipal,
    safe_cast(
        articulacao_politica_emprego_consorcio_administrativo_estado as string
    ) articulacao_politica_emprego_consorcio_administrativo_estado,
    safe_cast(
        articulacao_politica_emprego_consorcio_administrativo_uniao as string
    ) articulacao_politica_emprego_consorcio_administrativo_uniao,
    safe_cast(
        articulacao_politica_emprego_convenio_setor_privado as string
    ) articulacao_politica_emprego_convenio_setor_privado,
    safe_cast(
        articulacao_politica_emprego_apoio_setor_privado_comunidades as string
    ) articulacao_politica_emprego_apoio_setor_privado_comunidades,
    safe_cast(
        articulacao_educacao_consorcio_publico_intermunicipal as string
    ) articulacao_educacao_consorcio_publico_intermunicipal,
    safe_cast(
        articulacao_educacao_consorcio_publico_estado as string
    ) articulacao_educacao_consorcio_publico_estado,
    safe_cast(
        articulacao_educacao_consorcio_publico_uniao as string
    ) articulacao_educacao_consorcio_publico_uniao,
    safe_cast(
        articulacao_educacao_consorcio_administrativo_intermunicipal as string
    ) articulacao_educacao_consorcio_administrativo_intermunicipal,
    safe_cast(
        articulacao_educacao_consorcio_administrativo_estado as string
    ) articulacao_educacao_consorcio_administrativo_estado,
    safe_cast(
        articulacao_educacao_consorcio_administrativo_uniao as string
    ) articulacao_educacao_consorcio_administrativo_uniao,
    safe_cast(
        articulacao_educacao_convenio_setor_privado as string
    ) articulacao_educacao_convenio_setor_privado,
    safe_cast(
        articulacao_educacao_apoio_setor_privado_comunidades as string
    ) articulacao_educacao_apoio_setor_privado_comunidades,
    safe_cast(
        articulacao_saude_consorcio_publico_intermunicipal as string
    ) articulacao_saude_consorcio_publico_intermunicipal,
    safe_cast(
        articulacao_saude_consorcio_publico_estado as string
    ) articulacao_saude_consorcio_publico_estado,
    safe_cast(
        articulacao_saude_consorcio_publico_uniao as string
    ) articulacao_saude_consorcio_publico_uniao,
    safe_cast(
        articulacao_saude_consorcio_admnistrativo_intermunicipal as string
    ) articulacao_saude_consorcio_admnistrativo_intermunicipal,
    safe_cast(
        articulacao_saude_consorcio_admnistrativo_estado as string
    ) articulacao_saude_consorcio_admnistrativo_estado,
    safe_cast(
        articulacao_saude_consorcio_admnistrativo_uniao as string
    ) articulacao_saude_consorcio_admnistrativo_uniao,
    safe_cast(
        articulacao_saude_convenio_setor_privado as string
    ) articulacao_saude_convenio_setor_privado,
    safe_cast(
        articulacao_saude_apoio_setor_privado_comunidades as string
    ) articulacao_saude_apoio_setor_privado_comunidades,
    safe_cast(
        articulacao_assistencia_social_consorcio_publico_intermunicipal as string
    ) articulacao_assistencia_social_consorcio_publico_intermunicipal,
    safe_cast(
        articulacao_assistencia_social_consorcio_publico_estado as string
    ) articulacao_assistencia_social_consorcio_publico_estado,
    safe_cast(
        articulacao_assistencia_social_consorcio_publico_uniao as string
    ) articulacao_assistencia_social_consorcio_publico_uniao,
    safe_cast(
        articulacao_assistencia_social_consorcio_administrativo_intermunicipal as string
    ) articulacao_assistencia_social_consorcio_administrativo_intermunicipal,
    safe_cast(
        articulacao_assistencia_social_consorcio_administrativo_estado as string
    ) articulacao_assistencia_social_consorcio_administrativo_estado,
    safe_cast(
        articulacao_assistencia_social_consorcio_administrativo_uniao as string
    ) articulacao_assistencia_social_consorcio_administrativo_uniao,
    safe_cast(
        articulacao_assistencia_social_convenio_setor_privado as string
    ) articulacao_assistencia_social_convenio_setor_privado,
    safe_cast(
        articulacao_assistencia_social_apoio_setor_privado_comunidades as string
    ) articulacao_assistencia_social_apoio_setor_privado_comunidades,
    safe_cast(
        articulacao_direito_crianca_adolescente_consorcio_publico_intermunicipal
        as string
    ) articulacao_direito_crianca_adolescente_consorcio_publico_intermunicipal,
    safe_cast(
        articulacao_direito_crianca_adolescente_consorcio_publico_estado as string
    ) articulacao_direito_crianca_adolescente_consorcio_publico_estado,
    safe_cast(
        articulacao_direito_crianca_adolescente_consorcio_publico_uniao as string
    ) articulacao_direito_crianca_adolescente_consorcio_publico_uniao,
    safe_cast(
        articulacao_direito_crianca_adolescente_convenio_setor_privado as string
    ) articulacao_direito_crianca_adolescente_convenio_setor_privado,
    safe_cast(
        articulacao_direito_crianca_adolescente_apoio_setor_privado_comunidades
        as string
    ) articulacao_direito_crianca_adolescente_apoio_setor_privado_comunidades,
    safe_cast(
        articulacao_turismo_consorcio_publico_intermunicipal as string
    ) articulacao_turismo_consorcio_publico_intermunicipal,
    safe_cast(
        articulacao_turismo_consorcio_publico_estado as string
    ) articulacao_turismo_consorcio_publico_estado,
    safe_cast(
        articulacao_turismo_consorcio_publico_uniao as string
    ) articulacao_turismo_consorcio_publico_uniao,
    safe_cast(
        articulacao_turismo_consorcio_administrativo_intermunicipal as string
    ) articulacao_turismo_consorcio_administrativo_intermunicipal,
    safe_cast(
        articulacao_turismo_consorcio_administrativo_estado as string
    ) articulacao_turismo_consorcio_administrativo_estado,
    safe_cast(
        articulacao_turismo_consorcio_administrativo_uniao as string
    ) articulacao_turismo_consorcio_administrativo_uniao,
    safe_cast(
        articulacao_turismo_convenio_setor_privado as string
    ) articulacao_turismo_convenio_setor_privado,
    safe_cast(
        articulacao_turismo_apoio_setor_privado_comunidades as string
    ) articulacao_turismo_apoio_setor_privado_comunidades,
    safe_cast(
        articulacao_cultura_consorcio_publico_intermunicipal as string
    ) articulacao_cultura_consorcio_publico_intermunicipal,
    safe_cast(
        articulacao_cultura_consorcio_publico_estado as string
    ) articulacao_cultura_consorcio_publico_estado,
    safe_cast(
        articulacao_cultura_consorcio_publico_uniao as string
    ) articulacao_cultura_consorcio_publico_uniao,
    safe_cast(
        articulacao_cultura_consorcio_administrativo_intermunicipal as string
    ) articulacao_cultura_consorcio_administrativo_intermunicipal,
    safe_cast(
        articulacao_cultura_consorcio_administrativo_estado as string
    ) articulacao_cultura_consorcio_administrativo_estado,
    safe_cast(
        articulacao_cultura_consorcio_administrativo_uniao as string
    ) articulacao_cultura_consorcio_administrativo_uniao,
    safe_cast(
        articulacao_cultura_convenio_setor_privado as string
    ) articulacao_cultura_convenio_setor_privado,
    safe_cast(
        articulacao_cultura_apoio_setor_privado_comunidades as string
    ) articulacao_cultura_apoio_setor_privado_comunidades,
    safe_cast(
        articulacao_habitacao_consorcio_publico_intermunicipal as string
    ) articulacao_habitacao_consorcio_publico_intermunicipal,
    safe_cast(
        articulacao_habitacao_consorcio_publico_estado as string
    ) articulacao_habitacao_consorcio_publico_estado,
    safe_cast(
        articulacao_habitacao_consorcio_publico_uniao as string
    ) articulacao_habitacao_consorcio_publico_uniao,
    safe_cast(
        articulacao_habitacao_consorcio_administrativo_intermunicipal as string
    ) articulacao_habitacao_consorcio_administrativo_intermunicipal,
    safe_cast(
        articulacao_habitacao_consorcio_administrativo_estado as string
    ) articulacao_habitacao_consorcio_administrativo_estado,
    safe_cast(
        articulacao_habitacao_consorcio_administrativo_uniao as string
    ) articulacao_habitacao_consorcio_administrativo_uniao,
    safe_cast(
        articulacao_habitacao_convenio_setor_privado as string
    ) articulacao_habitacao_convenio_setor_privado,
    safe_cast(
        articulacao_habitacao_apoio_setor_privado_comunidades as string
    ) articulacao_habitacao_apoio_setor_privado_comunidades,
    safe_cast(
        articulacao_meio_ambiente_consorcio_publico_intermunicipal as string
    ) articulacao_meio_ambiente_consorcio_publico_intermunicipal,
    safe_cast(
        articulacao_meio_ambiente_consorcio_publico_estado as string
    ) articulacao_meio_ambiente_consorcio_publico_estado,
    safe_cast(
        articulacao_meio_ambiente_consorcio_publico_uniao as string
    ) articulacao_meio_ambiente_consorcio_publico_uniao,
    safe_cast(
        articulacao_meio_ambiente_consorcio_administrativo_intermunicipal as string
    ) articulacao_meio_ambiente_consorcio_administrativo_intermunicipal,
    safe_cast(
        articulacao_meio_ambiente_consorcio_administrativo_estado as string
    ) articulacao_meio_ambiente_consorcio_administrativo_estado,
    safe_cast(
        articulacao_meio_ambiente_consorcio_administrativo_uniao as string
    ) articulacao_meio_ambiente_consorcio_administrativo_uniao,
    safe_cast(
        articulacao_meio_ambiente_convenio_setor_privado as string
    ) articulacao_meio_ambiente_convenio_setor_privado,
    safe_cast(
        articulacao_meio_ambiente_apoio_setor_privado_comunidades as string
    ) articulacao_meio_ambiente_apoio_setor_privado_comunidades,
    safe_cast(
        articulacao_transporte_consorcio_publico_intermunicipal as string
    ) articulacao_transporte_consorcio_publico_intermunicipal,
    safe_cast(
        articulacao_transporte_consorcio_publico_estado as string
    ) articulacao_transporte_consorcio_publico_estado,
    safe_cast(
        articulacao_transporte_consorcio_publico_uniao as string
    ) articulacao_transporte_consorcio_publico_uniao,
    safe_cast(
        articulacao_transporte_consorcio_administrativo_intermunicipal as string
    ) articulacao_transporte_consorcio_administrativo_intermunicipal,
    safe_cast(
        articulacao_transporte_consorcio_administrativo_estado as string
    ) articulacao_transporte_consorcio_administrativo_estado,
    safe_cast(
        articulacao_transporte_consorcio_administrativo_uniao as string
    ) articulacao_transporte_consorcio_administrativo_uniao,
    safe_cast(
        articulacao_transporte_convenio_setor_privado as string
    ) articulacao_transporte_convenio_setor_privado,
    safe_cast(
        articulacao_transporte_apoio_setor_privado_comunidades as string
    ) articulacao_transporte_apoio_setor_privado_comunidades,
    safe_cast(
        articulacao_saneamento_consorcio_publico_intermunicipal as string
    ) articulacao_saneamento_consorcio_publico_intermunicipal,
    safe_cast(
        articulacao_saneamento_consorcio_publico_estado as string
    ) articulacao_saneamento_consorcio_publico_estado,
    safe_cast(
        articulacao_saneamento_consorcio_publico_uniao as string
    ) articulacao_saneamento_consorcio_publico_uniao,
    safe_cast(
        articulacao_saneamento_consorcio_administrativo_intermunicipal as string
    ) articulacao_saneamento_consorcio_administrativo_intermunicipal,
    safe_cast(
        articulacao_saneamento_consorcio_administrativo_estado as string
    ) articulacao_saneamento_consorcio_administrativo_estado,
    safe_cast(
        articulacao_saneamento_consorcio_administrativo_uniao as string
    ) articulacao_saneamento_consorcio_administrativo_uniao,
    safe_cast(
        articulacao_saneamento_convenio_setor_privado as string
    ) articulacao_saneamento_convenio_setor_privado,
    safe_cast(
        articulacao_saneamento_apoio_setor_privado_comunidades as string
    ) articulacao_saneamento_apoio_setor_privado_comunidades
from {{ set_datalake_project("br_ibge_munic_staging.recursos_gestao") }} as t
