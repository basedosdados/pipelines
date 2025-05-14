{{
    config(
        alias="aluno_ef_5ano",
        schema="br_inep_saeb",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1995, "end": 2023, "interval": 1},
        },
        cluster_by=["sigla_uf"],
        labels={"tema": "educacao"},
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_regiao as string) id_regiao,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(area as string) area,
    safe_cast(mascara as string) mascara,
    safe_cast(ano_mascara as int64) ano_mascara,
    safe_cast(id_escola as string) id_escola,
    safe_cast(rede as string) rede,
    safe_cast(escola_publica as string) escola_publica,
    safe_cast(localizacao as string) localizacao,
    safe_cast(id_turma as string) id_turma,
    safe_cast(turno as string) turno,
    safe_cast(serie as int64) serie,
    safe_cast(id_aluno as string) id_aluno,
    safe_cast(situacao_censo as string) situacao_censo,
    safe_cast(disciplina as string) disciplina,
    safe_cast(preenchimento_caderno as int64) preenchimento_caderno,
    safe_cast(presenca as int64) presenca,
    safe_cast(caderno as string) caderno,
    safe_cast(bloco_1 as string) bloco_1,
    safe_cast(bloco_2 as string) bloco_2,
    safe_cast(bloco_3 as string) bloco_3,
    safe_cast(respostas_bloco_1 as string) respostas_bloco_1,
    safe_cast(respostas_bloco_2 as string) respostas_bloco_2,
    safe_cast(respostas_bloco_3 as string) respostas_bloco_3,
    safe_cast(gabarito_bloco_1 as string) gabarito_bloco_1,
    safe_cast(gabarito_bloco_2 as string) gabarito_bloco_2,
    safe_cast(gabarito_bloco_3 as string) gabarito_bloco_3,
    safe_cast(indicador_proficiencia as int64) indicador_proficiencia,
    safe_cast(indicador_prova_brasil as int64) indicador_prova_brasil,
    safe_cast(amostra as string) amostra,
    safe_cast(estrato as string) estrato,
    safe_cast(peso_escola as float64) peso_escola,
    safe_cast(peso_turma as float64) peso_turma,
    safe_cast(peso_aluno_turma as float64) peso_aluno_turma,
    safe_cast(peso_aluno as float64) peso_aluno,
    safe_cast(proficiencia as float64) proficiencia,
    safe_cast(erro_padrao as float64) erro_padrao,
    safe_cast(proficiencia_saeb as float64) proficiencia_saeb,
    safe_cast(erro_padrao_saeb as float64) erro_padrao_saeb,
    safe_cast(desempenho_aluno as float64) desempenho_aluno,
    safe_cast(preenchimento_questionario as int64) preenchimento_questionario,
    safe_cast(sexo as string) sexo,
    safe_cast(raca_cor as string) raca_cor,
    safe_cast(mes_nascimento as int64) mes_nascimento,
    safe_cast(ano_nascimento as int64) ano_nascimento,
    safe_cast(idade as string) idade,
    safe_cast(faixa_etaria as string) faixa_etaria,
    safe_cast(aniversariante as string) aniversariante,
    safe_cast(possui_necessidade_especial as string) possui_necessidade_especial,
    safe_cast(com_quem_mora as string) com_quem_mora,
    safe_cast(mora_mae as string) mora_mae,
    safe_cast(mora_pai as string) mora_pai,
    safe_cast(mora_irmaos as string) mora_irmaos,
    safe_cast(mora_avos as string) mora_avos,
    safe_cast(mora_outros_parentes as string) mora_outros_parentes,
    safe_cast(mora_amigos as string) mora_amigos,
    safe_cast(idioma_domicilio as string) idioma_domicilio,
    safe_cast(quantidade_pessoas_domicilio as string) quantidade_pessoas_domicilio,
    safe_cast(
        quantidade_pessoas_despesas_domicilio as string
    ) quantidade_pessoas_despesas_domicilio,
    safe_cast(indicador_inse as int64) indicador_inse,
    safe_cast(inse as float64) inse,
    safe_cast(nivel_inse as string) nivel_inse,
    safe_cast(peso_inse as float64) peso_inse,
    safe_cast(possui_trabalho as string) possui_trabalho,
    safe_cast(turma_bolsa_escola as string) turma_bolsa_escola,
    safe_cast(aluno_bolsa_escola as string) aluno_bolsa_escola,
    safe_cast(possui_moradia_rua_urbanizada as string) possui_moradia_rua_urbanizada,
    safe_cast(possui_agua_encanada as string) possui_agua_encanada,
    safe_cast(possui_eletrecidade as string) possui_eletrecidade,
    safe_cast(
        possui_eletrodomestico_sem_eletricidade as string
    ) possui_eletrodomestico_sem_eletricidade,
    safe_cast(possui_casa_dormitorio as string) possui_casa_dormitorio,
    safe_cast(possui_casa_quarto_individual as string) possui_casa_quarto_individual,
    safe_cast(possui_casa_cozinha as string) possui_casa_cozinha,
    safe_cast(possui_casa_banheiro as string) possui_casa_banheiro,
    safe_cast(possui_casa_sala as string) possui_casa_sala,
    safe_cast(possui_casa_garagem as string) possui_casa_garagem,
    safe_cast(possui_automovel as string) possui_automovel,
    safe_cast(possui_geladeira as string) possui_geladeira,
    safe_cast(possui_geladeira_freezer as string) possui_geladeira_freezer,
    safe_cast(possui_freezer as string) possui_freezer,
    safe_cast(possui_microondas as string) possui_microondas,
    safe_cast(possui_maquina_lavar_roupa as string) possui_maquina_lavar_roupa,
    safe_cast(possui_aspirador_po as string) possui_aspirador_po,
    safe_cast(possui_radio as string) possui_radio,
    safe_cast(possui_tv as string) possui_tv,
    safe_cast(possui_tv_assinatura as string) possui_tv_assinatura,
    safe_cast(possui_videocassete_dvd as string) possui_videocassete_dvd,
    safe_cast(possui_internet as string) possui_internet,
    safe_cast(possui_computador as string) possui_computador,
    safe_cast(possui_computador_sem_internet as string) possui_computador_sem_internet,
    safe_cast(possui_tablet as string) possui_tablet,
    safe_cast(possui_telefone as string) possui_telefone,
    safe_cast(possui_celular as string) possui_celular,
    safe_cast(possui_escrivaninha as string) possui_escrivaninha,
    safe_cast(possui_enciclopedia as string) possui_enciclopedia,
    safe_cast(possui_atlas as string) possui_atlas,
    safe_cast(possui_dicionario as string) possui_dicionario,
    safe_cast(possui_calculadora as string) possui_calculadora,
    safe_cast(possui_empregada_domestica as string) possui_empregada_domestica,
    safe_cast(
        possui_empregada_domestica_cinco_dias as string
    ) possui_empregada_domestica_cinco_dias,
    safe_cast(escolaridade_mae as string) escolaridade_mae,
    safe_cast(mae_sabe_ler_escrever as string) mae_sabe_ler_escrever,
    safe_cast(mae_le as string) mae_le,
    safe_cast(ocupacao_mae as string) ocupacao_mae,
    safe_cast(escolaridade_pai as string) escolaridade_pai,
    safe_cast(pai_sabe_ler_escrever as string) pai_sabe_ler_escrever,
    safe_cast(pai_le as string) pai_le,
    safe_cast(ocupacao_pai as string) ocupacao_pai,
    safe_cast(pessoa_acompanha_vida_escolar as string) pessoa_acompanha_vida_escolar,
    safe_cast(escolaridade_pessoa as string) escolaridade_pessoa,
    safe_cast(responsaveis_conhecem_diretor as string) responsaveis_conhecem_diretor,
    safe_cast(
        responsaveis_conhecem_professor as string
    ) responsaveis_conhecem_professor,
    safe_cast(responsaveis_conhecem_amigo as string) responsaveis_conhecem_amigo,
    safe_cast(
        responsaveis_conhecem_responsavel_amigo as string
    ) responsaveis_conhecem_responsavel_amigo,
    safe_cast(
        responsaveis_conversam_professor_diretor as string
    ) responsaveis_conversam_professor_diretor,
    safe_cast(
        responsaveis_almocam_jantam_contigo as string
    ) responsaveis_almocam_jantam_contigo,
    safe_cast(
        responsaveis_ouvem_musica_contigo as string
    ) responsaveis_ouvem_musica_contigo,
    safe_cast(
        responsaveis_conversam_livros_contigo as string
    ) responsaveis_conversam_livros_contigo,
    safe_cast(
        responsaveis_conversam_filmes_contigo as string
    ) responsaveis_conversam_filmes_contigo,
    safe_cast(
        responsaveis_conversam_programas_tv_contigo as string
    ) responsaveis_conversam_programas_tv_contigo,
    safe_cast(responsaveis_conversam_amigos as string) responsaveis_conversam_amigos,
    safe_cast(
        responsaveis_conversam_responsaveis_amigo as string
    ) responsaveis_conversam_responsaveis_amigo,
    safe_cast(
        responsaveis_conversam_outros_assuntos_contigo as string
    ) responsaveis_conversam_outros_assuntos_contigo,
    safe_cast(responsaveis_conversam_escola as string) responsaveis_conversam_escola,
    safe_cast(responsaveis_conversam_boletim as string) responsaveis_conversam_boletim,
    safe_cast(
        responsaveis_conversam_comportamento as string
    ) responsaveis_conversam_comportamento,
    safe_cast(responsaveis_leem as string) responsaveis_leem,
    safe_cast(
        responsaveis_cobram_realizacao_licao_casa as string
    ) responsaveis_cobram_realizacao_licao_casa,
    safe_cast(
        responsaveis_ajudam_realizacao_licao_casa as string
    ) responsaveis_ajudam_realizacao_licao_casa,
    safe_cast(
        responsaveis_incentivam_realizacao_licao_casa as string
    ) responsaveis_incentivam_realizacao_licao_casa,
    safe_cast(
        responsaveis_incentivam_estudos as string
    ) responsaveis_incentivam_estudos,
    safe_cast(
        responsaveis_incentivam_leitura as string
    ) responsaveis_incentivam_leitura,
    safe_cast(
        responsaveis_incentivam_comparecer_aulas as string
    ) responsaveis_incentivam_comparecer_aulas,
    safe_cast(
        responsaveis_incentivam_pontualidade as string
    ) responsaveis_incentivam_pontualidade,
    safe_cast(
        responsaveis_incentivam_boas_notas as string
    ) responsaveis_incentivam_boas_notas,
    safe_cast(
        responsaveis_comparecem_reuniao_pais as string
    ) responsaveis_comparecem_reuniao_pais,
    safe_cast(
        responsaveis_participam_festas_escola as string
    ) responsaveis_participam_festas_escola,
    safe_cast(
        responsaveis_participam_trabalho_voluntario as string
    ) responsaveis_participam_trabalho_voluntario,
    safe_cast(tempo_chegada_escola as string) tempo_chegada_escola,
    safe_cast(forma_chegada_escola as string) forma_chegada_escola,
    safe_cast(transporte_escolar as string) transporte_escolar,
    safe_cast(inicio_estudos as string) inicio_estudos,
    safe_cast(quantos_anos_primeiro_grau as string) quantos_anos_primeiro_grau,
    safe_cast(idade_entrada_escola as string) idade_entrada_escola,
    safe_cast(quantidade_mudancas_escola as string) quantidade_mudancas_escola,
    safe_cast(rede_ef as string) rede_ef,
    safe_cast(estudos_mesma_escola as string) estudos_mesma_escola,
    safe_cast(reprovacao as string) reprovacao,
    safe_cast(evasao_escolar_ate_final_ano as string) evasao_escolar_ate_final_ano,
    safe_cast(evasao_escolar_temporaria as string) evasao_escolar_temporaria,
    safe_cast(motivo_evasao_escolar as string) motivo_evasao_escolar,
    safe_cast(faltas_aula as string) faltas_aula,
    safe_cast(motivo_faltas as string) motivo_faltas,
    safe_cast(tempo_lazer as string) tempo_lazer,
    safe_cast(tipo_programacao_tv as string) tipo_programacao_tv,
    safe_cast(tempo_cursos as string) tempo_cursos,
    safe_cast(tempo_trabalho_domestico as string) tempo_trabalho_domestico,
    safe_cast(tempo_estudos as string) tempo_estudos,
    safe_cast(gosta_leitura as string) gosta_leitura,
    safe_cast(tipo_leitura as string) tipo_leitura,
    safe_cast(quantidade_livros as string) quantidade_livros,
    safe_cast(recebe_jornais as string) recebe_jornais,
    safe_cast(recebe_revistas as string) recebe_revistas,
    safe_cast(leitura_jornais as string) leitura_jornais,
    safe_cast(leitura_noticias as string) leitura_noticias,
    safe_cast(leitura_livros_geral as string) leitura_livros_geral,
    safe_cast(leitura_literatura as string) leitura_literatura,
    safe_cast(
        leitura_literatura_infantojuvenil as string
    ) leitura_literatura_infantojuvenil,
    safe_cast(leitura_historia_quadrinhos as string) leitura_historia_quadrinhos,
    safe_cast(leitura_revistas_tematicas as string) leitura_revistas_tematicas,
    safe_cast(leitura_revista_comportamento as string) leitura_revista_comportamento,
    safe_cast(leitura_revistas_geral as string) leitura_revistas_geral,
    safe_cast(leitura_internet as string) leitura_internet,
    safe_cast(leitura_outros_materiais as string) leitura_outros_materiais,
    safe_cast(frequenta_biblioteca as string) frequenta_biblioteca,
    safe_cast(frequenta_cinema as string) frequenta_cinema,
    safe_cast(frequenta_museu as string) frequenta_museu,
    safe_cast(frequenta_espetaculo_exposicao as string) frequenta_espetaculo_exposicao,
    safe_cast(frequenta_teatro as string) frequenta_teatro,
    safe_cast(frequenta_show as string) frequenta_show,
    safe_cast(frequenta_festas_comunidade as string) frequenta_festas_comunidade,
    safe_cast(participa_atividades_esporte as string) participa_atividades_esporte,
    safe_cast(
        participa_atividades_artisticas as string
    ) participa_atividades_artisticas,
    safe_cast(participa_trabalho_solidario as string) participa_trabalho_solidario,
    safe_cast(participa_reforco_escolar as string) participa_reforco_escolar,
    safe_cast(participa_excursao_acampamento as string) participa_excursao_acampamento,
    safe_cast(participa_festas as string) participa_festas,
    safe_cast(curso_atividades_artisticas as string) curso_atividades_artisticas,
    safe_cast(curso_reforco_escolar as string) curso_reforco_escolar,
    safe_cast(curso_idioma_externo as string) curso_idioma_externo,
    safe_cast(curso_informatica as string) curso_informatica,
    safe_cast(quando_nao_entende_materia as string) quando_nao_entende_materia,
    safe_cast(gosta_estudar_disciplina as string) gosta_estudar_disciplina,
    safe_cast(entendimento_ensino as string) entendimento_ensino,
    safe_cast(professor_disciplina as string) professor_disciplina,
    safe_cast(faz_licao_casa as string) faz_licao_casa,
    safe_cast(tempo_licao_casa as string) tempo_licao_casa,
    safe_cast(fez_redacao as string) fez_redacao,
    safe_cast(
        professor_leitura_livros_licao_casa as string
    ) professor_leitura_livros_licao_casa,
    safe_cast(correcao_licao_casa as string) correcao_licao_casa,
    safe_cast(correcao_licao_casa_alunos as string) correcao_licao_casa_alunos,
    safe_cast(
        consulta_jornais_revistas_licao_casa as string
    ) consulta_jornais_revistas_licao_casa,
    safe_cast(estuda_fazendo_licao_casa as string) estuda_fazendo_licao_casa,
    safe_cast(estuda_fazendo_resumos as string) estuda_fazendo_resumos,
    safe_cast(estuda_lendo_relendo_livros as string) estuda_lendo_relendo_livros,
    safe_cast(estuda_refazendo_exercicios as string) estuda_refazendo_exercicios,
    safe_cast(estuda_fazendo_anotacoes as string) estuda_fazendo_anotacoes,
    safe_cast(estuda_lendo_anotacoes as string) estuda_lendo_anotacoes,
    safe_cast(
        estuda_somente_assistindo_aulas as string
    ) estuda_somente_assistindo_aulas,
    safe_cast(estuda_fora_sala_aula as string) estuda_fora_sala_aula,
    safe_cast(utiliza_computador_licao_casa as string) utiliza_computador_licao_casa,
    safe_cast(utiliza_computador as string) utiliza_computador,
    safe_cast(utiliza_computador_escola as string) utiliza_computador_escola,
    safe_cast(utiliza_biblioteca_escola as string) utiliza_biblioteca_escola,
    safe_cast(utiliza_biblioteca_externa as string) utiliza_biblioteca_externa,
    safe_cast(existe_lugar_calmo_estudos as string) existe_lugar_calmo_estudos,
    safe_cast(comparacao_colegas_disciplina as string) comparacao_colegas_disciplina,
    safe_cast(boas_notas_disciplina as string) boas_notas_disciplina,
    safe_cast(professor_importa_voce_diz as string) professor_importa_voce_diz,
    safe_cast(professor_conversa_avaliacao as string) professor_conversa_avaliacao,
    safe_cast(professor_elogia_merecimento as string) professor_elogia_merecimento,
    safe_cast(professor_esforca_aprendizado as string) professor_esforca_aprendizado,
    safe_cast(professor_trata_carinho as string) professor_trata_carinho,
    safe_cast(turma_exclui as string) turma_exclui,
    safe_cast(alunos_desordeiros as string) alunos_desordeiros,
    safe_cast(alunos_atentos as string) alunos_atentos,
    safe_cast(alunos_obedientes as string) alunos_obedientes,
    safe_cast(possui_amigos_sala as string) possui_amigos_sala,
    safe_cast(motivo_estar_escola as string) motivo_estar_escola,
    safe_cast(
        utiliza_aprendizado_diariamente as string
    ) utiliza_aprendizado_diariamente,
    safe_cast(
        pandemia_equipamento_ensino_remoto as string
    ) pandemia_equipamento_ensino_remoto,
    safe_cast(
        pandemia_internet_ensino_remoto as string
    ) pandemia_internet_ensino_remoto,
    safe_cast(pandemia_facilidade_programas as string) pandemia_facilidade_programas,
    safe_cast(pandemia_recebeu_material as string) pandemia_recebeu_material,
    safe_cast(pandemia_auxilio_professor as string) pandemia_auxilio_professor,
    safe_cast(pandemia_compreensao_conteudo as string) pandemia_compreensao_conteudo,
    safe_cast(pandemia_ambiente_tranquilo as string) pandemia_ambiente_tranquilo,
    safe_cast(pandemia_apoio_familia as string) pandemia_apoio_familia,
    safe_cast(pandemia_apoio_colegas as string) pandemia_apoio_colegas,
    safe_cast(pretensao_futura as string) pretensao_futura,
    safe_cast(opiniao_teste as string) opiniao_teste
from {{ set_datalake_project("br_inep_saeb_staging.aluno_ef_5ano") }} as t
