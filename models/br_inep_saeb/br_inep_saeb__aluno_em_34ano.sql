{{ config(alias="aluno_em_34ano", schema="br_inep_saeb", materialized="table") }}
select
    safe_cast(ano as int64) ano,
    safe_cast(id_regiao as string) id_regiao,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(area as string) area,
    safe_cast(id_escola as string) id_escola,
    safe_cast(escola_publica as string) escola_publica,
    safe_cast(localizacao as string) localizacao,
    safe_cast(id_turma as string) id_turma,
    safe_cast(serie as string) serie,
    safe_cast(id_aluno as string) id_aluno,
    safe_cast(situacao_censo as string) situacao_censo,
    safe_cast(preenchimento_caderno as string) preenchimento_caderno,
    safe_cast(disciplina as string) disciplina,
    safe_cast(presenca as string) presenca,
    safe_cast(caderno as string) caderno,
    safe_cast(bloco_1 as string) bloco_1,
    safe_cast(bloco_2 as string) bloco_2,
    safe_cast(respostas_bloco_1 as string) respostas_bloco_1,
    safe_cast(respostas_bloco_2 as string) respostas_bloco_2,
    safe_cast(indicador_proficiencia as string) indicador_proficiencia,
    safe_cast(amostra as string) amostra,
    safe_cast(estrato as string) estrato,
    safe_cast(peso_aluno as float64) peso_aluno,
    safe_cast(proficiencia as string) proficiencia,
    safe_cast(erro_padrao as string) erro_padrao,
    safe_cast(erro_padrao_saeb as string) erro_padrao_saeb,
    safe_cast(proficiencia_saeb as string) proficiencia_saeb,
    safe_cast(preenchimento_questionario as string) preenchimento_questionario,
    safe_cast(indicador_inse as string) indicador_inse,
    safe_cast(inse as float64) inse,
    safe_cast(nivel_inse as string) nivel_inse,
    safe_cast(peso_inse as float64) peso_inse,
    safe_cast(sexo as string) sexo,
    safe_cast(idade as string) idade,
    safe_cast(idioma_domicilio as string) idioma_domicilio,
    safe_cast(raca_cor as string) raca_cor,
    safe_cast(possui_deficiencia as string) possui_deficiencia,
    safe_cast(possui_tea as string) possui_tea,
    safe_cast(
        possui_altas_habilidades_superdotacao as string
    ) possui_altas_habilidades_superdotacao,
    safe_cast(quantidade_pessoas_domicilio as string) quantidade_pessoas_domicilio,
    safe_cast(mora_mae as string) mora_mae,
    safe_cast(mora_pai as string) mora_pai,
    safe_cast(mora_avos as string) mora_avos,
    safe_cast(mora_avo as string) mora_avo,
    safe_cast(mora_outros_parentes as string) mora_outros_parentes,
    safe_cast(escolaridade_mae as string) escolaridade_mae,
    safe_cast(escolaridade_pai as string) escolaridade_pai,
    safe_cast(responsaveis_leem as string) responsaveis_leem,
    safe_cast(responsaveis_conversam_escola as string) responsaveis_conversam_escola,
    safe_cast(responsaveis_incentivam_estudo as string) responsaveis_incentivam_estudo,
    safe_cast(
        responsvaveis_incentivam_tarefa_casa as string
    ) responsvaveis_incentivam_tarefa_casa,
    safe_cast(
        responsaveis_incentivam_comparecer_aulas as string
    ) responsaveis_incentivam_comparecer_aulas,
    safe_cast(
        responsaveis_comparecem_reuniao_pais as string
    ) responsaveis_comparecem_reuniao_pais,
    safe_cast(possui_moradia_rua_urbanizada as string) possui_moradia_rua_urbanizada,
    safe_cast(possui_agua_encanada as string) possui_agua_encanada,
    safe_cast(possui_eletricidade as string) possui_eletricidade,
    safe_cast(possui_geladeira as string) possui_geladeira,
    safe_cast(possui_computador as string) possui_computador,
    safe_cast(possui_casa_quartos as string) possui_casa_quartos,
    safe_cast(possui_casa_televisao as string) possui_casa_televisao,
    safe_cast(possui_casa_banheiro as string) possui_casa_banheiro,
    safe_cast(possui_casa_carro as string) possui_casa_carro,
    safe_cast(posui_smartphone_com_internet as string) posui_smartphone_com_internet,
    safe_cast(possui_tv_assinatura as string) possui_tv_assinatura,
    safe_cast(possui_wifi as string) possui_wifi,
    safe_cast(possui_casa_quarto_individual as string) possui_casa_quarto_individual,
    safe_cast(possui_escrivaninha as string) possui_escrivaninha,
    safe_cast(possui_microondas as string) possui_microondas,
    safe_cast(possui_aspirador as string) possui_aspirador,
    safe_cast(possui_maquina_lavar_roupa as string) possui_maquina_lavar_roupa,
    safe_cast(possui_freezer as string) possui_freezer,
    safe_cast(possui_casa_garagem as string) possui_casa_garagem,
    safe_cast(tempo_chegada_escola as string) tempo_chegada_escola,
    safe_cast(transporte_escolar as string) transporte_escolar,
    safe_cast(possui_passe_escolar as string) possui_passe_escolar,
    safe_cast(forma_chegada_escola as string) forma_chegada_escola,
    safe_cast(inicio_estudos as string) inicio_estudos,
    safe_cast(rede_ef as string) rede_ef,
    safe_cast(reprovacao as string) reprovacao,
    safe_cast(evasao_escolar_ate_final_ano as string) evasao_escolar_ate_final_ano,
    safe_cast(tempo_estudos as string) tempo_estudos,
    safe_cast(tempo_cursos as string) tempo_cursos,
    safe_cast(tempo_trabalho_domestico as string) tempo_trabalho_domestico,
    safe_cast(tempo_trabalho as string) tempo_trabalho,
    safe_cast(tempo_lazer as string) tempo_lazer,
    safe_cast(
        professor_informa_conteudo_inicio_ano as string
    ) professor_informa_conteudo_inicio_ano,
    safe_cast(
        professor_investiga_conhecimento_previo as string
    ) professor_investiga_conhecimento_previo,
    safe_cast(
        professor_contextualiza_conteudo_cotidiano as string
    ) professor_contextualiza_conteudo_cotidiano,
    safe_cast(
        professor_aborda_desigualdade_racial as string
    ) professor_aborda_desigualdade_racial,
    safe_cast(
        professor_aborda_desigualdade_genero as string
    ) professor_aborda_desigualdade_genero,
    safe_cast(
        professor_aborda_bullying_violencia as string
    ) professor_aborda_bullying_violencia,
    safe_cast(
        professor_desenvolve_trabalho_grupo as string
    ) professor_desenvolve_trabalho_grupo,
    safe_cast(
        professor_aborda_futuro_profissional as string
    ) professor_aborda_futuro_profissional,
    safe_cast(aluno_interesse_conteudos as string) aluno_interesse_conteudos,
    safe_cast(
        aluno_motivacao_aplicar_aprendizado as string
    ) aluno_motivacao_aplicar_aprendizado,
    safe_cast(
        sala_espaco_diferentes_opinioes as string
    ) sala_espaco_diferentes_opinioes,
    safe_cast(aluno_sente_seguranca_escola as string) aluno_sente_seguranca_escola,
    safe_cast(
        aluno_conforto_discordar_professor as string
    ) aluno_conforto_discordar_professor,
    safe_cast(aluno_capacidade_argumentacao as string) aluno_capacidade_argumentacao,
    safe_cast(
        avaliacao_representa_aprendizado as string
    ) avaliacao_representa_aprendizado,
    safe_cast(
        professor_acredita_capacidade_aluno as string
    ) professor_acredita_capacidade_aluno,
    safe_cast(
        professor_motiva_continuidade_estudos as string
    ) professor_motiva_continuidade_estudos,
    safe_cast(pretensao_futura as string) pretensao_futura,
    safe_cast(concluiu_fundamental_eja as string) concluiu_fundamental_eja,
from {{ set_datalake_project("br_inep_saeb_staging.aluno_em_34ano") }} as t
