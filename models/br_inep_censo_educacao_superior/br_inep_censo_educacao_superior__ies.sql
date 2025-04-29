{{
    config(
        alias="ies",
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
    safe_cast(tipo_organizacao_academica as string) tipo_organizacao_academica,
    safe_cast(tipo_categoria_administrativa as string) tipo_categoria_administrativa,
    safe_cast(nome_mantenedora as string) nome_mantenedora,
    safe_cast(id_mantenedora as string) id_mantenedora,
    safe_cast(id_ies as string) id_ies,
    safe_cast(nome as string) nome,
    safe_cast(sigla as string) sigla,
    safe_cast(endereco as string) endereco,
    safe_cast(numero as string) numero,
    safe_cast(complemento as string) complemento,
    safe_cast(bairro as string) bairro,
    safe_cast(cep as string) cep,
    safe_cast(quantidade_tecnicos as int64) quantidade_tecnicos,
    safe_cast(
        quantidade_tecnicos_ef_incompleto_feminino as int64
    ) quantidade_tecnicos_ef_incompleto_feminino,
    safe_cast(
        quantidade_tecnicos_ef_incompleto_masculino as int64
    ) quantidade_tecnicos_ef_incompleto_masculino,
    safe_cast(
        quantidade_tecnicos_ef_completo_feminino as int64
    ) quantidade_tecnicos_ef_completo_feminino,
    safe_cast(
        quantidade_tecnicos_ef_completo_masculino as int64
    ) quantidade_tecnicos_ef_completo_masculino,
    safe_cast(quantidade_tecnicos_em_feminino as int64) quantidade_tecnicos_em_feminino,
    safe_cast(
        quantidade_tecnicos_em_masculino as int64
    ) quantidade_tecnicos_em_masculino,
    safe_cast(quantidade_tecnicos_es_feminino as int64) quantidade_tecnicos_es_feminino,
    safe_cast(
        quantidade_tecnicos_es_masculino as int64
    ) quantidade_tecnicos_es_masculino,
    safe_cast(
        quantidade_tecnicos_especializacao_feminino as int64
    ) quantidade_tecnicos_especializacao_feminino,
    safe_cast(
        quantidade_tecnicos_especializacao_masculino as int64
    ) quantidade_tecnicos_especializacao_masculino,
    safe_cast(
        quantidade_tecnicos_mestrado_feminino as int64
    ) quantidade_tecnicos_mestrado_feminino,
    safe_cast(
        quantidade_tecnicos_mestrado_masculino as int64
    ) quantidade_tecnicos_mestrado_masculino,
    safe_cast(
        quantidade_tecnicos_doutorado_feminino as int64
    ) quantidade_tecnicos_doutorado_feminino,
    safe_cast(
        quantidade_tecnicos_doutorado_masculino as int64
    ) quantidade_tecnicos_doutorado_masculino,
    safe_cast(
        indicador_biblioteca_acesso_portal_capes as boolean
    ) indicador_biblioteca_acesso_portal_capes,
    safe_cast(
        indicador_biblioteca_acesso_outras_bases as boolean
    ) indicador_biblioteca_acesso_outras_bases,
    safe_cast(
        indicador_biblioteca_assina_outras_bases as boolean
    ) indicador_biblioteca_assina_outras_bases,
    safe_cast(
        indicador_biblioteca_repositorio_institucional as boolean
    ) indicador_biblioteca_repositorio_institucional,
    safe_cast(
        indicador_biblioteca_busca_integrada as boolean
    ) indicador_biblioteca_busca_integrada,
    safe_cast(indicador_biblioteca_internet as boolean) indicador_biblioteca_internet,
    safe_cast(
        indicador_biblioteca_rede_social as boolean
    ) indicador_biblioteca_rede_social,
    safe_cast(
        indicador_biblioteca_catalogo_online as boolean
    ) indicador_biblioteca_catalogo_online,
    safe_cast(
        quantidade_biblioteca_periodicos_eletronicos as int64
    ) quantidade_biblioteca_periodicos_eletronicos,
    safe_cast(
        quantidade_biblioteca_livros_eletronicos as int64
    ) quantidade_biblioteca_livros_eletronicos,
    safe_cast(quantidade_docentes as int64) quantidade_docentes,
    safe_cast(quantidade_docentes_exercicio as int64) quantidade_docentes_exercicio,
    safe_cast(
        quantidade_docentes_exercicio_feminino as int64
    ) quantidade_docentes_exercicio_feminino,
    safe_cast(
        quantidade_docentes_exercicio_masculino as int64
    ) quantidade_docentes_exercicio_masculino,
    safe_cast(
        quantidade_docentes_exercicio_sem_graduacao as int64
    ) quantidade_docentes_exercicio_sem_graduacao,
    safe_cast(
        quantidade_docentes_exercicio_graduacao as int64
    ) quantidade_docentes_exercicio_graduacao,
    safe_cast(
        quantidade_docentes_exercicio_especializacao as int64
    ) quantidade_docentes_exercicio_especializacao,
    safe_cast(
        quantidade_docentes_exercicio_mestrado as int64
    ) quantidade_docentes_exercicio_mestrado,
    safe_cast(
        quantidade_docentes_exercicio_doutorado as int64
    ) quantidade_docentes_exercicio_doutorado,
    safe_cast(
        quantidade_docentes_exercicio_integral as int64
    ) quantidade_docentes_exercicio_integral,
    safe_cast(
        quantidade_docentes_exercicio_integral_dedicacao_exclusiva as int64
    ) quantidade_docentes_exercicio_integral_dedicacao_exclusiva,
    safe_cast(
        quantidade_docentes_exercicio_integral_sem_dedicacao_exclusiva as int64
    ) quantidade_docentes_exercicio_integral_sem_dedicacao_exclusiva,
    safe_cast(
        quantidade_docentes_exercicio_parcial as int64
    ) quantidade_docentes_exercicio_parcial,
    safe_cast(
        quantidade_docentes_exercicio_horista as int64
    ) quantidade_docentes_exercicio_horista,
    safe_cast(
        quantidade_docentes_exercicio_0_29 as int64
    ) quantidade_docentes_exercicio_0_29,
    safe_cast(
        quantidade_docentes_exercicio_30_34 as int64
    ) quantidade_docentes_exercicio_30_34,
    safe_cast(
        quantidade_docentes_exercicio_35_39 as int64
    ) quantidade_docentes_exercicio_35_39,
    safe_cast(
        quantidade_docentes_exercicio_40_44 as int64
    ) quantidade_docentes_exercicio_40_44,
    safe_cast(
        quantidade_docentes_exercicio_45_49 as int64
    ) quantidade_docentes_exercicio_45_49,
    safe_cast(
        quantidade_docentes_exercicio_50_54 as int64
    ) quantidade_docentes_exercicio_50_54,
    safe_cast(
        quantidade_docentes_exercicio_55_59 as int64
    ) quantidade_docentes_exercicio_55_59,
    safe_cast(
        quantidade_docentes_exercicio_60_mais as int64
    ) quantidade_docentes_exercicio_60_mais,
    safe_cast(
        quantidade_docentes_exercicio_branca as int64
    ) quantidade_docentes_exercicio_branca,
    safe_cast(
        quantidade_docentes_exercicio_preta as int64
    ) quantidade_docentes_exercicio_preta,
    safe_cast(
        quantidade_docentes_exercicio_parda as int64
    ) quantidade_docentes_exercicio_parda,
    safe_cast(
        quantidade_docentes_exercicio_amarela as int64
    ) quantidade_docentes_exercicio_amarela,
    safe_cast(
        quantidade_docentes_exercicio_indigena as int64
    ) quantidade_docentes_exercicio_indigena,
    safe_cast(
        quantidade_docentes_exercicio_cor_nao_declarada as int64
    ) quantidade_docentes_exercicio_cor_nao_declarada,
    safe_cast(
        quantidade_docentes_exercicio_brasileiro as int64
    ) quantidade_docentes_exercicio_brasileiro,
    safe_cast(
        quantidade_docentes_exercicio_estrangeiro as int64
    ) quantidade_docentes_exercicio_estrangeiro,
    safe_cast(
        quantidade_docentes_exercicio_deficiencia as int64
    ) quantidade_docentes_exercicio_deficiencia,
from {{ set_datalake_project("br_inep_censo_educacao_superior_staging.ies") }} as t
