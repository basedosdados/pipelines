{{
    config(
        alias="historico_inscritos",
        schema="world_oceanos_mapeamento",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(id_inscrito as string) id_inscrito,
    safe_cast(responsavel_inscricao as string) responsavel,
    safe_cast(nome_responsavel_inscricao as string) nome_responsavel,
    safe_cast(titulo_livro as string) titulo_livro,
    safe_cast(isbn as string) isbn,
    safe_cast(genero_livro_categorias as string) genero_livro,
    safe_cast(nome_pais_primeira_edicao as string) nome_pais_primeira_edicao,
    safe_cast(tipo_publicacao as string) tipo_publicacao,
    safe_cast(nome_autor_final as string) autor_nome,
    safe_cast(genero_autor as string) autor_genero,
    safe_cast(idade_autor as string) autor_idade,
    safe_cast(nome_pais_autor as string) autor_nome_pais,
    safe_cast(nacionaldade_autor as string) autor_nacionalidade,
    safe_cast(
        indicador_atividade_economica_principal_autor as string
    ) autor_indicador_atividade_economica_principal,
    safe_cast(educacao_formal_autor as string) autor_educacao_formal,
    safe_cast(
        indicador_publicacao_outras_obras as boolean
    ) autor_indicador_publicacao_outras_obras,
    safe_cast(quantidade_obras_publicadas as int64) autor_quantidade_obras_publicadas,
    safe_cast(nome_editora_final_3 as string) editora_nome,
    safe_cast(pais_origem_editora as string) editora_pais_origem,
    safe_cast(local_sede_editora_normalizado as string) editora_local_sede,
    safe_cast(ano_criacao_editora as int64) editora_ano_criacao,
    safe_cast(linha_predominante_editora as string) editora_linha_predominante,
    safe_cast(canal_distribuicao_editora as string) editora_canal_distribuicao,
    safe_cast(tiragem_edicao_editora as string) editora_tiragem_edicao,
    safe_cast(financiamento_edicao_editora as string) editora_financiamento_edicao,
    safe_cast(grupo_financiamento as string) editora_grupo_financiamento,
    safe_cast(site_editora as string) editora_site,
    safe_cast(indicador_outras_edicoes as string) indicador_outras_edicoes,
    safe_cast(nome_editora_outras_edicoes as string) outras_edicoes_nome_editora,
    safe_cast(ano_publicacao_outras_edicoes as float64) outras_edicoes_ano_publicacao,
    safe_cast(nome_pais_outras_edicoes as string) outras_edicoes_nome_pais,
    safe_cast(indicador_semifinalista_2 as string) indicador_semifinalista,
    safe_cast(indicador_finalista_2 as string) indicador_finalista,
    safe_cast(indicador_vencedor_2 as string) indicador_vencedor,
from
    {{ set_datalake_project("world_oceanos_mapeamento_staging.historico_inscritos") }}
    as t
