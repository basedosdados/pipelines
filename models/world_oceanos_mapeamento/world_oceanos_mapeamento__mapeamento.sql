{{
    config(
        alias="mapeamento", schema="world_oceanos_mapeamento", materialized="table"
    )
}}

select
    safe_cast(id as string) id,
    safe_cast(livro_titulo as string) titulo,
    safe_cast(livro_genero_literario as string) genero_literario,
    safe_cast(livro_outros_generos_literarios as float64) outros_generos_literarios,
    safe_cast(livro_registro_linguistico as string) registro_linguistico,
    safe_cast(livro_tematica as string) tematica,
    safe_cast(livro_espaco_de_representacao as string) espaco_representacao,
    safe_cast(livro_ambiente_predominante as string) ambiente_predominante,
    safe_cast(livro_temporalidade as string) temporalidade,
    safe_cast(livro_foco_narrativo as string) foco_narrativo,
    safe_cast(livro_tipo_de_narrador as string) tipo_narrador,
    safe_cast(livro_procedimento_expressivo as string) procedimento_expressivo,
    safe_cast(livro_genero_dramaturgico as string) genero_dramaturgico,
    safe_cast(livro_interprete as string) interprete,
    safe_cast(livro_narrador as string) narrador,
    safe_cast(livro_formato_de_cena as string) formato_cena,
    safe_cast(livro_estetica_cenografica as string) estetica_cenografica,
    safe_cast(livro_tipo_localizacao_pred as string) tipo_localizacao,
    safe_cast(livro_localizacao_geografica as string) localizacao_geografica,
from {{ set_datalake_project("world_oceanos_mapeamento_staging.mapeamento") }} as t
