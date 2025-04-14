{{
    config(
        alias="cadastro_enderecos",
        schema="br_ibge_censo_2022",
        partition_by={
            "field": "sigla_uf",
            "data_type": "string",
        },
        cluster_by=["id_municipio"],
    )
}}

select
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(cod_municipio as string) id_municipio,
    safe_cast(cod_distrito as string) id_distrito,
    safe_cast(cod_subdistrito as string) id_subdistrito,
    safe_cast(cod_setor as string) id_setor_censitario,
    safe_cast(cep as string) cep,
    safe_cast(cod_unico_endereco as string) id_endereco,
    safe_cast(num_quadra as string) numero_quadra,
    safe_cast(num_face as string) numero_face,
    safe_cast(dsc_localidade as string) localidade,
    safe_cast(nom_tipo_seglogr as string) tipo_segmento_logradouro,
    safe_cast(nom_titulo_seglogr as string) titulo_segmento_logradouro,
    safe_cast(nom_seglogr as string) nome_logradouro,
    safe_cast(num_endereco as string) numero_logradouro,
    safe_cast(dsc_modificador as string) modificador_numero,
    safe_cast(nom_comp_elem1 as string) complemento_elemento_1,
    safe_cast(val_comp_elem1 as string) complemento_valor_1,
    safe_cast(nom_comp_elem2 as string) complemento_elemento_2,
    safe_cast(val_comp_elem2 as string) complemento_valor_2,
    safe_cast(nom_comp_elem3 as string) complemento_elemento_3,
    safe_cast(val_comp_elem3 as string) complemento_valor_3,
    safe_cast(nom_comp_elem4 as string) complemento_elemento_4,
    safe_cast(val_comp_elem4 as string) complemento_valor_4,
    safe_cast(nom_comp_elem5 as string) complemento_elemento_5,
    safe_cast(val_comp_elem5 as string) complemento_valor_5,
    safe_cast(dsc_estabelecimento as string) descricao_estabelecimento,
    safe_cast(cod_especie as string) tipo_especie,
    safe_cast(cod_indicador_estab_endereco as string) tipo_estabelecimento,
    safe_cast(cod_indicador_const_endereco as string) tipo_construcao,
    safe_cast(cod_indicador_finalidade_const as string) tipo_finalidade_construcao,
    safe_cast(cod_tipo_especi as string) tipo_edificacao_domicilio,
    safe_cast(nv_geo_coord as string) nivel_geocodificacao_coordenadas,
    safe_cast(latitude as string) latitude,
    safe_cast(longitude as string) longitude,
    st_geogpoint(safe_cast(longitude as float64), safe_cast(latitude as float64)) ponto
from {{ set_datalake_project("br_ibge_censo_2022_staging.cadastro_enderecos") }} as t
