{{ config(alias="frente", schema="br_camara_dados_abertos") }}
select
    safe_cast(id as string) id_frente,
    safe_cast(titulo as string) titulo,
    safe_cast(datacriacao as date) data_criacao,
    safe_cast(idlegislatura as string) id_legislatura,
    safe_cast(telefone as string) telefone,
    safe_cast(situacao as string) situacao,
    safe_cast(urldocumento as string) url_documento,
    safe_cast(coordenador_id as string) id_coordenador,
    safe_cast(coordenador_nome as string) nome_coordenador,
    safe_cast(coordenador_urlfoto as string) url_foto_coordenador,
from {{ set_datalake_project("br_camara_dados_abertos_staging.frente") }} as t
