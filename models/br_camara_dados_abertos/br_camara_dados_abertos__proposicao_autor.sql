{{ config(alias="proposicao_autor", schema="br_camara_dados_abertos") }}

select distinct
    safe_cast(idproposicao as string) id_proposicao,
    replace(safe_cast(iddeputadoautor as string), ".0", "") id_deputado,
    initcap(safe_cast(tipoautor as string)) tipo_autor,
    initcap(safe_cast(nomeautor as string)) nome_autor,
    safe_cast(siglapartidoautor as string) sigla_partido,
    upper(safe_cast(siglaufautor as string)) sigla_uf_autor,
    safe_cast(ordemassinatura as string) ordem_assinatura,
    safe_cast(proponente as string) proponente,
from {{ set_datalake_project("br_camara_dados_abertos_staging.proposicao_autor") }} as t
