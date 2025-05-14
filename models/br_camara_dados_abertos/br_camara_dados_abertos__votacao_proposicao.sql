{{ config(alias="votacao_proposicao", schema="br_camara_dados_abertos") }}
select
    safe_cast(replace(idvotacao, "-", "") as string) id_votacao,
    safe_cast(data as date) data,
    safe_cast(descricao as string) descricao,
    safe_cast(proposicao_id as string) id_proposicao,
    safe_cast(replace(proposicao_ano, ".0", "") as int64) ano_proposicao,
    safe_cast(proposicao_titulo as string) titulo,
    safe_cast(proposicao_ementa as string) ementa,
    safe_cast(proposicao_codtipo as string) codigo_tipo,
    safe_cast(proposicao_siglatipo as string) sigla_tipo,
    safe_cast(replace(proposicao_numero, ".0", "") as string) numero,
from
    {{ set_datalake_project("br_camara_dados_abertos_staging.votacao_proposicao") }}
    as t
