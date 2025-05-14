{{ config(alias="votacao", schema="br_camara_dados_abertos") }}


select
    safe_cast(replace(id, "-", "") as string) id_votacao,
    safe_cast(data as date) data,
    safe_cast(
        split(
            format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(datahoraregistro)), 'T'
        )[offset(0)] as date
    ) data_registro,
    safe_cast(
        split(
            format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(datahoraregistro)), 'T'
        )[offset(1)] as time
    ) horario_registro,
    safe_cast(idorgao as string) id_orgao,
    safe_cast(siglaorgao as string) sigla_orgao,
    safe_cast(idevento as string) id_evento,
    safe_cast(replace(aprovacao, ".0", "") as int64) aprovacao,
    safe_cast(votossim as int64) voto_sim,
    safe_cast(votosnao as int64) voto_nao,
    safe_cast(votosoutros as int64) voto_outro,
    safe_cast(descricao as string) descricao,
    safe_cast(
        ultimaaberturavotacao_datahoraregistro as datetime
    ) data_hora_ultima_votacao,
    safe_cast(ultimaaberturavotacao_descricao as string) descricao_ultima_votacao,
    safe_cast(
        ultimaapresentacaoproposicao_datahoraregistro as datetime
    ) data_hora_ultima_proposicao,
    safe_cast(ultimaapresentacaoproposicao_idproposicao as string) id_ultima_proposicao,
    case
        when ultimaapresentacaoproposicao_descricao = 'nan'
        then null
        else initcap(ultimaapresentacaoproposicao_descricao)
    end as descricao_ultima_proposicao
from {{ set_datalake_project("br_camara_dados_abertos_staging.votacao") }} as t
