{{ config(alias="votacao_parlamentar", schema="br_camara_dados_abertos") }}
select
    safe_cast(replace(idvotacao, "-", "") as string) id_votacao,
    safe_cast(
        split(format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(datahoravoto)), 'T')[
            offset(0)
        ] as date
    ) data,
    safe_cast(
        split(format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(datahoravoto)), 'T')[
            offset(1)
        ] as time
    ) horario,
    safe_cast(voto as string) voto,
    safe_cast(replace(deputado_id, ".0", "") as string) id_deputado,
    safe_cast(deputado_nome as string) nome,
    safe_cast(deputado_siglapartido as string) sigla_partido,
    safe_cast(deputado_siglauf as string) sigla_uf,
    safe_cast(deputado_idlegislatura as string) id_legislatura
from
    {{ set_datalake_project("br_camara_dados_abertos_staging.votacao_parlamentar") }}
    as t
