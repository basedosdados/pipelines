{{ config(alias="votacao_orientacao_bancada", schema="br_camara_dados_abertos") }}
select
    safe_cast(replace(idvotacao, "-", "") as string) id_votacao,
    safe_cast(siglaorgao as string) sigla_orgao,
    safe_cast(descricao as string) descricao,
    safe_cast(siglabancada as string) sigla_bancada,
    safe_cast(orientacao as string) orientacao,
from
    {{
        set_datalake_project(
            "br_camara_dados_abertos_staging.votacao_orientacao_bancada"
        )
    }} as t
