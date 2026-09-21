{{ config(alias="mesa", schema="br_senado_dados_abertos") }}

select
    safe_cast(id_colegiado as string) id_colegiado,
    safe_cast(sigla_colegiado as string) sigla_colegiado,
    safe_cast(nome_colegiado as string) nome_colegiado,
    safe_cast(cargo as string) cargo,
    safe_cast(id_senador as string) id_senador,
    safe_cast(nome_parlamentar as string) nome_parlamentar,
    safe_cast(bancada as string) bancada,
    safe_cast(ordem as string) ordem,
    safe_cast(origem as string) origem,
from {{ set_datalake_project("br_senado_dados_abertos_staging.mesa") }} as t
