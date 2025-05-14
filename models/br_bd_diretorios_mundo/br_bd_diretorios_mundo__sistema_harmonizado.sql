{{ config(alias="sistema_harmonizado", schema="br_bd_diretorios_mundo") }}


select
    safe_cast(co_sh6 as string) id_sh6,
    safe_cast(co_sh4 as string) id_sh4,
    safe_cast(co_sh2 as string) id_sh2,
    safe_cast(co_ncm_secrom as string) id_ncm_secrom,
    safe_cast(no_sh6_por as string) nome_sh6_portugues,
    safe_cast(no_sh4_por as string) nome_sh4_portugues,
    safe_cast(no_sh2_por as string) nome_sh2_portugues,
    safe_cast(no_sec_por as string) nome_sec_portugues,
    safe_cast(no_sh6_esp as string) nome_sh6_espanhol,
    safe_cast(no_sh4_esp as string) nome_sh4_espanhol,
    safe_cast(no_sh2_esp as string) nome_sh2_espanhol,
    safe_cast(no_sec_esp as string) nome_sec_espanhol,
    safe_cast(no_sh6_ing as string) nome_sh6_ingles,
    safe_cast(no_sh4_ing as string) nome_sh4_ingles,
    safe_cast(no_sh2_ing as string) nome_sh2_ingles,
    safe_cast(no_sec_ing as string) nome_sec_ingles,
from
    {{ set_datalake_project("br_bd_diretorios_mundo_staging.sistema_harmonizado") }}
    as x
