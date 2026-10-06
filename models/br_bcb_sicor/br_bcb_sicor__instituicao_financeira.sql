{{
    config(
        alias="instituicao_financeira",
        schema="br_bcb_sicor",
        materialized="table",
    )
}}


-- Nome e segmento vêm em caixa alta da fonte e são mantidos assim: `initcap`
-- estragaria siglas ("BCO DO BRASIL S.A.", "CC ARACREDI LTDA.").
select
    safe_cast(cnpj_basico as string) cnpj_basico,
    safe_cast(nome as string) nome,
    safe_cast(nullif(trim(segmento), '') as string) segmento
from {{ set_datalake_project("br_bcb_sicor_staging.instituicao_financeira") }} as t
