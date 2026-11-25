-- materialized dictionary table for br_ms_cnes.
-- O staging é um CSV mantido à mão: códigos novos do DATASUS entram por
-- models/br_ms_cnes/code/update_dicionario.py (--apply). Última inclusão:
-- tipo_equipe 83 (EMCP-Ped, Portaria SAES/MS nº 4.288/2026).
{{ config(alias="dicionario", schema="br_ms_cnes") }}

select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor
from {{ set_datalake_project("br_ms_cnes_staging.dicionario") }} as t
