-- materialized dictionary table for br_ms_cnes.
-- O staging é um CSV mantido à mão: códigos novos do DATASUS entram por
-- models/br_ms_cnes/code/update_dicionario.py (--apply). Última inclusão:
-- rótulos da Tabela Unificada do SIGTAP (2026-10) em habilitacao e
-- servico_especializado; 38 incentivos com o nome das portarias do MS que os
-- criaram; e "Código <chave>: sem significado nas fontes oficiais (nota da
-- BD)" no tipo_unidade 16 e no subtipo_servico_especializado 170002.
{{ config(alias="dicionario", schema="br_ms_cnes") }}

select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor
from {{ set_datalake_project("br_ms_cnes_staging.dicionario") }} as t
