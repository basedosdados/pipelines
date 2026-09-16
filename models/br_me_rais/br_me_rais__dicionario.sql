-- register 12/02/2026
{{ config(alias="dicionario", schema="br_me_rais") }}
select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor,
from {{ set_datalake_project("br_me_rais_staging.dicionario") }} as t
union all
select 'microdados_vinculos', 'indicador_vinculo_abandonado', '0', '2023(1)', 'Não'
union all
select 'microdados_vinculos', 'indicador_vinculo_abandonado', '1', '2023(1)', 'Sim'
-- Códigos de motivo_desligamento que a fonte passou a emitir em 2025 e que o
-- dicionário da RAIS ainda não documenta. Registrados como não encontrados, o
-- mesmo tratamento já dado aos códigos 1-9, 89 e 99. Ver README §6.7.
union all
select
    'microdados_vinculos',
    'motivo_desligamento',
    chave,
    '2025(1)',
    'Código não encontrado nos dicionários oficiais.'
from unnest(['24', '35', '36', '65', '81', '82']) as chave
