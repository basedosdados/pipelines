{{ config(materialized="ephemeral") }}

-- Minas Gerais tender <-> empenho bridge, from `fl_compras_empenho` (1,098,846 links).
--
-- Column order must match the other state models: the parent union resolves
-- positionally.
select
    'MG' as sigla_uf,
    concat('MG-', id_processo) as id_licitacao_bd,
    safe_cast(id_processo as string) as id_licitacao_origem,
    safe_cast(id_empenho as string) as id_empenho,
    cast(null as string) as numero_empenho,
    cast(null as string) as instrumento_orcamentario,
    safe_cast(dotacao_orcamentaria as string) as dotacao_orcamentaria
from
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_fl_compras_empenho") }}
    as t
