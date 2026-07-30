{{
    config(
        schema="test_dataset",
        alias="tabela_grande",
        materialized="incremental",
        unique_key="id",
    )
}}

-- ~24 000 000 linhas × 48 bytes ≈ 1,1 GB físico — condição > 1 GB (sem export)
-- Incremental: cria uma vez; execuções seguintes inserem 0 linhas (no-op)
select
    n as id,
    farm_fingerprint(cast(n as string)) as col1,
    farm_fingerprint(cast(n * 31 as string)) as col2,
    farm_fingerprint(cast(n * 97 as string)) as col3,
    farm_fingerprint(cast(n * 257 as string)) as col4,
    farm_fingerprint(cast(n * 1031 as string)) as col5
from unnest(generate_array(1, 24000000)) as n

{% if is_incremental() %}
    where n > (select coalesce(max(id), 0) from {{ this }})
{% endif %}
