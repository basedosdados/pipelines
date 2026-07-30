{{
    config(
        schema="test_dataset",
        alias="tabela_grande",
        materialized="incremental",
        unique_key="id",
    )
}}

-- 4900 × 4900 = 24 010 000 linhas × 48 bytes ≈ 1,1 GB — condição > 1 GB (sem export)
-- IDs gerados via cross join para evitar generate_array de milhões de elementos
with
    a as (select n from unnest(generate_array(1, 4900)) as n),
    b as (select n from unnest(generate_array(1, 4900)) as n)

select
    (a.n - 1) * 4900 + b.n as id,
    farm_fingerprint(cast((a.n - 1) * 4900 + b.n as string)) as col1,
    farm_fingerprint(cast(((a.n - 1) * 4900 + b.n) * 31 as string)) as col2,
    farm_fingerprint(cast(((a.n - 1) * 4900 + b.n) * 97 as string)) as col3,
    farm_fingerprint(cast(((a.n - 1) * 4900 + b.n) * 257 as string)) as col4,
    farm_fingerprint(cast(((a.n - 1) * 4900 + b.n) * 1031 as string)) as col5
from a
cross join b

{% if is_incremental() %}
    where (a.n - 1) * 4900 + b.n > (select coalesce(max(id), 0) from {{ this }})
{% endif %}
