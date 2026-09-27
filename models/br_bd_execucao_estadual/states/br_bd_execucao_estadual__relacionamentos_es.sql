{{ config(materialized="ephemeral") }}

-- Espírito Santo tender <-> empenho bridge, from the `Empenhos-<ano>.csv` files in
-- `portal-da-transparencia-contratos` (181,272 rows).
--
-- Column order must match the other state models: the parent union resolves
-- positionally.
--
-- `NumeroEmpenho` is FREE TEXT, not a key, and treating it as one loses or corrupts
-- rows silently. Measured across every published file, 2026-09-07: 172,514 cells are
-- clean upper case ('2024NE00581'), 494 are the same shape in lower case, 5 are mixed,
-- and 8,258 are something else entirely.
--
-- That last group includes bare numbers with no exercise prefix ('84543590'), sequences
-- with no exercise at all ('01226', '00655', '18') which cannot be recovered without
-- guessing the year from the process, a date ('14072020'), keys with a trailing
-- exercise ('2020NE00352/2020'), and -- most dangerously -- several keys in one cell:
-- '2020NE04017, 2020NE04016, 2020NE04015, 2020NE0401'.
--
-- That last shape is the dangerous one. The column is capped at 50 characters -- the
-- longest observed cell is exactly 50 -- so a list of four or more keys is cut mid-key
-- and the tail becomes a plausible-looking but wrong id ('2020NE0401', four sequence
-- digits where every real key has five). Splitting on the comma and keeping every part
-- would emit that id as fact.
--
-- So the accepted form is strict: upper-cased, `/`-suffix removed, and matching
-- exactly `NNNNXXNNNNN`. The five-digit sequence requirement is what rejects the
-- truncated tail, without needing to reason about the cap.
--
-- Result: 171,652 of 181,272 rows (94.69%) yield at least one key; 190 cells hold a
-- list and contribute 54 extra keys beyond their first. The remaining 9,620 rows
-- (5.31%) yield none and are DROPPED rather than emitted with a null empenho -- a
-- bridge row that bridges nothing is not a relationship.
with
    fonte as (
        select *
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.es_contrato_empenho"
                )
            }}
    ),
    exploded as (
        select
            nullif(trim(f.numeroprocesso), '') as numero_processo,
            trim(f.tipodocumento) as tipo_documento,
            trim(f.naturezadespesa) as natureza_despesa,
            trim(split(parte, '/')[safe_offset(0)]) as chave
        from fonte as f, unnest(split(upper(trim(f.numeroempenho)), ',')) as parte
    )

-- DISTINCT because the source is a document ledger, not a relation: ES publishes one
-- row per contract instrument referencing the process, so 181,296 rows describe only
-- 85,543 distinct (process, empenho) pairs. A bridge row is a fact -- "this tender
-- produced this commitment" -- and repeating it once per document would both fail the
-- table's uniqueness test and fan out any join made through it.
select distinct
    'ES' as sigla_uf,
    concat('ES-', numero_processo) as id_licitacao_bd,
    safe_cast(numero_processo as string) as id_licitacao_origem,
    safe_cast(chave as string) as id_empenho,
    safe_cast(chave as string) as numero_empenho,
    safe_cast(nullif(tipo_documento, '') as string) as instrumento_orcamentario,
    safe_cast(nullif(natureza_despesa, '') as string) as dotacao_orcamentaria
from exploded
where
    numero_processo is not null
    and regexp_contains(chave, r'^[0-9]{4}[A-Z]{2}[0-9]{5}$')
