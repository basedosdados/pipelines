-- Minas Gerais (MG) contribution to world_wb_mides.liquidacao.
-- Split out of the monolithic liquidacao model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    -- One row per empenho, carrying the STABLE key.
    --
    -- `id_empenho_bd` for MG is built from fields the source does not
    -- regenerate: the municipality's own empenho number plus the administrative
    -- organ and unit codes. It deliberately does NOT use `seq_empenho` /
    -- `seq_orgao`, which are global portal sequences TCE-MG reassigns between
    -- extractions -- on MG 2021 only 58% of seq-based keys survived a
    -- re-harvest, while 99.3% of the "missing" records were still present under
    -- new sequence numbers.
    --
    -- Equal grain, measured over ALL 13 MG exercises (68,345,259 staging rows):
    -- this key and the old seq-based key induce the SAME partition, year for
    -- year, with no NULL in any key field. 2021, for instance: 5,438,862 raw
    -- rows -> 5,434,944 distinct under either key, and 5,434,944 under the two
    -- combined. The collapsed rows are a single empenho fanned out over several
    -- contracts/licitacoes, differing ONLY in seq_contrato, id_licitacao,
    -- seq_termo_aditivo, seq_dispensa and seq_convenio. So the `select distinct`
    -- below returns exactly one row per `id_empenho` and the joins using it
    -- cannot fan out. If that ever stops holding, liquidacao and pagamento row
    -- counts rise -- that is the signal to re-check.
    --
    -- `data` is in the key for a reason: without it the key is exact in 11 of
    -- the 13 exercises but merges 4 genuinely distinct empenhos -- 1 in 2014
    -- (municipality 3166709) and 3 in 2016 (3161403, all with the sentinel
    -- cod_subunidade = '-9'), where a municipality reissued one empenho number
    -- inside a single organ/unit and year. Their dates differ, so `data`
    -- separates them and restores exact parity. It does not over-split the
    -- fan-out above, whose rows share a date.
    --
    -- `seq_empenho` is the join handle, and this CTE is NOT scoped by
    -- municipality. That deserves an explicit measurement rather than an
    -- appeal to "unique within one extraction", because the mirror is NOT one
    -- extraction: the municipalities TCE-MG withdrew from 2017/2018 are carried
    -- over from the published vintage (see `remap_mg_2017_2018_orgao.py`), and
    -- TCE-MG reassigns `seq_empenho` between extractions. So the question is
    -- whether a sequence can resolve to an empenho of a DIFFERENT municipality
    -- across the mixed vintages.
    --
    -- Measured on the whole mirror, every row, 2026-09-24: of 68,314,208
    -- distinct `seq_empenho` values, ZERO map to more than one `id_empenho_bd`
    -- and ZERO appear under more than one municipality. The join therefore
    -- neither fans out nor crosses municipalities on this data. (Re-measure
    -- with the query in `validate_mg.py`'s companion checks if a future harvest
    -- mixes vintages again.)
    --
    -- This model resolves through the sequence instead of rebuilding the key
    -- locally because it could not rebuild it: liquidacao does not carry the
    -- empenho's number or unit codes at all.
    emp_key_mg as (
        select distinct
            -- Deliberately NOT called `id_empenho`: this CTE is joined alongside
            -- the liquidacao/pagamento and rsp mirrors, which have a column of
            -- that name, and an unqualified reference then fails to resolve
            -- ("Column name id_empenho is ambiguous"). dbt compile does not catch
            -- it; only a real run does.
            id_empenho as seq_empenho_lookup,
            concat(
                numero_empenho,
                ' ',
                trim(orgao),
                ' ',
                cod_unidade,
                ' ',
                cod_subunidade,
                ' ',
                data,
                ' ',
                id_municipio,
                ' ',
                (right(ano, 2))
            ) as id_empenho_bd
        from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_mg") }}

    ),
    liquidacao_mg as (
        select
            safe_cast(ano as int64) as ano,
            safe_cast(mes as int64) as mes,
            safe_cast(data as date) as data,
            'MG' as sigla_uf,
            safe_cast(l.id_municipio as string) as id_municipio,
            safe_cast(l.orgao as string) as orgao,
            safe_cast(l.id_unidade_gestora as string) as id_unidade_gestora,
            -- Resolved through the empenho itself, so the key carries the
            -- EMPENHO's organ/unit/exercise rather than the liquidacao's or the
            -- rsp's restatement of them.
            safe_cast(
                (
                    case
                        when id_empenho != '-1'
                        then ek.id_empenho_bd
                        when id_empenho = '-1'
                        then eko.id_empenho_bd
                    end
                ) as string
            ) as id_empenho_bd,
            safe_cast(
                (
                    case
                        when id_empenho = '-1'
                        then replace (id_empenho, '-1', id_empenho_origem)
                    end
                ) as string
            ) as id_empenho,
            safe_cast(numero_empenho as string) as numero_empenho,
            -- STABLE liquidacao key: exact grain parity with the old
            -- `id_liquidacao` (seq) key in all 13 MG exercises, 0 NULL and 0
            -- empty in every component.
            --
            -- `numero_liquidacao` is nowhere near unique on its own: one
            -- liquidacao document settles several empenhos and each is a
            -- separate row, so the empenho it settles is PART OF the key.
            -- Without that component the best candidate collapses 17.7% of
            -- rows (2021: 8,803,141 -> 7,245,614).
            safe_cast(
                concat(
                    l.numero_liquidacao,
                    ' ',
                    trim(l.orgao),
                    ' ',
                    l.data,
                    ' ',
                    -- The empenho settled. The third arm fires only when a
                    -- restos-a-pagar origin predates MG staging coverage
                    -- (2014-) -- 8 rows in 591,648 measured. It exists so
                    -- `id_liquidacao_bd` stays non-NULL there: a liquidacao's
                    -- own identity must not depend on its empenho resolving.
                    coalesce(
                        ek.id_empenho_bd,
                        eko.id_empenho_bd,
                        concat(
                            'rsp:',
                            ifnull(r.numero_empenho, ''),
                            ' ',
                            ifnull(r.num_ano_emp_origem, '')
                        )
                    ),
                    ' ',
                    l.id_municipio,
                    ' ',
                    (right(ano, 2))
                ) as string
            ) as id_liquidacao_bd,
            safe_cast(id_liquidacao as string) as id_liquidacao,
            safe_cast(numero_liquidacao as string) as numero,
            safe_cast(nome_responsavel as string) as nome_responsavel,
            safe_cast(documento_responsavel as string) as documento_responsavel,
            safe_cast(
                (case when l.id_rsp != '-1' then 1 else 0 end) as bool
            ) as indicador_restos_pagar,
            round(safe_cast(valor_liquidacao_original as float64), 2) as valor_inicial,
            round(safe_cast(valor_anulado as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(
                safe_cast(valor_liquidacao_original as float64)
                - ifnull(safe_cast(valor_anulado as float64), 0),
                2
            ) as valor_final
        from {{ set_datalake_project("world_wb_mides_staging.raw_liquidacao_mg") }} as l
        left join
            {{ set_datalake_project("world_wb_mides_staging.raw_rsp_mg") }} as r
            on l.id_rsp = r.id_rsp
        -- Direct liquidacao -> its empenho.
        left join emp_key_mg as ek on l.id_empenho = ek.seq_empenho_lookup
        -- Restos-a-pagar liquidacao -> the ORIGIN empenho, in an earlier
        -- exercise. Unresolved when that exercise predates the MG staging
        -- coverage (2014-): those references dangle under the old key too,
        -- because the empenho they name is not in the dataset at all.
        left join emp_key_mg as eko on r.id_empenho_origem = eko.seq_empenho_lookup

    )
select *
from liquidacao_mg
