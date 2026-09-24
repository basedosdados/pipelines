-- Minas Gerais (MG) contribution to world_wb_mides.pagamento.
-- Split out of the monolithic pagamento model. The SQL is unchanged: the CTE
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
    -- `seq_empenho` remains the join handle WITHIN one extraction: it is
    -- globally unique across MG municipalities (0 clashes in 2021), which is
    -- why this model resolves through it instead of rebuilding the key locally.
    -- It could not rebuild it -- liquidacao does not carry the empenho's number
    -- or unit codes at all.
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
    pagamento_mg as (
        select distinct
            safe_cast(p.ano as int64) as ano,
            safe_cast(p.mes as int64) as mes,
            safe_cast(p.data as date) as data,
            safe_cast(p.sigla_uf as string) as sigla_uf,
            safe_cast(p.id_municipio as string) as id_municipio,
            safe_cast(p.orgao as string) as orgao,
            safe_cast(p.id_unidade_gestora as string) as id_unidade_gestora,
            -- Resolved through the empenho itself, so the key carries the
            -- EMPENHO's organ/unit/exercise rather than the pagamento's or the
            -- rsp's restatement of them. Note pagamento DOES denormalise
            -- `numero_empenho`/`dat_empenho`, but only alongside
            -- `seq_orgao_empenho`/`seq_unid_empenho` -- sequence forms of the
            -- organ and unit, not the `cod_*` codes the key needs -- so it
            -- resolves through the lookup like liquidacao does.
            safe_cast(
                case
                    when id_empenho != '-1'
                    then ek.id_empenho_bd
                    when id_empenho = '-1'
                    then eko.id_empenho_bd
                end as string
            ) as id_empenho_bd,
            safe_cast(
                case
                    when p.id_empenho = '-1'
                    then replace (p.id_empenho, '-1', id_empenho_origem)
                end as string
            ) as id_empenho,
            safe_cast(p.numero_empenho as string) as numero_empenho,
            -- Must be BYTE-IDENTICAL to `id_liquidacao_bd` in the liquidacao
            -- model, or pagamento stops linking to its liquidacao. Rebuilt from
            -- pagamento's OWN columns rather than by joining that 110M-row
            -- table, which is sound because pagamento restates the liquidacao
            -- exactly: measured over 465,182 referencing rows, numero_liquidacao,
            -- dat_liquidacao and orgao agree with the liquidacao row in 100% of
            -- cases, the two agree on the settled empenho in 500,783 of 500,783,
            -- and every referenced liquidacao sits in the same exercise.
            --
            -- The old `when p.id_liquidacao = '-1'` arm built
            -- concat(' ', r.orgao, ...) -- a key with a leading space and NO
            -- liquidacao component, so every such row in a municipality-year
            -- collided. A payment with no liquidacao has no liquidacao identity;
            -- it is NULL now.
            safe_cast(
                case
                    when p.id_liquidacao != '-1'
                    then
                        concat(
                            p.numero_liquidacao,
                            ' ',
                            trim(p.orgao),
                            ' ',
                            p.dat_liquidacao,
                            ' ',
                            -- Identical in shape to the liquidacao model's arm, on
                            -- purpose: a differently-shaped fallback could diverge on
                            -- an unresolvable empenho and break the link.
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
                            p.id_municipio,
                            ' ',
                            (right(p.ano, 2))
                        )
                end as string
            ) as id_liquidacao_bd,
            safe_cast(
                case
                    when p.id_empenho = '-1' then replace (p.id_liquidacao, '-1', '')
                end as string
            ) as id_liquidacao,
            safe_cast(p.numero_liquidacao as string) as numero_liquidacao,
            -- STABLE pagamento key: exact grain parity with the old
            -- `id_pagamento` (seq) key in all 13 MG exercises, verified with the
            -- restos-a-pagar arm resolved to the ORIGIN EMPENHO exactly as the
            -- join below does.
            --
            -- Every component is needed. Dropping `fonte` costs parity in 4 of
            -- the 13 exercises (13 rows): one payment can be split across
            -- funding sources, same number, date, empenho and liquidacao.
            -- Dropping the empenho component costs far more -- two different
            -- empenhos can share numero_empenho + dat_empenho and be told apart
            -- only by their unit, which pagamento does not carry but the
            -- resolved empenho key does.
            --
            -- Only the fonte CODE goes in, never the label. `fonte` arrives as
            -- '<code> - <description>' in two schemes at once (legacy '100',
            -- current '2.621.000'), and the description half is reworded and
            -- mis-encoded between extractions -- exactly the instability this
            -- key exists to avoid.
            safe_cast(
                concat(
                    p.numero_pagamento,
                    ' ',
                    trim(p.orgao),
                    ' ',
                    p.data,
                    ' ',
                    coalesce(
                        ek.id_empenho_bd,
                        eko.id_empenho_bd,
                        case
                            when p.id_rsp != '-1'
                            then
                                concat(
                                    'rsp:',
                                    ifnull(r.numero_empenho, ''),
                                    ' ',
                                    ifnull(r.num_ano_emp_origem, '')
                                )
                            else
                                concat(
                                    'den:',
                                    ifnull(p.numero_empenho, ''),
                                    ' ',
                                    ifnull(p.dat_empenho, '')
                                )
                        end
                    ),
                    ' ',
                    -- NULL in 2.2% of rows (no liquidacao). concat() returns
                    -- NULL on a NULL argument, so these must be defaulted or
                    -- the whole key vanishes for those rows.
                    ifnull(p.numero_liquidacao, ''),
                    ' ',
                    ifnull(p.dat_liquidacao, ''),
                    ' ',
                    coalesce(regexp_extract(p.fonte, r'^\s*([0-9.]+)'), p.fonte, ''),
                    ' ',
                    p.id_municipio,
                    ' ',
                    (right(p.ano, 2))
                ) as string
            ) as id_pagamento_bd,
            safe_cast(id_pagamento as string) as id_pagamento,
            safe_cast(p.numero_pagamento as string) as numero,
            safe_cast(nome_credor as string) as nome_credor,
            safe_cast(
                replace(replace (documento_credor, '.', ''), '-', '') as string
            ) as documento_credor,
            safe_cast(
                case when p.id_rsp != '-1' then 1 else 0 end as bool
            ) as indicador_restos_pagar,
            safe_cast(left(fonte, 3) as string) as fonte,
            round(safe_cast(valor_pagamento_original as float64), 2) as valor_inicial,
            round(ifnull(safe_cast(vlr_anu_fonte as float64), 0), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(
                safe_cast(valor_pagamento_original as float64)
                - ifnull(safe_cast(vlr_anu_fonte as float64), 0),
                2
            ) as valor_final,
            round(
                safe_cast(valor_pagamento_original as float64)
                - ifnull(safe_cast(vlr_anu_fonte as float64), 0)
                - ifnull(safe_cast(vlr_ret_fonte as float64), 0),
                2
            ) as valor_liquido_recebido,
        from {{ set_datalake_project("world_wb_mides_staging.raw_pagamento_mg") }} as p
        left join
            {{ set_datalake_project("world_wb_mides_staging.raw_rsp_mg") }} as r
            on p.id_rsp = r.id_rsp
        -- Direct pagamento -> its empenho.
        left join emp_key_mg as ek on p.id_empenho = ek.seq_empenho_lookup
        -- Restos-a-pagar pagamento -> the ORIGIN empenho, in an earlier
        -- exercise. Unresolved when that exercise predates MG staging coverage
        -- (2014-); those references dangle under the old key too.
        left join emp_key_mg as eko on r.id_empenho_origem = eko.seq_empenho_lookup

    )
select *
from pagamento_mg
