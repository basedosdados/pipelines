-- Prod-data validation of the schema diagnosis (DIAGNOSIS.md).
-- Each query probes basedosdados.br_tse_eleicoes for the *predicted symptom* of the
-- positional-mapping FAILs found by the diagnostics harness. All tables are
-- partitioned by `ano`; every probe aggregates per year so FAIL years can be
-- compared against control (OK) years of the same table.
-- Run billed to basedosdados-dev (prod datasets are public).
-- ============================================================================
-- [Q1] candidatos — FAIL years: 1994, 1996, 2014, 2018, 2020, 2022
-- Predicted symptoms if built from re-republished files with current code:
-- titulo_eleitoral <- CD_GRAU_INSTRUCAO  => 1-2 digit codes, not 10-13 digit titles
-- data_nascimento  <- CD_GENERO/CD_ESTADO_CIVIL => safe_cast(date) fails => NULL
-- genero           <- CD_ESTADO_CIVIL (<=2018) / CD_OCUPACAO (2020/22) => alien domain
-- situacao         <- NR_PARTIDO => party numbers in a label column
-- ============================================================================
select
    ano,
    count(*) as n,
    round(
        avg(if(regexp_contains(titulo_eleitoral, r'^\d{10,13}$'), 1, 0)), 3
    ) as pct_titulo_valido,
    round(avg(if(data_nascimento is null, 1, 0)), 3) as pct_data_nasc_nula,
    round(
        avg(if(regexp_contains(situacao, r'^\d+$'), 1, 0)), 3
    ) as pct_situacao_numerica,
    count(distinct genero) as n_generos,
    array_to_string(
        array_agg(distinct genero ignore nulls order by genero limit 6), ' | '
    ) as amostra_genero
from `basedosdados.br_tse_eleicoes.candidatos`
group by ano
order by ano
;

-- ============================================================================
-- [Q2] partidos — FAIL years: 1994–2012 (two alternating blocks), 2014/2018/2020
-- Predicted symptoms:
-- 1994–2012: cargo <- SG_UF (UF siglas), nome_coligacao <- NR_PARTIDO (numeric),
-- tipo_agremiacao <- NM_UE (municipality names), sigla_uf <- CD_ELEICAO
-- 2014/2018/2020: sequencial_coligacao <- NR_FEDERACAO (constant '-1'),
-- nome_coligacao <- NM_FEDERACAO (all NULL pre-federation era)
-- ============================================================================
select
    ano,
    count(*) as n,
    round(
        avg(
            if(
                cargo in (
                    'AC',
                    'AL',
                    'AM',
                    'AP',
                    'BA',
                    'CE',
                    'DF',
                    'ES',
                    'GO',
                    'MA',
                    'MG',
                    'MS',
                    'MT',
                    'PA',
                    'PB',
                    'PE',
                    'PI',
                    'PR',
                    'RJ',
                    'RN',
                    'RO',
                    'RR',
                    'RS',
                    'SC',
                    'SE',
                    'SP',
                    'TO',
                    'BR',
                    'ZZ',
                    'VT'
                ),
                1,
                0
            )
        ),
        3
    ) as pct_cargo_eh_uf,
    round(
        avg(if(regexp_contains(nome_coligacao, r'^-?\d+$'), 1, 0)), 3
    ) as pct_nome_colig_numerico,
    round(avg(if(sequencial_coligacao = '-1', 1, 0)), 3) as pct_seq_colig_menos1,
    round(avg(if(sigla_uf is null, 1, 0)), 3) as pct_sigla_uf_nula,
    array_to_string(
        array_agg(
            distinct tipo_agremiacao ignore nulls order by tipo_agremiacao limit 5
        ),
        ' | '
    ) as amostra_tipo_agremiacao
from `basedosdados.br_tse_eleicoes.partidos`
group by ano
order by ano
;

-- ============================================================================
-- [Q3] vagas — FAIL years: 1994–2012 (controls: 2014+)
-- Predicted symptoms:
-- vagas    <- SG_UF        => safe_cast(uf as int64) => NULL
-- cargo    <- DT_POSSE     => date strings in the office column
-- sigla_uf <- NM_TIPO_ELEICAO => invalid UF => NULL after model cleanup
-- ============================================================================
select
    ano,
    count(*) as n,
    round(avg(if(vagas is null, 1, 0)), 3) as pct_vagas_nulas,
    round(avg(if(sigla_uf is null, 1, 0)), 3) as pct_sigla_uf_nula,
    round(
        avg(if(regexp_contains(cargo, r'\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2}'), 1, 0)), 3
    ) as pct_cargo_eh_data,
    array_to_string(
        array_agg(distinct cargo ignore nulls order by cargo limit 5), ' | '
    ) as amostra_cargo
from `basedosdados.br_tse_eleicoes.vagas`
group by ano
order by ano
;

-- ============================================================================
-- [Q4] detalhes_votacao_municipio_zona — FAIL years: 1996–2016 (controls: 2018+)
-- Predicted symptoms:
-- votos_nominais   <- ST_VOTO_EM_TRANSITO ('S'/'N') => safe_cast int => NULL
-- votos_brancos    <- QT_VOTOS (total)   => brancos ~= comparecimento
-- aptos_totalizadas<- QT_SECOES_NAO_INSTALADAS => collapses to ~0
-- ============================================================================
select
    ano,
    count(*) as n,
    round(avg(if(votos_nominais is null, 1, 0)), 3) as pct_votos_nominais_nulos,
    round(
        safe_divide(sum(votos_brancos), sum(comparecimento)), 3
    ) as razao_brancos_comparecimento,
    round(avg(aptos_totalizadas), 1) as media_aptos_totalizadas,
    round(avg(aptos), 1) as media_aptos
from `basedosdados.br_tse_eleicoes.detalhes_votacao_municipio_zona`
group by ano
order by ano
;

-- ============================================================================
-- [Q5] perfil_eleitorado_municipio_zona — FAIL: 2008–2016, 2020–2024 (control: 2018)
-- Predicted symptoms:
-- zona     <- DS_GENERO        => gender labels in the zone column
-- genero   <- CD_ESTADO_CIVIL  => numeric civil-status codes
-- eleitores<- CD_IDENTIDADE_GENERO (or TP_OBRIGATORIEDADE_VOTO in 2024)
-- => tiny codes instead of voter counts
-- ============================================================================
select
    ano,
    count(*) as n,
    round(avg(if(regexp_contains(zona, r'^\d+$'), 1, 0)), 3) as pct_zona_numerica,
    round(avg(if(regexp_contains(genero, r'^\d+$'), 1, 0)), 3) as pct_genero_numerico,
    round(avg(safe_cast(eleitores as int64)), 2) as media_eleitores,
    array_to_string(
        array_agg(distinct genero ignore nulls order by genero limit 6), ' | '
    ) as amostra_genero
from `basedosdados.br_tse_eleicoes.perfil_eleitorado_municipio_zona`
group by ano
order by ano
;

-- ============================================================================
-- [Q6] resultados_candidato_municipio_zona — FAIL: 1994, 1996, 2016, 2018–2022
-- Predicted symptoms:
-- votos    <- SQ_COLIGACAO (1994/2018+: 11-13 digit sequentials => absurd sums)
-- or SG_FEDERACAO/NR_FEDERACAO (strings/-1 => NULL or -1)
-- resultado<- DS_COMPOSICAO_COLIGACAO => party lists ("PT / PCdoB / ...")
-- numero_partido <- DS_SITUACAO_JULGAMENTO => judgment text, non-numeric
-- ============================================================================
select
    ano,
    count(*) as n,
    max(votos) as max_votos,
    round(avg(if(votos is null or votos < 0, 1, 0)), 3) as pct_votos_nulos_neg,
    round(avg(if(votos > 10000000, 1, 0)), 3) as pct_votos_absurdos,
    round(
        avg(if(regexp_contains(resultado, r'/'), 1, 0)), 3
    ) as pct_resultado_lista_partidos,
    round(
        avg(if(regexp_contains(numero_partido, r'^\d+$'), 1, 0)), 3
    ) as pct_num_partido_numerico
from `basedosdados.br_tse_eleicoes.resultados_candidato_municipio_zona`
group by ano
order by ano
;

-- ============================================================================
-- [Q7] resultados_partido_municipio_zona — FAIL years: 1996, 2016
-- Predicted symptoms:
-- votos_nominais <- SQ_COLIGACAO => absurd magnitudes
-- votos_legenda  <- NM_COLIGACAO => safe_cast int => NULL
-- ============================================================================
select
    ano,
    count(*) as n,
    max(votos_nominais) as max_votos_nominais,
    round(avg(if(votos_nominais > 10000000, 1, 0)), 3) as pct_nominais_absurdos,
    round(avg(if(votos_legenda is null, 1, 0)), 3) as pct_legenda_nula
from `basedosdados.br_tse_eleicoes.resultados_partido_municipio_zona`
group by ano
order by ano
;

-- ============================================================================
-- [Q8] despesas_candidato — FAIL year: 2014 (controls: 2010, 2012, 2016)
-- Predicted symptoms (one-position shift in the 2014 block):
-- cargo            <- "Sigla Partido"      => party siglas in office column
-- tipo_documento   <- "Nome candidato"     => person names
-- cpf_cnpj_fornecedor <- "CPF do vice/suplente" => candidate CPFs, not suppliers
-- ============================================================================
select
    ano,
    count(*) as n,
    round(
        avg(
            if(
                cargo in (
                    'PT',
                    'PSDB',
                    'PMDB',
                    'MDB',
                    'PSD',
                    'PP',
                    'PDT',
                    'PTB',
                    'PSB',
                    'PSOL',
                    'PV',
                    'PSC',
                    'DEM',
                    'PFL',
                    'PR',
                    'PL',
                    'PRB',
                    'PPS',
                    'PCdoB',
                    'PSL',
                    'PODE',
                    'REPUBLICANOS',
                    'UNIAO',
                    'NOVO',
                    'REDE',
                    'AVANTE',
                    'PATRIOTA',
                    'SOLIDARIEDADE',
                    'CIDADANIA'
                ),
                1,
                0
            )
        ),
        3
    ) as pct_cargo_eh_partido,
    array_to_string(
        array_agg(distinct tipo_documento ignore nulls order by tipo_documento limit 5),
        ' | '
    ) as amostra_tipo_documento
from `basedosdados.br_tse_eleicoes.despesas_candidato`
where ano in (2010, 2012, 2014, 2016)
group by ano
order by ano
;

-- ============================================================================
-- [Q9] bens_candidato — positive control (fixed by PR #1564 — harness says OK)
-- Pre-fix symptom was descricao_item <- SG_UF for ano <= 2012. If prod was
-- rebuilt after the fix, pct_descricao_eh_uf must be ~0 in all years.
-- ============================================================================
select
    ano,
    count(*) as n,
    round(
        avg(
            if(
                descricao_item in (
                    'AC',
                    'AL',
                    'AM',
                    'AP',
                    'BA',
                    'CE',
                    'DF',
                    'ES',
                    'GO',
                    'MA',
                    'MG',
                    'MS',
                    'MT',
                    'PA',
                    'PB',
                    'PE',
                    'PI',
                    'PR',
                    'RJ',
                    'RN',
                    'RO',
                    'RR',
                    'RS',
                    'SC',
                    'SE',
                    'SP',
                    'TO',
                    'BR'
                ),
                1,
                0
            )
        ),
        3
    ) as pct_descricao_eh_uf,
    round(
        avg(if(regexp_contains(titulo_eleitoral_candidato, r'^\d{10,13}$'), 1, 0)), 3
    ) as pct_titulo_valido
from `basedosdados.br_tse_eleicoes.bens_candidato`
group by ano
order by ano
;

-- ============================================================================
-- FOLLOW-UP probes — characterize the anomalies surfaced by Q1–Q9
-- ============================================================================
-- [Q10] candidatos 1996 — is the whole demographic block NULL?
-- Result (2026-06-10): all 10,356 rows have titulo_eleitoral, genero,
-- estado_civil, instrucao, nacionalidade, municipio_nascimento = NULL.
select
    countif(titulo_eleitoral is null) as titulo_nulo,
    countif(genero is null) as genero_nulo,
    countif(estado_civil is null) as estado_civil_nulo,
    countif(instrucao is null) as instrucao_nulo,
    countif(nacionalidade is null) as nacionalidade_nula,
    countif(municipio_nascimento is null) as mun_nasc_nulo,
    count(*) as n
from `basedosdados.br_tse_eleicoes.candidatos`
where ano = 1996
;

-- [Q11] detalhes_votacao_municipio_zona 1996 — row coverage + sentinel values.
-- Result: only 548 rows across 19 UFs (vs ~12k rows in comparable municipal
-- years); aptos_totalizadas = -3 (TSE "#NULO#" sentinel) in 100% of rows.
select
    count(*) as n,
    count(distinct sigla_uf) as n_ufs,
    countif(aptos_totalizadas < 0) as aptos_tot_negativos,
    min(aptos_totalizadas) as min_aptos_tot
from `basedosdados.br_tse_eleicoes.detalhes_votacao_municipio_zona`
where ano = 1996
;

-- [Q12] resultados_candidato_municipio_zona 1994 — votos integrity.
-- Result: votos = 0 in ALL 842,223 rows (resultado/sigla_partido look sane).
-- Matches the predicted shift votos <- SG_FEDERACAO (blank pre-federation
-- in the republished 1994 file => parsed empty => 0).
select
    count(*) as n,
    countif(votos = 0) as votos_zero,
    countif(votos is null) as votos_null
from `basedosdados.br_tse_eleicoes.resultados_candidato_municipio_zona`
where ano = 1994
;

-- [Q13] bens_candidato 2014 — titulo_eleitoral_candidato content.
-- Result: 0 NULLs, but every value is a single digit (3..8) =
-- CD_GRAU_INSTRUCAO codes. This is exactly the candidatos-2014 predicted
-- shift (v41: titulo_eleitoral <- CD_GRAU_INSTRUCAO) propagated into bens
-- via the norm_candidatos merge. Smoking gun for the diagnosis.
select
    count(*) as n,
    countif(titulo_eleitoral_candidato is null) as titulo_nulo,
    countif(regexp_contains(titulo_eleitoral_candidato, r'^\d$')) as titulo_1_digito,
    array_to_string(
        array_agg(distinct titulo_eleitoral_candidato ignore nulls limit 8), ' | '
    ) as amostra_titulo
from `basedosdados.br_tse_eleicoes.bens_candidato`
where ano = 2014
;

-- [Q14] perfil_eleitorado_municipio_zona 2008/2016 — mixed value domains.
-- Result: dominant rows use raw TSE codes (genero in 0/2/4, media_eleitores
-- ~35), plus a small residue with text labels (genero 'masculino'/'feminino',
-- media_eleitores ~200) => two different build generations coexist in the
-- same ano partition.
select
    ano,
    genero,
    count(*) as n,
    round(avg(safe_cast(eleitores as int64)), 1) as media_eleitores
from `basedosdados.br_tse_eleicoes.perfil_eleitorado_municipio_zona`
where ano in (2008, 2016)
group by ano, genero
order by ano, n desc
;

-- ============================================================================
-- [Q15–Q18] Dependency-chain propagation probes (2026-06-10). Each confirmed
-- prod anomaly predicts corruption in the tables downstream of it in the
-- dependency graph (DIAGNOSIS.md / DEPENDENCIES.md): phase-3 aggregations
-- inherit their input's values; phase-2 titulo merges inherit norm_candidatos.
-- ============================================================================
-- [Q15] rcmz 1994 (votos = 0, Q12) -> aggregation.py rollups.
-- Result: resultados_candidato_municipio 1994: ALL 705,492 rows votos = 0;
-- resultados_candidato 1994: ALL 6,719 rows votos = 0. Control 1998: 0 zero
-- rows, max votos 3,289,332. Propagation through both aggregation edges
-- confirmed.
select count(*) as n, countif(votos = 0) as n_zero, max(votos) as max_votos
from `basedosdados.br_tse_eleicoes.resultados_candidato_municipio`
where ano = 1994
;

select count(*) as n, countif(votos = 0) as n_zero, max(votos) as max_votos
from `basedosdados.br_tse_eleicoes.resultados_candidato`
where ano = 1994
;

-- [Q16] candidatos 1996 (demographic block incl. titulo_eleitoral NULL, Q10)
-- -> norm_candidatos merge -> titulo_eleitoral_candidato downstream.
-- Result: titulo_eleitoral_candidato is NULL in 100% of 1996 rows of
-- resultados_candidato_municipio_zona (74,903), resultados_candidato_secao
-- (4,075,079) and resultados_candidato (9,983). Controls: 1994 13.2%,
-- 1998 1.2%, 2000 0.7% NULL. Merge-edge propagation confirmed; note rcmz
-- 1996 is corrupted in prod even though its own raw parsing was clean.
select
    ano,
    count(*) as n,
    round(
        100 * countif(titulo_eleitoral_candidato is null) / count(*), 1
    ) as pct_titulo_nulo
from `basedosdados.br_tse_eleicoes.resultados_candidato_municipio_zona`
where ano in (1994, 1996, 1998, 2000)
group by ano
order by ano
;

select
    ano,
    count(*) as n,
    round(
        100 * countif(titulo_eleitoral_candidato is null) / count(*), 1
    ) as pct_titulo_nulo
from `basedosdados.br_tse_eleicoes.resultados_candidato_secao`
where ano in (1994, 1996)
group by ano
order by ano
;

-- [Q17] Did the corrupted candidatos-2014 vintage (proved by bens 2014, Q13)
-- also feed the other titulo-merge consumers built in 2014?
-- Result: NO — receitas_candidato, despesas_candidato, rcmz and rcs 2014 all
-- have 0 single-digit titles and >= 99.7% valid 10–13-digit titles. Only the
-- bens_candidato 2014 build consumed the corrupted candidatos vintage; the
-- phase-2 enrichment ran from different candidatos builds per table.
select
    'receitas_candidato' as tabela,
    count(*) as n,
    countif(regexp_contains(titulo_eleitoral_candidato, r'^\d$')) as n_1_digito,
    countif(regexp_contains(titulo_eleitoral_candidato, r'^\d{10,13}$')) as n_valido
from `basedosdados.br_tse_eleicoes.receitas_candidato`
where ano = 2014
union all
select
    'despesas_candidato',
    count(*),
    countif(regexp_contains(titulo_eleitoral_candidato, r'^\d$')),
    countif(regexp_contains(titulo_eleitoral_candidato, r'^\d{10,13}$'))
from `basedosdados.br_tse_eleicoes.despesas_candidato`
where ano = 2014
union all
select
    'resultados_candidato_municipio_zona',
    count(*),
    countif(regexp_contains(titulo_eleitoral_candidato, r'^\d$')),
    countif(regexp_contains(titulo_eleitoral_candidato, r'^\d{10,13}$'))
from `basedosdados.br_tse_eleicoes.resultados_candidato_municipio_zona`
where ano = 2014
union all
select
    'resultados_candidato_secao',
    count(*),
    countif(regexp_contains(titulo_eleitoral_candidato, r'^\d$')),
    countif(regexp_contains(titulo_eleitoral_candidato, r'^\d{10,13}$'))
from `basedosdados.br_tse_eleicoes.resultados_candidato_secao`
where ano = 2014
;

-- [Q18] dvmz 1996 degradation (548 rows / 19 UFs, Q11) -> aggregation.py.
-- Result: detalhes_votacao_municipio 1996 has 242 municipalities in 19 UFs
-- vs 11,143 (2000) and 11,226 (2004) in comparable municipal-election years.
-- The aptos_totalizadas = -3 sentinel does NOT surface here (dvm aggregates
-- aptos, min = 951), but the coverage collapse propagates intact.
select ano, count(*) as n_munis, count(distinct sigla_uf) as n_uf
from `basedosdados.br_tse_eleicoes.detalhes_votacao_municipio`
where ano in (1996, 2000, 2004)
group by ano
order by ano
;
