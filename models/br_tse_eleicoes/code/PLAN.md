# br_tse_eleicoes: Stata → Python Refactoring Plan

## Goal

Rewrite the entire br_tse_eleicoes ETL pipeline from Stata (.do files) to Python.
The final partitioned CSV outputs must exactly match the current Stata outputs.
Existing Stata code and outputs are NOT deleted. Python outputs go to `output_python/` for comparison.

## Decisions

- **Python version**: >=3.10,<3.11 (per AGENTS.md)
- **Libraries**: Use best tool for the job (pandas, polars, etc.)
- **Intermediate format**: Parquet for internal pipeline use; CSV only for final partitioned output
- **Output directory**: `output_python/` sibling to `output/`
- **`local_votacao.do` (line 43 of build.do)**: Ignore — handled by `perfil_eleitorado_local_votacao.do`
- **`certidoes_criminais`**: Ignore — not in current Stata pipeline
- **Commits**: Atomic logical steps, no "Co-Authored-By" line

## Key Risks

- **Schema drift**: TSE files have 12+ different column layouts per table across years. Each year-specific parsing block must be faithfully reproduced.
- **String encoding**: TSE files use Latin-1 with Portuguese accents. `clean_string` must produce identical output to Stata version.
- **Floating-point precision**: Stata's `compress`/`destring` have specific rounding. Compare proportions carefully.
- **Duplicate handling order**: Stata's `duplicates drop ..., force` keeps first row in physical order. Must match sort order before dedup.
- **Memory**: Some files (despesas 2022, resultados_secao SP) are very large. May need chunked processing.

---

## Directory Structure

All Python code under `models/br_tse_eleicoes/code/python/`:

```
python/
├── build.py                     # Main entry point (equivalent of build.do)
├── config.py                    # Paths, constants, state lists per year
├── validate.py                  # Compare Python vs Stata outputs
├── utils/
│   ├── __init__.py
│   ├── clean_string.py          # ← fnc/clean_string.do
│   ├── clean_election_type.py   # ← fnc/limpa_tipo_eleicao.do
│   ├── clean_education.py       # ← fnc/limpa_instrucao.do
│   ├── clean_marital_status.py  # ← fnc/limpa_estado_civil.do
│   ├── clean_result.py          # ← fnc/limpa_resultado.do
│   ├── clean_party.py           # ← fnc/limpa_partido.do
│   ├── fix_candidate.py         # ← fnc/limpa_candidato.do
│   └── helpers.py               # Shared: date parsing, null cleaning, CSV I/O, partitioning
└── sub/
    ├── __init__.py
    ├── candidates.py
    ├── parties.py
    ├── voting_details_mun_zone.py
    ├── voting_details_section.py
    ├── voting_details_state.py
    ├── voter_profile_mun_zone.py
    ├── voter_profile_section.py
    ├── voter_profile_polling_place.py
    ├── vacancies.py
    ├── results_mun_zone.py
    ├── results_section.py
    ├── results_state.py
    ├── campaign_finance.py       # bens + receitas + despesas (all 3 sections)
    ├── normalization_partition.py
    └── aggregation.py
```

---

## Implementation Phases

### Phase 0: Scaffolding & Infrastructure
**Status: DONE**

1. Create directory structure
2. `config.py` — centralize paths, state lists, year ranges
3. `utils/helpers.py` — shared functions:
   - `read_raw_csv()` — TSE semicolon-delimited files (encoding, header skip, column selection)
   - `parse_date_br()` — DD/MM/YYYY → YYYY-MM-DD
   - `clean_nulls()` — replace #NULO#, #NE#, -1, -3, -4 with empty
   - `pad_cpf()`, `pad_titulo()` — left-pad with zeros
   - `save_partitioned()` — Hive-style ano=YYYY/sigla_uf=XX/ CSV export
4. `validate.py` — reads .dta via pandas, compares against Python CSV: row counts, columns, values

### Phase 1: Cleaning Functions (utils/)
**Status: DONE**

5. `clean_string.py` — accent removal, lowercase, trim
6. `clean_election_type.py` — election type standardization
7. `clean_education.py` — education level normalization
8. `clean_marital_status.py` — marital status normalization
9. `clean_result.py` — election result normalization
10. `clean_party.py` — party name standardization (year-conditional mergers)
11. `fix_candidate.py` — manual candidate corrections

### Phase 2: Core Table Builders (sub/) — no interdependencies
**Status: NOT STARTED**

These can be implemented in any order. Each reads raw TSE input and writes intermediate parquet + validates against existing .dta:

12. `candidates.py` — 1994-2024 (LARGEST, most complex)
13. `parties.py` — 1990-2024
14. `vacancies.py` — 1994-2024
15. `voting_details_mun_zone.py` — 1994-2024
16. `voting_details_section.py` — 1994-2024
17. `voting_details_state.py` — 1945-1990 (historical)
18. `voter_profile_mun_zone.py` — 1994-2024
19. `voter_profile_section.py` — 2008-2024
20. `voter_profile_polling_place.py` — 2010-2024
21. `results_mun_zone.py` — 1994-2024 (candidate + party results at mun-zona)
22. `results_section.py` — 1994-2024 (COMPLEX: separates candidate vs party/legend votes)
23. `results_state.py` — 1945-1990 (historical)
24. `campaign_finance.py` — bens 2006+, receitas 2002+, despesas 2002+ (COMPLEX: wildly different schemas per year)

### Phase 3: Normalization, Partitioning & Aggregation (depends on Phase 2)
**Status: NOT STARTED**

25. `normalization_partition.py` — appends all years, deduplicates candidates, creates norm_candidatos/norm_partidos, writes final Hive-partitioned CSVs for ALL tables
26. `aggregation.py` — collapses mun-zona to municipality level, creates resultados_candidato (all-year aggregation)

### Phase 4: Runner & Final Validation
**Status: NOT STARTED**

27. `build.py` — orchestrates full pipeline, supports running individual steps
28. Run `validate.py` against every output table/year, document discrepancies

---

## Output Schema Reference

All final output CSVs go to `output_python/` with Hive-style partitioning.
Partition columns (ano, sigla_uf) are NOT in the CSV files — they are in the folder path.

### Partitioning Structures

| Table | Partitioning | Years |
|-------|-------------|-------|
| candidatos | `ano=YYYY/` | 1994-2024 (even) |
| partidos | `ano=YYYY/` | 1990, 1994-2024 (even) |
| bens_candidato | `ano=YYYY/` | 2006-2024 (even) |
| despesas_candidato | `ano=YYYY/` | 2002-2024 (even) |
| receitas_candidato | `ano=YYYY/` | 2002-2024 (even) |
| resultados_candidato | `ano=YYYY/` | 1945-2024 (mixed) |
| vagas | `ano=YYYY/sigla_uf=XX/` | 1994-2024 (even) |
| detalhes_votacao_municipio_zona | `ano=YYYY/sigla_uf=XX/` | 1994-2024 (even) |
| detalhes_votacao_secao | `ano=YYYY/sigla_uf=XX/` | 1994-2024 (even) |
| detalhes_votacao_municipio | `ano=YYYY/sigla_uf=XX/` | 1994-2024 (even) |
| resultados_candidato_municipio_zona | `ano=YYYY/sigla_uf=XX/` | 1994-2024 (even) |
| resultados_candidato_secao | `ano=YYYY/sigla_uf=XX/` | 1994-2024 (even) |
| resultados_candidato_municipio | `ano=YYYY/sigla_uf=XX/` | 1994-2024 (even) |
| resultados_partido_municipio_zona | `ano=YYYY/sigla_uf=XX/` | 1994-2024 (even) |
| resultados_partido_secao | `ano=YYYY/sigla_uf=XX/` | 1994-2024 (even) |
| resultados_partido_municipio | `ano=YYYY/sigla_uf=XX/` | 1994-2024 (even) |
| perfil_eleitorado_municipio_zona | `ano=YYYY/sigla_uf=XX/` | 1994-2024 (even) |
| perfil_eleitorado_secao | `ano=YYYY/sigla_uf=XX/` | 2008-2024 (even) |
| perfil_eleitorado_local_votacao | `ano=YYYY/sigla_uf=XX/` | 2010-2024 (even) |

### Column Schemas (from existing 2024 CSVs)

#### candidatos
```
id_eleicao, tipo_eleicao, data_eleicao, sigla_uf, id_municipio, id_municipio_tse,
titulo_eleitoral, cpf, sequencial, numero, nome, nome_urna, numero_partido,
sigla_partido, cargo, email, situacao, nacionalidade, sigla_uf_nascimento,
municipio_nascimento, data_nascimento, idade, genero, instrucao, estado_civil,
raca, ocupacao
```

#### partidos
```
turno, id_eleicao, tipo_eleicao, data_eleicao, sigla_uf, id_municipio_tse, cargo,
id_municipio, numero, sigla, nome, tipo_agremiacao, sequencial_coligacao,
nome_coligacao, composicao_coligacao, numero_federacao, nome_federacacao,
sigla_federacao, composicao_federacao, situacao_legenda
```

#### bens_candidato
```
id_eleicao, tipo_eleicao, data_eleicao, sigla_uf, titulo_eleitoral_candidato,
sequencial_candidato, tipo_item, descricao_item, valor_item
```

#### despesas_candidato
```
turno, id_eleicao, tipo_eleicao, data_eleicao, sigla_uf, id_municipio, id_municipio_tse,
titulo_eleitoral_candidato, sequencial_candidato, numero_candidato, numero_partido,
sigla_partido, cargo, sequencial_despesa, data_despesa, tipo_despesa, descricao_despesa,
origem_despesa, valor_despesa, tipo_prestacao_contas, data_prestacao_contas,
sequencial_prestador_contas, cnpj_prestador_contas, cnpj_candidato, tipo_documento,
numero_documento, especie_recurso, fonte_recurso, cpf_cnpj_fornecedor, nome_fornecedor,
nome_fornecedor_rf, cnae_2_fornecedor, descricao_cnae_2_fornecedor, tipo_fornecedor,
esfera_partidaria_fornecedor, sigla_uf_fornecedor, id_municipio_tse_fornecedor,
sequencial_candidato_fornecedor, numero_candidato_fornecedor, numero_partido_fornecedor,
sigla_partido_fornecedor, cargo_fornecedor
```

#### receitas_candidato
```
turno, id_eleicao, tipo_eleicao, data_eleicao, sigla_uf, id_municipio, id_municipio_tse,
titulo_eleitoral_candidato, sequencial_candidato, numero_candidato, cnpj_candidato,
numero_partido, sigla_partido, cargo, sequencial_receita, data_receita, fonte_receita,
origem_receita, natureza_receita, especie_receita, situacao_receita, descricao_receita,
valor_receita, sequencial_candidato_doador, cpf_cnpj_doador, sigla_uf_doador,
id_municipio_tse_doador, nome_doador, nome_doador_rf, cargo_candidato_doador,
numero_partido_doador, sigla_partido_doador, esfera_partidaria_doador,
numero_candidato_doador, cnae_2_doador, descricao_cnae_2_doador, cpf_cnpj_doador_orig,
nome_doador_orig, nome_doador_orig_rf, tipo_doador_orig, descricao_cnae_2_doador_orig,
nome_administrador, cpf_administrador, numero_recibo_eleitoral, numero_documento,
numero_recibo_doacao, numero_documento_doacao, tipo_prestacao_contas,
data_prestacao_contas, sequencial_prestador_contas, cnpj_prestador_contas,
entrega_conjunto
```

#### resultados_candidato
```
turno, id_eleicao, tipo_eleicao, data_eleicao, sigla_uf, id_municipio, id_municipio_tse,
cargo, numero_partido, sigla_partido, titulo_eleitoral_candidato, sequencial_candidato,
numero_candidato, nome_candidato, resultado, votos
```

#### vagas
```
id_eleicao, tipo_eleicao, data_eleicao, id_municipio, id_municipio_tse, cargo, vagas
```

#### detalhes_votacao_municipio_zona
```
turno, id_eleicao, tipo_eleicao, data_eleicao, id_municipio, id_municipio_tse, zona,
cargo, aptos, secoes, secoes_agregadas, aptos_totalizadas, secoes_totalizadas,
comparecimento, abstencoes, votos_validos, votos_brancos, votos_nulos, votos_nominais,
votos_legenda, proporcao_comparecimento, proporcao_votos_validos,
proporcao_votos_brancos, proporcao_votos_nulos
```

#### detalhes_votacao_secao
```
turno, id_eleicao, tipo_eleicao, data_eleicao, id_municipio, id_municipio_tse, zona,
secao, cargo, aptos, comparecimento, abstencoes, votos_nominais, votos_brancos,
votos_nulos, votos_legenda, votos_nulos_apu_sep, proporcao_comparecimento,
proporcao_votos_nominais, proporcao_votos_legenda, proporcao_votos_brancos,
proporcao_votos_nulos
```

#### detalhes_votacao_municipio
```
turno, id_eleicao, tipo_eleicao, data_eleicao, id_municipio, id_municipio_tse, cargo,
aptos, secoes, secoes_agregadas, aptos_totalizadas, secoes_totalizadas,
comparecimento, abstencoes, votos_validos, votos_brancos, votos_nulos, votos_nominais,
votos_legenda, proporcao_comparecimento, proporcao_votos_validos,
proporcao_votos_brancos, proporcao_votos_nulos
```

#### resultados_candidato_municipio_zona
```
turno, id_eleicao, tipo_eleicao, data_eleicao, id_municipio, id_municipio_tse, zona,
cargo, numero_partido, sigla_partido, titulo_eleitoral_candidato, sequencial_candidato,
numero_candidato, resultado, votos
```

#### resultados_candidato_secao
```
turno, id_eleicao, tipo_eleicao, data_eleicao, id_municipio, id_municipio_tse, zona,
secao, cargo, numero_partido, sigla_partido, titulo_eleitoral_candidato,
sequencial_candidato, numero_candidato, votos
```

#### resultados_candidato_municipio
```
turno, id_eleicao, tipo_eleicao, data_eleicao, id_municipio, id_municipio_tse, cargo,
numero_partido, sigla_partido, titulo_eleitoral_candidato, sequencial_candidato,
numero_candidato, resultado, votos
```

#### resultados_partido_municipio_zona
```
turno, id_eleicao, tipo_eleicao, data_eleicao, id_municipio, id_municipio_tse, zona,
cargo, numero_partido, sigla_partido, votos_nominais, votos_legenda
```

#### resultados_partido_secao
```
turno, id_eleicao, tipo_eleicao, data_eleicao, id_municipio, id_municipio_tse, zona,
secao, cargo, numero_partido, sigla_partido, votos_nominais, votos_legenda
```

#### resultados_partido_municipio
```
turno, id_eleicao, tipo_eleicao, data_eleicao, id_municipio, id_municipio_tse, cargo,
numero_partido, sigla_partido, votos_nominais, votos_legenda
```

#### perfil_eleitorado_municipio_zona
```
id_municipio, id_municipio_tse, situacao_biometria, zona, genero, estado_civil,
grupo_idade, instrucao, eleitores, eleitores_biometria, eleitores_deficiencia,
eleitores_inclusao_nome_social
```

#### perfil_eleitorado_secao
```
id_municipio, id_municipio_tse, zona, secao, genero, estado_civil, grupo_idade,
instrucao, situacao_biometria, eleitores, eleitores_biometria, eleitores_deficiencia,
eleitores_inclusao_nome_social
```

#### perfil_eleitorado_local_votacao
```
turno, id_municipio, id_municipio_tse, zona, secao, tipo_secao_agregada, numero, nome,
tipo, endereco, bairro, cep, telefone, latitude, longitude, situacao, situacao_zona,
situacao_secao, situacao_localidade, situacao_secao_acessibilidade, eleitores_secao
```

---

## Validation Strategy

For each table × year, compare Python output against Stata reference:
1. **Row count** must match exactly
2. **Column names and order** must match exactly
3. **Values**: sample-based comparison (first 100, last 100, random 100 rows)
4. **Numeric columns**: check min, max, mean, sum, null count
5. **String columns**: check unique count, null count, sample values

The .dta files are the ground truth for intermediate validation.
The final partitioned CSVs are the ground truth for end-to-end validation.

---

## Session Instructions

Each implementation session should:
1. Read this PLAN.md first
2. Check which phase is current (look at Status fields above)
3. Implement the next incomplete phase
4. After completing a phase, update the Status field in this file
5. Commit the phase with a clear message
6. Run validate.py against completed outputs before moving to the next phase
