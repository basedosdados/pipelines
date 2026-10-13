# PRF federal highway crash data — source inspection

All figures below are measured from the 51 published archives, downloaded 2026-09-28.
No figure is inferred from documentation.

## 1. What the source publishes

Twenty years, 2007–2026, at https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-acidentes

Files are zipped CSVs hosted on Google Drive, one Drive file id per year per shape.
Download endpoint that works without a browser:
`https://drive.usercontent.google.com/download?id=<id>&export=download&confirm=t`

Three shapes, not two:

| Shape | Source filename | Years | Rows | Grain |
|---|---|---|---|---|
| Agrupados por ocorrência | `datatran<YYYY>.csv` | 2007–2026 | 2,243,625 | one row per crash |
| Agrupados por pessoa | `acidentes<YYYY>.csv` | 2007–2026 | 5,156,169 | one row per person × vehicle |
| Agrupados por pessoa — todas as causas e tipos | `acidentes<YYYY>_todas_causas_tipos.csv` | 2017–2026 | 4,479,488 | one row per person × vehicle × cause × type |

The third shape is a 2017-onward addition. It exists because from 2017 a crash carries
multiple causes and multiple types; it adds `causa_principal` and `ordem_tipo_acidente`
and repeats each person once per (cause, type) pair. It is built as `pessoa_causa_tipo`.

## 2. Per-year column inventory

`ocorrencia` — 4 regimes, 3 break points:

| Years | Cols | Sep | Date format | Decimal | `ano` col | Coords | Accents in `municipio` |
|---|---|---|---|---|---|---|---|
| 2007–2011 | 26 | `;` | `DD/MM/YYYY` | `.` | yes | no | yes |
| 2012–2015 | 26 | `;` | `YYYY-MM-DD` | `.` | yes | no | yes |
| 2016 | 25 | `;` | `DD/MM/YY` | `,` | **dropped** | no | yes |
| 2017–2026 | 30 | `;` | `YYYY-MM-DD` | `,` | no | **added** | **stripped** |

`pessoa` — 3 regimes, 2 break points:

| Years | Cols | Sep | Date format | Decimal | Coords | Accents in `municipio` |
|---|---|---|---|---|---|---|
| 2007–2015 | 28 | **`,`** | `DD/MM/YYYY` | `.` | no | yes |
| 2016 | 28 | `;` | `DD/MM/YY` | `,` | no | yes |
| 2017–2026 | 35 | `;` | `YYYY-MM-DD` | `,` | **added** | **stripped** |

Column-set changes, exactly:

- `ocorrencia` 2016: `ano` removed (25 cols). Recoverable from `data_inversa`.
- `ocorrencia` 2017: `latitude`, `longitude`, `regional`, `delegacia`, `uop` added (30 cols).
- `pessoa` 2017: `nacionalidade`, `naturalidade` removed; `ilesos`, `feridos_leves`,
  `feridos_graves`, `mortos`, `latitude`, `longitude`, `regional`, `delegacia`, `uop`
  added (28 → 35 cols).

The non-obvious break: **`data_inversa` diverges between the two shapes for 2012–2015.**
`ocorrencia` is ISO `YYYY-MM-DD` in those four years while `pessoa` is still
`DD/MM/YYYY`. A per-year map keyed on year alone is wrong; the map must be keyed on
(shape, year).

Every file is internally homogeneous: each has exactly one date format across 100% of
its rows, and one decimal convention. No file mixes them.

## 3. Three premises in the brief that the data contradicts

1. **Encoding does not drift.** All 51 files are latin-1. None is valid UTF-8, none
   carries a BOM, and zero mojibake sequences (`Ã<80-bf>`, `Â.`) appear in any file.
   One decode path, not a per-year map.
2. **Column names do not drift.** Names are stable within each regime and reused across
   regimes; nothing is renamed. The drift is structural (columns added/removed) and
   value-level (formats), which is a different and easier problem.
3. **`municipio` is a name in every year, never a code.** Zero numeric values in
   7,399,794 rows across both shapes. The name/code split does not exist; what changes
   at 2017 is that PRF stops emitting accents.

## 4. Grain and referential integrity

`ocorrencia`: `id` is unique from 2017 onward. 2007–2016 carry 1–7 fully duplicated rows
per year (35 rows total, 0.0016%). A strict `unique_combination_of_columns(ano, id)` test
fails on those years.

`pessoa`: the unique key is **`(id, pesid, id_veiculo)`**, not `(id, pesid)`. From 2017,
`(id, pesid)` alone leaves 4,325–6,716 duplicate rows per year (~3%) because a person is
recorded against more than one vehicle; adding `id_veiculo` reduces that to exactly 0.
Pre-2017 a handful of true duplicate rows survive even on the full key (23 in 2007,
falling to 3 by 2016). Neither `pesid` nor `id_veiculo` is ever blank.

Cross-table: every `pessoa.id` resolves to an `ocorrencia.id` from 2013 onward. Earlier
years have crash ids present in `pessoa` but absent from `ocorrencia`:

| Year | 2007 | 2008 | 2009 | 2010 | 2011 | 2012 | 2013–2026 |
|---|---|---|---|---|---|---|---|
| orphan crash ids | 773 | 82 | 1 | 14 | 4 | 2 | 0 |

The reverse never happens: no `ocorrencia.id` is missing from `pessoa` in any year. A
`relationships` test from `pessoa` to `ocorrencia` therefore needs a tolerance for
2007–2012 or it fails.

## 5. Municipality resolution — measured, not projected

2,507 distinct (uf, municipio) pairs appear across all years. Resolution chain against
`basedosdados.br_bd_diretorios_brasil.municipio` (5,571 rows, no ambiguous names within
any UF):

| Step | Pairs resolved | Rows |
|---|---|---|
| Accent- and case-insensitive match on (uf, name) | 2,456 | 7,374,187 |
| Same, additionally stripping apostrophes/hyphens/spaces | +25 | +14,322 |
| Manual override list (18 entries, IBGE renames) | +18 | +25,548 |
| Name-only match where `uf` is null and the name is nationally unique | +2 | +43 |
| **Unresolvable** | **6** | **16** |

Residual is **16 rows in 7,399,794 (0.00022%)**, by year:

| Shape | Year | Residual | Cause |
|---|---|---|---|
| ocorrencia | 2007 | 1 | `PB\|NOVA SERRANA` — Nova Serrana is in MG; source UF is wrong |
| pessoa | 2007 | 3 | same |
| pessoa | 2010 | 12 | `municipio` empty in the source |
| all other shape-years | | 0 | |

Both residual classes stay NULL. The 4 UF-mismatch rows are a source error, not a
matching failure; resolving them by name alone would silently overwrite a wrong UF.

The apostrophe class is what a naive normalizer misses: PRF writes `SAO MIGUEL DOESTE`
and `ITAPORANGA DAJUDA` where the directory has `São Miguel do Oeste` and
`Itaporanga d'Ajuda`. Replacing the apostrophe with a space produces `D OESTE` and fails;
deleting it produces `DOESTE` and matches.

Override list, every code verified against the directory:

| PRF (uf, name) | IBGE code | Directory name | Reason |
|---|---|---|---|
| SP, EMBU | 3515004 | Embu das Artes | renamed 2011 |
| RJ, PARATI | 3303807 | Paraty | spelling |
| SC, PICARRAS | 4212809 | Balneário Piçarras | renamed 2004 |
| SC, SAO MIGUEL DOESTE | 4217204 | São Miguel do Oeste | apostrophe + spacing |
| PA, SANTA IZABEL DO PARA | 1506500 | Santa Isabel do Pará | spelling (Iz/Is) |
| PE, BELEM DE SAO FRANCISCO | 2601607 | Belém do São Francisco | de/do |
| BA, MUQUEM DO SAO FRANCISCO | 2922250 | Muquém de São Francisco | do/de |
| RN, ASSU | 2400208 | Açu | spelling |
| CE, ITAPAJE | 2306306 | Itapagé | spelling |
| PA, ELDORADO DOS CARAJAS | 1502954 | Eldorado do Carajás | dos/do |
| PB, SAO BENTO DE POMBAL | 2513927 | São Bentinho | renamed |
| PB, SAO DOMINGOS DE POMBAL | 2513968 | São Domingos | renamed |
| RN, AUGUSTO SEVERO | 2401305 | Campo Grande | renamed 2013 |
| PR, VILA ALTA | 4128625 | Alto Paraíso | renamed 2010 |
| MT, POXOREU | 5107008 | Poxoréo | spelling |
| RO, VILA NOVA DO MAMORE | 1100338 | Nova Mamoré | renamed |
| SC, BARRA DO SUL | 4202057 | Balneário Barra do Sul | renamed |
| PB, SANTAREM | 2513653 | Joca Claudino | renamed 2010 |

## 6. Coordinates

Present only 2017–2026, comma decimal separator, never empty. Quality is high:

| Year | Rows | `0,0` pairs | Outside Brazil | Unparseable |
|---|---|---|---|---|
| 2017 | 89,567 | 7 | 49 | 0 |
| 2018–2023 | — | 0 | 0 | 0 |
| 2024 | 73,202 | 0 | 0 | 0 |
| 2025–2026 | — | 0 | 0 | 0 |

2017 is the only year with bad coordinates: 56 rows, 0.06%. One 2024 row at
(-3.84, -32.41) is Fernando de Noronha, inside Brazil — a bounding box that excludes it
is too tight.

### Validated against `br_geobr_mapas` after loading

Run on the materialized `ocorrencia` table, 681,393 points:

| Check | Result |
|---|---|
| Outside every Brazilian UF polygon | 1,188 (0.174%), of which 7 are exact `0,0` |
| Outside the municipality they are attributed to | 47,573 of 681,385 (6.98%) |

The 7% figure is not a defect in the municipality resolution. `id_municipio` comes from
PRF's administrative municipality field, not from the coordinate, and the two disagree
for highway crashes near a municipal boundary. Use the coordinate for spatial work and
`id_municipio` for administrative aggregation; they answer different questions.

A value that cannot be a coordinate is set to NULL: 2017 contains latitudes such as
`-1033382874`, which is `-10.33382874` with the decimal separator dropped. Five latitudes
and 33 longitudes in `ocorrencia` are affected, all in 2017. The point is not repaired by
guessing where the separator belonged. In-range values are kept raw, including the 1,188
that fall outside Brazil.

## 7. Other value-level facts that affect typing

`idade` missing sentinel changes: `-1` for 2007–2015 (25,118–59,831 rows/year, 9–15%),
`0` for 2017–2026 (22,800–38,058 rows/year, 17–19%). 2016 uses neither at scale.
From 2017 the sentinel collides with genuine infants under one year, which pre-2017 were
coded `0` alongside a `-1` missing marker (~400 rows/year). Age 0 from 2017 onward cannot
be read as "newborn"; that information is destroyed in the source.

`km` mixes integer and fractional values in every year; the decimal separator is the only
thing that changes.

`br` is a highway number with no decimals and no arithmetic meaning — a route label.
Per house convention it is STRING, not INT64.
