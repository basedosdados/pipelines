# us_epu_gpr — onboarding design

Two newspaper-based uncertainty families on one monthly (and daily) axis:

- **EPU** — Economic Policy Uncertainty (Baker, Bloom and Davis), hosted at
  policyuncertainty.com. Global (GEPU), country-level, US components, and US
  category-specific indices. Licensed **CC BY 4.0**; "these data can be used
  freely with attribution to the authors, the paper, and the website."
- **GPR** — Geopolitical Risk (Caldara and Iacoviello, Board of Governors of the
  Federal Reserve System), hosted at matteoiacoviello.com. Global (recent from
  1985, historical from 1900), country, and daily. Free to use with citation to
  Caldara and Iacoviello (2022), "Measuring Geopolitical Risk," *American
  Economic Review* 112(4).

Both families are **re-materialized in full every refresh** — the sources revise
history whenever article counts are recomputed, so the pipeline replaces rather
than appends.

## Tables (LONG)

| table | grain | partition | rows (2026-09) |
|---|---|---|---|
| `index_monthly` | country × index series × month | `year` (int64) | 113,007 |
| `index_daily` | country × index series × date | `year` (int64) | 60,928 |
| `dicionario` | index_family / index_name → label | — | 60 |

Shared long schema: `country_id` (ISO3, NULL for global aggregates),
`index_family` (`epu`/`gpr`), `index_name` (coded series, resolved by the
dicionario), `value`. `index_daily` adds `day` and a `date` column.

### What maps to what

- **EPU global** → `country_id = NULL`, `index_name` ∈ {`gepu_current`, `gepu_ppp`}.
- **EPU country** (All_Country_Data) → ISO3 `country_id`, `index_name = epu`;
  the two extra China panels are `epu_scmp` / `epu_mainland`.
- **US EPU** → USA. Headline news-based index (1900+) is `epu`; the four legacy
  components are `epu_three_component`, `epu_fedstatelocal_disagreement`,
  `epu_cpi_disagreement`, `epu_tax_expiration`.
- **US categorical EPU** → USA, `epu_cat_*` (12 categories).
- **GPR global** → NULL, `gpr`/`gpr_threats`/`gpr_acts` (recent) and
  `gpr_historical*` (from 1900).
- **GPR country** (GPRC_*/GPRHC_*) → ISO3, `gpr` / `gpr_historical` (stored by the
  source as a **share of articles in percent**, unlike the base-100 global index).
- **Daily**: US EPU (`epu_news`, USA) + global GPR (`gpr`/`gpr_acts`/`gpr_threats`).

### Deliberately excluded from GPR export

The GPR workbook ships methodological variants beyond the index levels:
`SHARE_*`, `N10`/`N3H` (article counts), `*_NOEW` / `*_AND` / `*_BASIC`
(alternative search criteria), `SHAREH_CAT_*` (category shares), and the daily
`GPRD_MA7`/`GPRD_MA30` moving averages and `event` text. These are excluded to
keep `value` a single index-level measure; they can be added later if needed.

## Comparability caveat

EPU country indices use different base periods and different newspaper panels, so
cross-country level comparisons are not meaningful — stated in the dataset and
table descriptions.

## Source files

| file | host | content |
|---|---|---|
| `Global_Policy_Uncertainty_Data.xlsx` | policyuncertainty.com/media | GEPU (1997+) |
| `US_Policy_Uncertainty_Data.xlsx` | " | US news (1900+) + legacy components |
| `All_Country_Data.xlsx` | " | country EPU panel |
| `Categorical_EPU_Data.xlsx` | " | US categorical EPU |
| `All_Daily_Policy_Data.csv` | " | US daily EPU |
| `data_gpr_export.xls` | matteoiacoviello.com/gpr_files | GPR monthly (global + country) |
| `data_gpr_daily_recent.xls` | " | GPR daily (global) |

policyuncertainty.com sits behind a GoDaddy/Sucuri WAF: HTML page paths return an
interstitial, but direct `/media/*` file downloads succeed with a browser
User-Agent.

## Code

- `pipelines/datasets/us_epu_gpr/` — `constants.py`, `utils.py` (pure download +
  transform), `tasks.py`, `flows.py`.
- `models/us_epu_gpr/code/` — architecture CSVs (schema source of truth),
  `clean_data.py` (bootstrap, imports the transform), `upload.py`.

Bootstrap: `uv run --no-project python models/us_epu_gpr/code/clean_data.py --download`.
