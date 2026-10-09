# us_dol_oflc — project context

Office of Foreign Labor Certification (OFLC) case disclosure data from the U.S.
Department of Labor: every Labor Condition Application (LCA, covering H-1B,
H-1B1 and E-3), PERM permanent labor certification, H-2A agricultural and H-2B
non-agricultural temporary certification, FY2008 to present.

## Non-obvious facts

- **`year` is the FISCAL year** (1 October – 30 September), not the calendar
  year. It is the partition column and comes from the source *file*, not from
  any date in the row. A case decided 2024-11-05 sits in FY2025.
- **www.dol.gov blocks scripted downloads.** `requests`, `curl` and `wget` all
  get HTTP 403 from Akamai regardless of headers — the block keys on the TLS
  fingerprint. Downloads go through `curl_cffi` with `impersonate="chrome"`.
- **The column set changes almost every year.** The year-to-canonical mapping in
  `code/crosswalk/` is the authoritative artifact; the cleaning code reads it and
  refuses to run on a source column it does not know.
- **Wages are reported in mixed units.** The source pairs a wage amount with a
  separate unit field (Hour / Week / Bi-Weekly / Month / Year). Both the source
  pair and a derived annualised column are published; the annualisation is
  documented in `code/crosswalk/wage_units.csv` and is NULL wherever the unit is
  missing or unrecognised, never guessed.
- **FY2009 LCA needs two source files** — the legacy H-1B eFile extract and the
  iCERT extract cover different parts of the year.
- **FY2010 LCA has no wage-unit column at all**, so every annualised wage for
  FY2010 is NULL.

## Layout

```
code/
├── architecture/     # one CSV per table — the schema source of truth
├── columns_json/     # backend column payloads, generated from architecture/
├── crosswalk/        # year → canonical column map (the deliverable)
├── build_crosswalk.py
├── clean_data.py
└── upload.py
```

Scratch data lives in `~/Downloads/us_dol_oflc_data/{input,output}` (override
with `OFLC_DATA_DIR`); it is never written into the repo or into Dropbox.
