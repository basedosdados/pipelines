---
description: Gather context for a Data Basis dataset onboarding (raw sources, docs, coverage, org)
argument-hint: <dataset_slug> [raw_data_path] [drive_folder_path]
---

Gather all context needed to onboard a dataset to Data Basis. Work through the following steps:

**Dataset:** $ARGUMENTS

## Step 1 — Search for official documentation

Search online for the dataset's official documentation, raw data source URLs, and any existing Data Basis presence. Look for:
- Official government or institution page hosting the raw data
- Download URL or API endpoint
- Data dictionary or codebook
- License information (look for open data licenses: CC-BY, CC0, OGL, etc.)
- Update frequency (monthly, annual, etc.)
- Responsible organization

Also read any local README, documentation, or metadata files present in the dataset folder if a path was provided.

## Step 2 — Ask the user to confirm or supplement

Present your findings and ask the user to confirm or fill in:
1. Raw data source URL(s) — where to download the raw files
2. Responsible organization slug on Data Basis (e.g. `ministerio-da-economia`)
3. Core language of the source data (`pt`, `en`, or `es`) — used to pick the style manual
4. Theme slug(s) (e.g. `health`, `education`, `environment`)
5. Tag slug(s) (optional, e.g. `pandemic`, `conservation`)
6. License slug (e.g. `cc`)
7. Update frequency and lag (e.g. annual, 1-year lag)
8. Geographic coverage (e.g. world, Brazil — municipalities, states, federal)
9. Temporal coverage (start year, end year or "ongoing")
10. Drive folder path for architecture tables: `Base dos Dados - Geral/Dados/Conjuntos/<dataset>/`

## Step 3 — Output a structured context block

Once all information is gathered, output a context block in this format for use by downstream skills:

```text
=== DATASET CONTEXT: <slug> ===
Organizations:     <org_slug(s)>
Language:          <pt | en | es>  (core language of source data and descriptions)
Themes:            <theme_slug(s)>
Tags:              <tag_slug(s)>
License:           <license_slug>
Raw source URL(s): <url(s)>
Update frequency:  <e.g. annual>
Update lag:        <e.g. 1 year>
Coverage:          <start_year>–<end_year or "present">, <geography>
Drive folder:      <path>
Notes:             <any other relevant info>
```

Keep the context block in the conversation for subsequent skills to reference.
