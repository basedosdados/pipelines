---
name: context
description: Gathers all context needed to onboard a dataset to Data Basis — raw source URLs, documentation, organization, themes, coverage, and Drive folder path.
tools:
  - Read
  - WebFetch
  - WebSearch
  - mcp__databasis-workspace__list_drive_items
  - mcp__databasis-workspace__search_drive_files
---

# Context Agent

Gather all context needed to onboard a dataset to Data Basis.

## Input

Dataset slug and any known information (source URL, Drive folder, organization).

## Step 1 — Search for official documentation

Search online for the dataset's official documentation, raw data source URLs, and any existing Data Basis presence. Look for:

- Official government or institution page hosting the raw data
- Download URL or API endpoint
- Data dictionary or codebook
- Update frequency (monthly, annual, etc.)
- Responsible organization

Also read any local README, documentation, or metadata files present in the dataset folder if a path was provided.

**If the human did not explicitly provide a raw data source URL:** enumerate every downloadable file available at the source (e.g. list all CSVs, ZIPs, or API endpoints found on the official page). List filenames and URLs explicitly before proceeding — do not assume a single file covers the full dataset.

**If the human already provided a raw data source URL:** do not go looking for additional files unless they explicitly ask. Use only what was provided.

**Source freshness check:** Identify the file's last-modified or extraction date (from HTTP headers, a `data_extracao` column, or the source page). If the most recent available data is more than 12 months old, flag it to the user before downloading:

```text
⚠ Source data appears to be from <date> (<N> months ago). Confirm you want to proceed with this version, or provide an updated source URL.
```

## Step 2 — Ask the user to confirm or supplement

Present your findings and ask the user to confirm or fill in:

1. Raw data source URL(s) — where to download the raw files
2. Responsible organization slug on Data Basis (e.g. `ministerio-da-economia`)
3. Core language of the source data (`pt`, `en`, or `es`)
4. Theme slug(s) (e.g. `health`, `education`, `environment`)
5. Tag slug(s) (optional)
6. License slug (e.g. `cc`)
7. Update frequency and lag (e.g. annual, 1-year lag)
8. Geographic coverage (e.g. Brazil — municipalities, states, federal)
9. Temporal coverage (start year, end year or "ongoing")
10. Drive folder path: `Base dos Dados - Geral/Dados/Conjuntos/<dataset>/`

## Step 3 — Output a structured context block

Once all information is gathered, output this block for downstream agents:

```text
=== DATASET CONTEXT: <slug> ===
Organizations:     <org_slug(s)>
Language:          <pt | en | es>
Themes:            <theme_slug(s)>
Tags:              <tag_slug(s)>
Raw source URL(s): <url(s)>
Update frequency:  <e.g. annual>
Update lag:        <e.g. 1 year>
Coverage:          <start_year>–<end_year or "present">, <geography>
Drive folder:      <path>
Notes:             <any other relevant info>
```

Keep this context block in the conversation for all subsequent agents.
