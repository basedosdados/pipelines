---
description: Open a pull request for a Data Basis dataset onboarding
argument-hint: <dataset_slug>
---

Open a pull request on `basedosdados/pipelines` for a dataset onboarding.

**Dataset:** $ARGUMENTS

## Step 1 — Confirm all files are committed

Run `git status` and `git diff`. If there are uncommitted changes, list them and ask the user whether to commit them first.

Ensure data files are excluded (check `.gitignore` covers the output parquet path).

## Step 2 — Draft changelog

Draft a changelog that includes:
- Dataset slug and full name
- Tables added or updated (list each)
- Coverage years
- Data source and organization
- Any known limitations or open issues

Present the draft to the user. **Do not open the PR until the user approves the changelog.**

## Step 3 — Open the PR

Once approved, open the PR:
```bash
gh pr create \
  --title "[$dataset_slug] <table names>" \
  --body "<changelog>" \
  --label "test-dev,table-approve,metadata-test"
```

PR body format:
```
## Dataset
**Slug:** <slug>
**Tables:** <list>
**Coverage:** <years>
**Source:** <org / URL>

## Changes
<changelog bullet points>

## Checklist
- [ ] DBT models run successfully in dev
- [ ] DBT tests pass
- [ ] Metadata registered in dev backend
- [ ] Verify at: https://development.basedosdados.org/dataset/<slug>

🤖 Generated with [Claude Code](https://claude.ai/claude-code)
```

## Step 4 — Return PR URL

Return the PR URL to the user.
