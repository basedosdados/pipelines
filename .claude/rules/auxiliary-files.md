# Auxiliary Files

Reference for every agent that onboards a dataset whose source publishes
documentation alongside the data — codebooks, questionnaires, technical reports,
import scripts, crosswalks, compendia.

Some datasets are unusable without their documentation. A PIAAC Public Use File is
2,483 unlabelled columns named `A2_Q03c1`; a codebook is not a nice-to-have. This
rule says where each kind of document goes, so it stops being decided case by case.

## The three destinations

Every document lands in exactly one of three places. Pick by asking **what the
document is**, not how big or how official it looks.

| Destination | For | Backend field |
|---|---|---|
| **Raw data source** | A place data or its schema is *published from* | `RawDataSource` record |
| **Auxiliary file** | A document a user of *this table* needs in hand to use it | `Table.auxiliaryFilesUrl` |
| **Link only** | Reference material a user reads occasionally, or that is large and stable | A line in the bundle README |

### Raw data source

One record per distinct **source**, not per file. The dataset's landing page, the
bulk-download endpoint, the API. Keep it to a handful: a reader scanning the
dataset page should see where the data came from, not a bibliography.

**Link at most one raw data source per table.** `client._raw_source_id` resolves a
table's source through `_query_id`, which raises when a table has two or more
(`allRawdatasource: mais de um nó encontrado`). Both the poll and commit tasks go
through it, so a table with two raw sources cannot run a recurring pipeline at all.
Record the others at dataset level and note it. → `prefect-pipeline-conventions`

### Auxiliary file

Bundle per **table**, not per dataset, and include only what a user of that table
needs. A codebook covering the respondent table does not belong in the dictionary
table's bundle.

Typical contents: the codebook or data dictionary, derived-variable documentation,
the questionnaire or collection instrument, official import scripts, crosswalks,
lists of suppressed or missing variables, and validation compendia.

### Link only

Long-form PDFs — technical reports, methodology manuals, assessment frameworks,
readers' companions. These are stable at the publisher, often tens of megabytes,
and rarely opened. Rehosting them bloats every bundle and creates a second copy
that silently goes stale. List them in the README with title and URL.

Rule of thumb: if it is over ~5 MB and a user would read it once, link it.

## Where auxiliary files are stored

```text
gs://basedosdados-public/auxiliary_files/<gcp_dataset_id>/<table_slug>/auxiliary_files.zip
```

recorded as the matching public URL:

```text
https://storage.googleapis.com/basedosdados-public/auxiliary_files/<gcp_dataset_id>/<table_slug>/auxiliary_files.zip
```

**Use `basedosdados-public`, not `basedosdados` or `basedosdados-dev`.** The two
data-lake buckets are requester-pays, which makes every link served from them
return HTTP 400 to an anonymous visitor:

```xml
<Error><Code>UserProjectMissing</Code>
<Message>Bucket is a requester pays bucket but no user project provided.</Message></Error>
```

Requester-pays is a bucket-level billing setting; it cannot be scoped to a
prefix, and the objects being world-readable does not help — `allUsers` already
holds `roles/storage.objectViewer` on `basedosdados-dev` and the links are dead
regardless. Turning it off on a data-lake bucket is not an option either: it
would make hundreds of terabytes of egress anonymously billable.

`basedosdados-public` is not requester-pays and is already how the public reaches
Data Basis data — it serves the one-click table downloads under
`one-click-download/<gcp_dataset_id>/<table_slug>/` that `pipelines/utils/tasks.py`
exports to and the website streams. Auxiliary bundles sit beside them under
`auxiliary_files/`.

### Always verify the link anonymously

The bucket choice is the whole fix, so confirm it rather than assuming:

```bash
curl -sI "<auxiliaryFilesUrl>" | head -1
```

Never state that auxiliary files are "available at" a URL you have not fetched
without credentials. A 400 means you used a requester-pays bucket.

`.github/scripts/migrate_auxiliary_files.py verify --env prod` runs this check
across every registered `auxiliaryFilesUrl` at once.

## Every bundle carries a README

A ZIP of loose PDFs with publisher-assigned filenames is barely better than
nothing. Include `README.md` at the top of every bundle:

- the citation the publisher asks for
- for each bundled file: what it is, and the URL it came from
- the date each file was downloaded, and any version or revision notice
- an index of the link-only documents, with titles and URLs
- anything a user must know to read the table: derived columns, transformations
  applied at load, codes converted to NULL

Rename files to something self-describing (`international_codebook.xlsx`, not
`piaac-cy2-int-cb-v3-final.xlsx`) and record the original URL in the README.

## Registering the URL

`Table.auxiliaryFilesUrl` exists on the backend and is exposed by
`create_update_table` in the databasis MCP as `auxiliary_files_url`. Pass it in the
same call that creates or updates the table; as with every other field, the API
does no partial updates, so re-pass the required fields when updating.

If the MCP in use predates that argument, set the field through Django admin and
say that you did — do not silently skip it.

## Sources that block scripted downloads

Some publishers serve documents only to a real browser. `www.oecd.org` returns 403
for every `.zip` to `curl`, `urllib` and `requests` alike, with or without browser
headers, cookies, or the CDN rendition path — while serving the same URL happily to
a browser session.

When that happens, fetch through the browser tools rather than assuming the
document is unavailable, and record the constraint in the dataset's memory file so
the next person does not rediscover it. Never quietly drop a document from a bundle
because it was awkward to fetch — either get it, or list it under link-only and say
why.

## Checklist

- [ ] Each document is a raw data source, an auxiliary file, or link-only — never two
- [ ] At most one raw data source linked per table
- [ ] Bundles are per table and contain only that table's documents
- [ ] Every bundle has a README with citation, per-file provenance and download dates
- [ ] Uploaded to `gs://basedosdados-public` under `auxiliary_files/<gcp_dataset_id>/<table_slug>/`
- [ ] `auxiliary_files_url` set on every table that has a bundle
- [ ] Each published URL fetched anonymously and its real status reported
