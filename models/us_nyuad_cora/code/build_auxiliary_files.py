"""Build the auxiliary-file bundle for us_nyuad_cora.speech (README + nulled citations).

Writes $US_NYUAD_CORA_DATA_DIR/auxiliary_files/speech/auxiliary_files.zip. Publish to
gs://basedosdados-public/auxiliary_files/us_nyuad_cora/speech/auxiliary_files.zip.

CORA publishes no codebook or questionnaire: the data dictionary is the field list
on the figshare page, and the documentation is the accompanying paper and the
processing code, both linked rather than rehosted. The bundle therefore carries the
README and the list of citation fields this onboarding set to NULL, read from the
manifest written by clean_data.py.
"""

import csv
import io
import json
import os
import zipfile
from pathlib import Path

D = Path(
    os.environ.get(
        "US_NYUAD_CORA_DATA_DIR",
        Path.home() / "Library/Caches/us_nyuad_cora_data",
    )
)
MANIFEST = D / "output/_manifest_speech.json"
OUT = D / "auxiliary_files/speech/auxiliary_files.zip"
DOWNLOADED = "2026-10-08"

README = """# Congressional Oratory Research Archive (CORA): auxiliary files for `us_nyuad_cora.speech`

## Citation

Rabbani, Shahid; Kaufman, Aaron R. (2026). Congressional Oratory Research Archive
(CORA): U.S. Congressional Record Speeches, 1873-2025. figshare. Dataset.
https://doi.org/10.6084/m9.figshare.33321423.v1

License: CC0 1.0.

## Source

- Data: `Speeches_figshare.zip` (4,092,238,658 bytes), 154 files `speeches_YYYY.jsonl`
  for 1873-2026, from https://ndownloader.figshare.com/files/67793247 (figshare
  version 1, published 2026-08-24). Downloaded {downloaded}.
- 15,215,977 speeches, from 1873-03-04 to 2026-03-16. The figshare title says 2025,
  but the archive also holds 2026 speeches up to 2026-03-16.

## Files in this bundle

- `README.md`: this file.
- `nulled_citations.csv`: the {n_nulled} citation fields set to NULL in this table
  (`speech_id`, `column`, number of items in the source list). See below.

## Documentation (link only)

- Paper: Rabbani and Kaufman, "Congressional Oratory Research Archive, A Comprehensive
  Data Set and Platform for Exploring and Analyzing the U.S. Congressional Record,
  1873 to 2025", https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6898241
- Processing, topic-model and validation code: https://github.com/Shahid0201/CORA-code
- Interactive platform: https://cora.nyuad.nyu.edu/
- Comparative Agendas Project master codebook (topic categories):
  https://www.comparativeagendas.net/pages/master-codebook

## How this table differs from the source

1. Columns are renamed to Data Basis conventions (the architecture lists each
   `original_name`): `id` → `speech_id`, `speaking` → `speech_text`,
   `speaker_state` → `state_abbreviation`, `topic_extracted` → `cap_major_topic`,
   `origin_id` → `source_document_id`, `origin_url` → `source_url`, and so on.
2. `year` is derived from `date`. `state_id` (FIPS) is derived from
   `state_abbreviation`; it is NULL for `US` and `DK` (Dakota Territory), which have no
   FIPS code.
3. `party` decodes D/R/I to Democratic/Republican/Independent; minor-party values
   are kept as published. `gender` decodes M/F to Male/Female.
4. `bioguide_url` is dropped: it is `https://bioguide.congress.gov/search/bio/` followed
   by `bioguide_id`.
5. Empty strings are NULL. `bioguide_id` is filled for 47.5% of speeches: the source
   writes the string "None" for 7,665 unlinked speeches (set to NULL here) and
   "R000606R" for 12 speeches by Jamie Raskin (corrected to R000606).
6. **Citation lists with more than 1,000 items are NULL.** CORA's extractor expands
   every range such as "H.R. 10710 - 10715" into each number in between. When OCR
   misreads the end of a range, the expansion explodes: one 1973 speech lists
   55,134,284 bills (an 816 MB JSON line, beyond BigQuery's 100 MB row limit) and one
   2017 speech lists 379,301. Lists of up to 1,000 items are kept verbatim, so shorter
   spurious expansions (e.g. "H.R. 97 ... H.R. 889") can remain.

## Known limitations of the source

- Text up to 1993 is OCR of the bound Congressional Record and contains recognition
  errors, including in speaker names (e.g. `Mr. CONKLINO`). From 1994 text comes from
  GovInfo.
- `cap_major_topic` is assigned by a classifier trained by the authors. 57% of
  speeches fall in "State Government Operations", which absorbs procedural speech.
"""


def main():
    manifest = json.loads(MANIFEST.read_text())
    nulled = manifest["nulled_citations"]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["speech_id", "column", "items"])
    w.writeheader()
    w.writerows(sorted(nulled, key=lambda r: -r["items"]))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(
            "README.md",
            README.format(downloaded=DOWNLOADED, n_nulled=len(nulled)),
        )
        z.writestr("nulled_citations.csv", buf.getvalue())
    print(f"{OUT} ({OUT.stat().st_size:,} bytes, {len(nulled)} nulled fields)")


if __name__ == "__main__":
    main()
