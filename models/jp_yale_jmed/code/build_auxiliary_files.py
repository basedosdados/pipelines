"""Build the auxiliary-file bundles for jp_yale_jmed.candidate and municipal_history.

Writes $JP_YALE_JMED_DATA_DIR/auxiliary_files/<table>/auxiliary_files.zip. Publish to
gs://basedosdados-public/auxiliary_files/jp_yale_jmed/<table>/auxiliary_files.zip.

Each bundle carries a README, the JMED codebook (which documents both tables; the
municipal history is its Appendix A) and the release README, both downloaded from
Harvard Dataverse into input/ alongside the CSVs.
"""

import os
import zipfile
from pathlib import Path

D = Path(
    os.environ.get(
        "JP_YALE_JMED_DATA_DIR",
        Path.home() / "Library/Caches/jp_yale_jmed_data",
    )
)
DOWNLOADED = "2026-10-27"
DATAVERSE = "https://dataverse.harvard.edu/api/access/datafile/{}"
# Bundled name -> (input file, Dataverse file id, description)
FILES = {
    "jmed_codebook_v1.0.pdf": (
        "jmed_codebook_v1.0.pdf",
        14367638,
        "JMED codebook, version 1.0: variable definitions, party labels, data "
        "availability, sources, citation, and the Municipal History Dataset "
        "(Appendix A) with worked examples of each change type",
    ),
    "release_readme.txt": (
        "README.txt",
        14367633,
        "Release README from Harvard Dataverse: file list, citation and license",
    ),
}

TABLE_NOTES = {
    "candidate": """\
- `year` is the source's `election_year`, renamed as the partition column.
- `prefecture_code` is zero-padded to two characters (`01` to `47`); the source
  stores it as an integer.
- `unified`, `uncontested`, `incumbent`, `elected` (0/1) and `sex` (M/F) are stored
  as codes; their labels are in the `dicionario` table.
- The source's `NA` is stored as NULL. No row was dropped: 223,561 rows, as in the
  codebook.
""",
    "municipal_history": """\
- Columns are as published; the source's `NA` is stored as NULL. 2,053 rows, as in
  the codebook.
- `predecessor_id` and `successor_id` match `municipality_id` in the `candidate`
  table.
""",
}

README = """# Japanese Municipal Elections Dataset (JMED): auxiliary files for `jp_yale_jmed.{table}`

## Citation

Cite both the dataset and the article that introduces it:

- McClean, Charles T. 2026. *Japanese Municipal Elections Dataset (JMED)*.
  Version 1.0. https://doi.org/10.7910/DVN/E9ZUNV
- McClean, Charles T. 2026. "Does Youth Representation Matter for Social
  Spending?" *The Journal of Politics*. https://doi.org/10.1086/741605

License: Creative Commons Attribution 4.0 International (CC BY 4.0).

## Source

Harvard Dataverse, doi:10.7910/DVN/E9ZUNV, version 1.0 (released 2026-10-23).
Project page and interactive map: https://www.charlesmcclean.com/jmed

## Files in this bundle

All downloaded on {downloaded}.

{files}

## Notes on the Data Basis table

{notes}"""


def main():
    for table, notes in TABLE_NOTES.items():
        listing = "\n".join(
            f"- `{name}`: {desc}. Source: {DATAVERSE.format(fid)}"
            for name, (_, fid, desc) in FILES.items()
        )
        readme = README.format(
            table=table, downloaded=DOWNLOADED, files=listing, notes=notes
        )
        out = D / "auxiliary_files" / table / "auxiliary_files.zip"
        out.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("README.md", readme)
            for name, (src, _, _) in FILES.items():
                z.write(D / "input" / src, name)
        print(f"{out} ({out.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
