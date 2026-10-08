"""Build the per-table auxiliary-file bundles for us_ucla_voteview.

Usage:
    uv run --no-sync python models/us_ucla_voteview/code/build_auxiliary_files.py

Fetches Voteview's codebook page for each table, converts it to text and zips it
with a README into
``$US_UCLA_VOTEVIEW_DATA_DIR/auxiliary_files/<table>/auxiliary_files.zip``.
Publish to
``gs://basedosdados-public/auxiliary_files/us_ucla_voteview/<table>/auxiliary_files.zip``.
"""

import datetime
import html
import os
import re
import zipfile
from pathlib import Path

import requests

D = Path(
    os.environ.get(
        "US_UCLA_VOTEVIEW_DATA_DIR",
        Path.home() / "Library" / "Caches" / "us_ucla_voteview_data",
    )
)
TODAY = datetime.date.today().isoformat()
CITATION = (
    "Lewis, Jeffrey B., Keith Poole, Howard Rosenthal, Adam Boche, Aaron Rudkin, "
    "and Luke Sonnet (2026). Voteview: Congressional Roll-Call Votes Database. "
    "https://voteview.com/"
)
TABLES = {
    "members": "Member Ideology",
    "parties": "Congressional Parties",
    "rollcalls": "Congressional Votes",
    "votes": "Members' Votes",
}
LINKS = [
    (
        "Boche, Lewis, Rudkin and Sonnet (2018), The new Voteview.com: preserving "
        "and continuing Keith Poole's infrastructure for scholars, students and "
        "observers of Congress. Public Choice 176(1-2)",
        "https://link.springer.com/article/10.1007/s11127-018-0546-0",
    ),
    ("Voteview data downloads", "https://voteview.com/data"),
    ("Voteview articles (methodology)", "https://voteview.com/articles"),
]
NOTES = {
    "members": [
        "`party_code` values printed as `100.0` in the source are stored as `100`.",
        "`last_means` takes the values 0, 6 and 7 in the data, which the codebook "
        "does not document; they are kept and labelled as undocumented.",
        "`state_abbrev` links to `br_bd_diretorios_us.state`. Presidents carry "
        "`USA` and `state_icpsr` 99; territories carry ICPSR codes 92-96.",
    ],
    "rollcalls": [
        "`session` values printed as `1.0` in the source are stored as `1`.",
        "The JSON-only fields of the Voteview export (issue codes, CRS policy "
        "area, source documents) are not included.",
    ],
    "votes": [
        "`date` is not in the Voteview votes file: Data Basis adds the roll "
        "call's date from the rollcalls table, joined on congress, chamber and "
        "rollnumber. Every vote matches a roll call.",
        "The 115th Congress file lists 152 of the president's (icpsr 99912) "
        "votes twice, identically. The copies are dropped.",
    ],
    "parties": [],
}


def page_text(url: str) -> str:
    raw = requests.get(url, timeout=60).text
    raw = re.sub(r"(?s)<script.*?</script>|<style.*?</style>", "", raw)
    raw = re.sub(r"<(br|p|tr|li|h[1-6])[^>]*>", "\n", raw)
    raw = re.sub(r"<td[^>]*>", " | ", raw)
    text = html.unescape(re.sub(r"<[^>]+>", "", raw))
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text).strip()
    start = text.rfind("Data - ")
    return text[start:] if start >= 0 else text


def readme(table: str, title: str, codebook_url: str) -> str:
    notes = "\n".join(f"- {n}" for n in NOTES[table]) or "- None."
    links = "\n".join(f"- {t}: {u}" for t, u in LINKS)
    return f"""# us_ucla_voteview.{table}: auxiliary files

Voteview, {title}. Data Basis table `basedosdados.us_ucla_voteview.{table}`.

## Citation

{CITATION}

## Files

| File | What it is | Source | Downloaded |
|---|---|---|---|
| `codebook.txt` | Voteview's field documentation for this table, converted to text | {codebook_url} | {TODAY} |

## Data source

https://voteview.com/static/data/out/{table}/HSall_{table}.csv (all congresses,
all chambers). Voteview states no data license; it asks users to cite the
database as above.

## Transformations applied by Data Basis

{notes}

## Further reading (not bundled)

{links}
"""


def main():
    for table, title in TABLES.items():
        url = f"https://voteview.com/articles/data_help_{table}"
        out = D / "auxiliary_files" / table
        out.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(
            out / "auxiliary_files.zip", "w", zipfile.ZIP_DEFLATED
        ) as z:
            z.writestr("README.md", readme(table, title, url))
            z.writestr("codebook.txt", page_text(url))
        print(f"{table}: {out / 'auxiliary_files.zip'}")


if __name__ == "__main__":
    main()
