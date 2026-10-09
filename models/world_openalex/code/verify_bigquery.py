"""Check the built world_openalex dev tables and write coverage.json.

    uv run python -m models.world_openalex.code.verify_bigquery

1. Row counts of every built table, read from table metadata (free), against
   the rows the loader staged (its state file). A mismatch means a model
   dropped or duplicated rows.
2. Min and max year of ``work.publication_year`` and of the two author-by-year
   tables, written to ``coverage.json`` for ``register_metadata.py``. Every
   works table takes the ``work`` range: their rows exist only for works.

Requires GOOGLE_APPLICATION_CREDENTIALS for the basedosdados-dev service account.
"""

import json
import os
from pathlib import Path

from google.cloud import bigquery

from models.world_openalex.code.tables import TABLES
from pipelines.datasets.world_openalex import loader, utils

PROJECT = "basedosdados-dev"
DS = "world_openalex"
HERE = Path(__file__).resolve().parent


def staged_rows() -> dict[str, int]:
    """Rows per table the loader wrote, from its GCS completion markers."""
    release = utils.release_date(utils.fetch_manifest())
    # The load may have resumed under an earlier fingerprint, so take the one
    # marker prefix of this release rather than the current code's.
    root = f"staging/{loader.DATASET_ID}/_load_state/"
    it = loader._bucket(PROJECT).list_blobs(prefix=root, delimiter="/")
    list(it)
    prefixes = [p for p in it.prefixes if p.startswith(f"{root}{release}_")]
    if len(prefixes) != 1:
        raise SystemExit(
            f"expected one marker prefix for {release}, found {prefixes}"
        )
    prefix = prefixes[0]
    rows: dict[str, int] = {}
    for rec in loader.read_markers(PROJECT, prefix):
        for t, n in rec["rows"].items():
            rows[t] = rows.get(t, 0) + n
    if not rows:
        raise SystemExit(f"no load markers under gs://{PROJECT}/{prefix}")
    return rows


def main() -> None:
    """Compare counts, then write coverage.json."""
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        raise SystemExit("GOOGLE_APPLICATION_CREDENTIALS is not set")
    client = bigquery.Client(project=PROJECT)
    built = {t.table_id: t for t in client.list_tables(f"{PROJECT}.{DS}")}
    staged = staged_rows()
    bad = []
    for table in TABLES:
        if table not in built:
            bad.append(f"{table}: not built")
            continue
        n = client.get_table(f"{PROJECT}.{DS}.{table}").num_rows
        want = staged.get(table)
        flag = "" if want is None or n == want else "  <-- MISMATCH"
        if flag:
            bad.append(f"{table}: {n:,} built vs {want:,} staged")
        print(
            f"{table:32} {n:>15,}  staged {want if want is not None else '-':>15}{flag}"
        )

    years = {}
    for table, col in [
        ("work", "publication_year"),
        ("author_affiliation", "year"),
        ("author_counts_by_year", "year"),
    ]:
        r = next(
            iter(
                client.query(
                    f"select min({col}) lo, max({col}) hi from `{PROJECT}.{DS}.{table}`"
                ).result()
            )
        )
        years[table] = [r.lo, r.hi]
        print(f"{table}.{col}: {r.lo}..{r.hi}")
    coverage = {t: years["work"] for t, s in TABLES.items() if s["scoped"]}
    coverage["author_affiliation"] = years["author_affiliation"]
    coverage["author_counts_by_year"] = years["author_counts_by_year"]
    (HERE / "coverage.json").write_text(json.dumps(coverage, indent=1) + "\n")
    print(f"coverage.json: {len(coverage)} tables")
    if bad:
        raise SystemExit("FAILED:\n  " + "\n  ".join(bad))


if __name__ == "__main__":
    main()
