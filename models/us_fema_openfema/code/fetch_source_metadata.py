"""Cache OpenFEMA's own catalog and field dictionary.

Every OpenFEMA set publishes a machine-readable field dictionary, so the
architecture tables are generated from the source rather than hand-written.
This script pulls the two catalog endpoints once and writes them to
``source_metadata.json``, which is committed so that a rebuild is reproducible
and the PR diff shows exactly what FEMA said.

Re-run when FEMA changes a schema:  uv run python fetch_source_metadata.py
"""

from __future__ import annotations

import json
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
OUT = HERE / "source_metadata.json"

DATASETS_URL = "https://www.fema.gov/api/open/v1/DataSets?$top=1000"
FIELDS_URL = "https://www.fema.gov/api/open/v1/DataSetFields?$top=2000"

# The sets this dataset onboards, pinned to the version we build against.
# NOTE: FimaNfipClaims/FimaNfipPolicies v2 are DEPRECATED (removed 2026-10-15,
# frozen 2026-06-01). Their live successors dropped the "Fima" prefix.
WANTED = {
    ("DisasterDeclarationsSummaries", 2),
    ("PublicAssistanceFundedProjectsDetails", 2),
    ("NfipClaims", 3),
    ("NfipPolicies", 3),
}

TIMEOUT = 180


def _get(url: str) -> dict:
    response = requests.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def main() -> None:
    datasets = _get(DATASETS_URL)["DataSets"]
    fields = _get(FIELDS_URL)["DataSetFields"]

    wanted_names = {name for name, _ in WANTED}
    kept_sets = [
        d
        for d in datasets
        if d["name"] in wanted_names and (d["name"], d["version"]) in WANTED
    ]
    kept_fields = [
        f
        for f in fields
        if (f["openFemaDataSet"], f["datasetVersion"]) in WANTED
    ]

    missing = WANTED - {(d["name"], d["version"]) for d in kept_sets}
    if missing:
        raise SystemExit(f"catalog is missing pinned sets: {sorted(missing)}")

    # Drop the volatile bookkeeping so the committed file only churns when the
    # schema or the publication facts actually change.
    for d in kept_sets:
        d.pop("hash", None)
    for f in kept_fields:
        f.pop("hash", None)
        f.pop("lastRefresh", None)

    payload = {
        "datasets": sorted(kept_sets, key=lambda d: d["name"]),
        "fields": sorted(
            kept_fields, key=lambda f: (f["openFemaDataSet"], f["sortOrder"])
        ),
    }
    OUT.write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n")

    for d in payload["datasets"]:
        n = sum(
            1 for f in payload["fields"] if f["openFemaDataSet"] == d["name"]
        )
        deprecated = f"  DEPRECATED {d['depDate']}" if d.get("depDate") else ""
        print(
            f"{d['name']:<40} v{d['version']}  {n:>3} fields  "
            f"{d['recordCount']:>10,} rows{deprecated}"
        )


if __name__ == "__main__":
    main()
