"""Work order 05 — run the prod-anomaly probes (Q1-Q18) against DEV.

The diagnostics probes in ``diagnostics/prod_validation.sql`` target
``basedosdados.br_tse_eleicoes`` (prod). This runner retargets them to the
dev-materialized dataset and executes each with the dev staging service
account (the ADC user lacks ``bigquery.jobs.create`` in ``basedosdados-dev``;
the staging SA has it). Every previously-failing probe must now come back
clean — see DIAGNOSIS.md "Prod-data validation" for the expected post-fix
values.

Run from ``code/python``::

    uv run --with basedosdados python wo05_dev_probes.py \
        [--dataset basedosdados-dev.br_tse_eleicoes] [--only Q1,Q13]
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

SA_KEY = "/Users/rdahis/.basedosdados/credentials/staging.json"
BILLING = "basedosdados-dev"
DEFAULT_DATASET = "basedosdados-dev.br_tse_eleicoes"
SQL = Path(__file__).resolve().parent / "diagnostics" / "prod_validation.sql"


def _statements(dataset: str):
    """Yield (label, sql) for each SELECT in prod_validation.sql, retargeted."""
    text = SQL.read_text().replace("basedosdados.br_tse_eleicoes", dataset)
    label = "?"
    for chunk in text.split(";"):
        # capture the most recent [Qn] marker seen in the comments
        m = re.findall(r"\[(Q\d+)\]", chunk)
        if m:
            label = m[-1]
        # strip comment lines to test for an actual statement
        body = "\n".join(
            ln for ln in chunk.splitlines() if not ln.strip().startswith("--")
        ).strip()
        if body.lower().startswith("select"):
            yield label, chunk.strip()


def main() -> None:
    dataset = DEFAULT_DATASET
    only = None
    if "--dataset" in sys.argv:
        dataset = sys.argv[sys.argv.index("--dataset") + 1]
    if "--only" in sys.argv:
        only = set(sys.argv[sys.argv.index("--only") + 1].split(","))

    from google.cloud import bigquery
    from google.oauth2 import service_account

    creds = service_account.Credentials.from_service_account_file(
        SA_KEY, scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    client = bigquery.Client(project=BILLING, credentials=creds)

    print(f"=== DEV PROBES vs {dataset} (billing {BILLING}) ===\n", flush=True)
    for label, sql in _statements(dataset):
        if only and label not in only:
            continue
        print(f"----- {label} -----", flush=True)
        try:
            rows = list(client.query(sql).result())
        except Exception as e:
            print(
                f"  QUERY ERROR: {type(e).__name__}: {str(e)[:200]}\n",
                flush=True,
            )
            continue
        if not rows:
            print("  (no rows)\n", flush=True)
            continue
        cols = rows[0].keys()
        print("  " + " | ".join(cols), flush=True)
        for r in rows:
            print("  " + " | ".join(str(r[c]) for c in cols), flush=True)
        print("", flush=True)


if __name__ == "__main__":
    main()
