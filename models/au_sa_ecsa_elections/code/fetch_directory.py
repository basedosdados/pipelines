"""Export the South Australian rows of the ASGS 2021 state electoral division directory.

One vintage is pinned for every election rather than matching each election to its
contemporaneous vintage. The three ASGS vintages re-use division ids for different
divisions — 18 ids name a different district in 2016 than in 2021 — so a
vintage-per-election join would let a group-by silently merge two unrelated
districts.

The crosswalk is written to the scratch directory, not the repository: it is
derived data, and the cleaning transform reads it to attach
``state_electoral_division_id`` to each district.
"""

from __future__ import annotations

from google.cloud import bigquery

from pipelines.datasets.au_sa_ecsa_elections.constants import data_dir

QUERY = """
select id_state_electoral_division, name
from `basedosdados.br_bd_diretorios_au.state_electoral_division_2021`
where upper(abbreviation_state) = 'SA'
order by name
"""


def main() -> int:
    client = bigquery.Client(project="basedosdados-dev")
    frame = client.query(QUERY).result().to_dataframe()
    target = data_dir() / "state_electoral_division_2021.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(target, index=False)
    print(f"wrote {len(frame)} SA divisions to {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
