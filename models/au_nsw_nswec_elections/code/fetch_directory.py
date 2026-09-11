"""Export the NSW rows of the ASGS 2021 state electoral division directory.

The crosswalk is written to the scratch directory rather than the repository: it is
derived data, and the cleaning transform reads it to attach
``state_electoral_division_id`` to each district.
"""

from __future__ import annotations

import os
import pathlib

from google.cloud import bigquery

DATA_DIR = pathlib.Path(
    os.environ.get(
        "NSWEC_DATA_DIR",
        str(pathlib.Path.home() / "Downloads" / "au_nsw_nswec_elections_data"),
    )
)

QUERY = """
select id_state_electoral_division, name
from `basedosdados.br_bd_diretorios_au.state_electoral_division_2021`
where upper(abbreviation_state) = 'NSW'
order by name
"""


def main() -> int:
    client = bigquery.Client(project="basedosdados-dev")
    frame = client.query(QUERY).result().to_dataframe()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    target = DATA_DIR / "state_electoral_division_2021.csv"
    frame.to_csv(target, index=False)
    print(f"wrote {len(frame)} NSW divisions to {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
