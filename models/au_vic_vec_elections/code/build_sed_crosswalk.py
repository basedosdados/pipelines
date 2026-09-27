"""Build the district name -> ASGS state electoral division crosswalk.

The Australian Statistical Geography Standard vintage labelled 2021 actually carries
the boundaries of Victoria's **2013** redivision: it still lists Keysborough and
Buninyong, both abolished in 2021, and has no Ashwood, Berwick, Eureka, Glen Waverley,
Greenvale, Kalkallo, Laverton, Pakenham or Point Cook, all created in 2021. So it
matches the 2014 and 2018 districts exactly and only partially matches 2022, 2010 and
2006. Districts with no counterpart are left unlinked rather than forced onto a
same-named division with different boundaries.

Division ids are also re-used across vintages with different meanings — of the 62
division names common to the 2011 and 2021 vintages, only 5 keep the same id — so the
crosswalk is keyed on (vintage, name), never on the id.

Run: PYTHONPATH=. ~/.venvs/bd-pipelines-vic/bin/python \
         models/au_vic_vec_elections/code/build_sed_crosswalk.py
"""

from __future__ import annotations

import json
import os
import re

from pipelines.datasets.au_vic_vec_elections.constants import (
    constants,
    data_root,
)

CREDENTIALS = os.path.expanduser("~/.basedosdados/credentials/staging.json")


def normalise(name: str) -> str:
    """Fold a district name to a comparison key.

    The directory prints the Legislative Council region in parentheses
    (``Albert Park (Southern Metropolitan)``); the VEC never does.
    """
    name = re.sub(r"\s*\(.*\)\s*$", "", name)
    name = name.replace("-", " ").replace("'", "")
    name = re.sub(r"[^a-z0-9 ]", "", name.lower())
    return re.sub(r"\s+", " ", name).strip()


def fetch_vintage(vintage: str) -> dict[str, str]:
    # Imported lazily so the module can be read without google-cloud installed.
    from google.cloud import bigquery
    from google.oauth2 import service_account

    # A stale GOOGLE_APPLICATION_CREDENTIALS would silently win over the file below.
    os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
    creds = service_account.Credentials.from_service_account_file(CREDENTIALS)
    client = bigquery.Client(credentials=creds, project="basedosdados-dev")
    query = f"""
        select id_state_electoral_division as id, name
        from `basedosdados.br_bd_diretorios_au.state_electoral_division_{vintage}`
        where upper(abbreviation_state) = 'VIC'
    """
    out: dict[str, str] = {}
    for row in client.query(query).result():
        key = normalise(row.name)
        # 'Migratory - Offshore - Shipping (Vic.)' and 'No usual address (Vic.)' are
        # ABS pseudo-areas, not districts; they never match a VEC contest name.
        out[key] = row.id
    return out


def main() -> None:
    vintage = constants.SED_VINTAGE.value
    mapping = fetch_vintage(vintage)
    path = data_root() / "inventory" / f"sed_crosswalk_{vintage}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(
            mapping, handle, ensure_ascii=False, indent=1, sort_keys=True
        )
    print(f"vintage {vintage}: {len(mapping)} Victorian divisions -> {path}")


if __name__ == "__main__":
    main()
