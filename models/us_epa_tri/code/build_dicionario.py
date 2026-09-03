"""Write code/dicionario.csv from the category spec in the shared transform.

The dicionario table is generated at clean time by ``utils.build_dicionario``
from the same spec, so the CSV here is documentation of what ships, not a
second source of truth. It carries the EN/ES labels too, for the metadata.

    PYTHONPATH=. uv run python models/us_epa_tri/code/build_dicionario.py
"""

import csv
from pathlib import Path

from pipelines.datasets.us_epa_tri.utils import dicionario_rows

HERE = Path(__file__).parent


def main():
    rows = dicionario_rows()
    cols = [
        "id_tabela",
        "nome_coluna",
        "chave",
        "cobertura_temporal",
        "valor",
        "valor_en",
        "valor_es",
    ]
    with open(
        HERE / "dicionario.csv", "w", newline="", encoding="utf-8"
    ) as fh:
        w = csv.DictWriter(fh, fieldnames=cols, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    print(f"dicionario.csv: {len(rows)} rows")


if __name__ == "__main__":
    main()
