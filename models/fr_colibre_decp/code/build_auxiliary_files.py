"""Build the per-table auxiliary-file bundles for fr_colibre_decp.

Usage (from the repo root):
    python -m models.fr_colibre_decp.code.build_auxiliary_files

Writes <DATA_DIR>/auxiliary_files/<table>/auxiliary_files.zip, each holding a
README, the publisher's field schema (schema.json) and its per-source statistics
(statistiques-sources.csv). Publish each bundle to
gs://basedosdados-public/auxiliary_files/fr_colibre_decp/<table>/auxiliary_files.zip;
the dev service account cannot write that bucket.

The same documents serve all three tables: the schema describes the source file
every table is cut from.
"""

import zipfile
from datetime import date

import requests

from pipelines.datasets.fr_colibre_decp.constants import constants

DATASET_PAGE = (
    "https://www.data.gouv.fr/datasets/" + constants.DATAGOUV_DATASET.value
)
FILES = {
    "schema.json": "https://www.data.gouv.fr/api/1/datasets/r/9a4144c0-ee44-4dec-bee5-bbef38191d9a",
    "statistiques-sources.csv": "https://www.data.gouv.fr/api/1/datasets/r/8ded94de-3b80-4840-a5bb-7faad1c9c234",
}
GRAIN = {
    "marche": "one row per contract (`id_marche`), as at its initial version",
    "modification": "one row per contract and version (`id_marche`, `id_modification`)",
    "titulaire": (
        "one row per contract, version and awardee "
        "(`id_marche`, `id_modification`, `id_titulaire`)"
    ),
}

README = """# fr_colibre_decp.{table}: auxiliary files

Data Basis table `fr_colibre_decp.{table}`: {grain}.

## Source

Données essentielles de la commande publique (DECP) consolidées, format
tabulaire, published on data.gouv.fr by Colin Maudry (colibre.fr / decp.info)
under the Licence Ouverte 2.0: {page}

The publisher rebuilds the file daily from about 60 open-data sources listed in
`statistiques-sources.csv`. Processing code:
https://github.com/ColinMaudry/decp-processing

## Files

| File | What it is | Downloaded from | Downloaded |
|---|---|---|---|
| `schema.json` | Publisher's field-by-field schema of the consolidated file, in French | {schema_url} | {today} |
| `statistiques-sources.csv` | Contracts and buyers per source dataset, with the share of unique contracts | {stats_url} | {today} |

## Reference documents (not bundled)

- Arrêté du 22 décembre 2022 relatif aux données essentielles des marchés publics:
  https://www.legifrance.gouv.fr/loda/id/JORFTEXT000046850496
- Source quality and completeness notes: https://decp.info/a-propos

## How Data Basis built this table

- The consolidated file has one row per contract x amendment x awardee. Data
  Basis splits it into `marche`, `modification` and `titulaire`. Where two rows
  for the same key disagree (two source files describing one contract), the row
  published most recently is kept, then the first by source dataset and file.
- `ano` and `mes` are the year and month of the contract's initial notification,
  so every row of a contract sits in the same partition in all three tables.
  Contracts with no valid notification date, or notified before 2014, are dropped.
- Spelling and case variants of the same label (`MARCHE` / `Marché`,
  `Appel d offres ouvert` / `Appel d'offres ouvert`) are unified in `nature`,
  `procedure`, `ccag`, `type_code_lieu_execution` and
  `type_identifiant_titulaire`.
- `taux_avance`, `origine_ue` and `origine_france` are published as proportions
  and stored as percentages (x 100).
- `siren_acheteur` and `siren_titulaire` are the first 9 digits of the 14-digit
  SIRET. They link to `fr_insee_sirene.unite_legale`.
- Dropped source columns: `dureeRestanteMois` (recomputed by the publisher on
  every rebuild), `acheteur_population` (empty), and the commune, department,
  region and activity labels, which the `br_bd_diretorios_fr` directories hold.
"""


def main() -> None:
    root = constants.DATA_DIR.value / "auxiliary_files"
    contents = {}
    for name, url in FILES.items():
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        contents[name] = response.content
    today = date.today().isoformat()
    for table, grain in GRAIN.items():
        target = root / table
        target.mkdir(parents=True, exist_ok=True)
        readme = README.format(
            table=table,
            grain=grain,
            page=DATASET_PAGE,
            schema_url=FILES["schema.json"],
            stats_url=FILES["statistiques-sources.csv"],
            today=today,
        )
        path = target / "auxiliary_files.zip"
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as bundle:
            bundle.writestr("README.md", readme)
            for name, data in contents.items():
                bundle.writestr(name, data)
        print(f"wrote {path} ({path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
