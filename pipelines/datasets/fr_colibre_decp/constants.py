"""Constants for the fr_colibre_decp dataset and its recurring pipeline."""

import os
from enum import Enum
from pathlib import Path


class constants(Enum):
    DATASET_ID = "fr_colibre_decp"

    # data.gouv.fr dataset "Données essentielles de la commande publique consolidées
    # (format tabulaire)", published by Colin Maudry (colibre.fr / decp.info).
    DATAGOUV_DATASET = "donnees-essentielles-de-la-commande-publique-consolidees-format-tabulaire"
    # decp.parquet. The resource id is stable across the daily rebuilds; the
    # static.data.gouv.fr URL behind it changes every day.
    RESOURCE_ID = "11cea8e8-df3e-4ed1-932b-781e2635e432"
    RESOURCE_URL = (
        "https://www.data.gouv.fr/api/1/datasets/r/"
        "11cea8e8-df3e-4ed1-932b-781e2635e432"
    )
    RESOURCE_API = (
        "https://www.data.gouv.fr/api/1/datasets/"
        "donnees-essentielles-de-la-commande-publique-consolidees-format-tabulaire"
        "/resources/11cea8e8-df3e-4ed1-932b-781e2635e432/"
    )

    # Contracts whose initial notification falls before this year are dropped. The
    # source has 104 rows for 2014 and stray dates back to year 1.
    FIRST_YEAR = 2014

    TABLES = ["marche", "modification", "titulaire"]

    # Scratch data never goes under the repo or Dropbox. ~/Library/Caches is local
    # and unsynced on the onboarding machine; the worker passes its own directory.
    DATA_DIR = Path(
        os.environ.get(
            "FR_COLIBRE_DECP_DATA_DIR",
            str(Path.home() / "Library" / "Caches" / "fr_colibre_decp_data"),
        )
    )
