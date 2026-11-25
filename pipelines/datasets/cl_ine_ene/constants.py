"""Constants for cl_ine_ene (INE Chile, Encuesta Nacional de Empleo)."""

from enum import Enum


class constants(Enum):
    DATASET_ID = "cl_ine_ene"

    #: The fact table. `dicionario` is a literal dbt model with no staging table,
    #: so it is built but never uploaded.
    TABLE_ID = "microdato"
    UPLOAD_TABLES = ["microdato"]
    DBT_TABLES = ["microdato", "dicionario"]

    #: First published moving quarter (enero-marzo 2010, centred on February).
    FIRST_PERIOD = (2010, 2)

    #: A period whose contents are re-read on every run to detect a recalibration.
    #: INE rebuilt the entire 2010-onwards series when it recalibrated expansion
    #: factors on the Censo 2017 projections, and the Censo 2024 will prompt
    #: another. A rewrite of the back-series is invisible to a poll that only
    #: looks at the newest quarter, so the oldest one is the canary.
    ANCHOR_PERIOD = (2010, 2)
