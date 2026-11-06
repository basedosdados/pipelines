"""Shared constants for the ar_indec_eph onboarding and pipeline code."""

import json
import os
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
ARCH_DIR = CODE_DIR / "architecture"

# Scratch data location. Never inside the repo and never inside Dropbox --
# on this machine ~/Downloads is a symlink into Dropbox, so it is not usable.
DATA_DIR = Path(
    os.environ.get(
        "AR_INDEC_EPH_DATA_DIR", "/Users/rdah0003/bd_scratch/ar_indec_eph_data"
    )
)
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

BASE_URL = "https://www.indec.gob.ar/ftp/cuadros/menusuperior/eph/"

# INDEC returns HTTP 200 with a ~37 KB HTML error page for a missing file, so a
# download must be validated on content type / size, never on the status code.
HTML_ERROR_SIZE = 37465

TABLES = ("microdatos_individuo", "microdatos_hogar")

# Waves INDEC never published, and why. Kept explicit so the gaps are
# documented rather than looking like a harvesting failure.
KNOWN_GAPS = {
    (2007, 3): (
        "No relevado: los aglomerados Mar del Plata-Batan, Bahia Blanca-Cerri y "
        "Gran La Plata no fueron relevados por causas administrativas, y Gran "
        "Buenos Aires no fue relevado por un paro del personal de la EPH."
    ),
    (
        2015,
        3,
    ): "No publicado por INDEC en el marco de la emergencia estadistica.",
    (
        2015,
        4,
    ): "No publicado por INDEC en el marco de la emergencia estadistica.",
    (
        2016,
        1,
    ): "No publicado por INDEC en el marco de la emergencia estadistica.",
}

# Series published between 2007 and 2015 carry an official INDEC reservation.
RESERVATION_YEARS = range(2007, 2016)


def waves() -> list[dict]:
    """The 87 published waves, each with year, quarter, source format and URL."""
    with open(CODE_DIR / "waves.json", encoding="utf-8") as handle:
        return json.load(handle)
