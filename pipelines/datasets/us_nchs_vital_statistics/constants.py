"""Constants for the us_nchs_vital_statistics pipeline."""

from enum import Enum
from pathlib import Path


class constants(Enum):
    """NCHS Vital Statistics public-use microdata."""

    DATASET_ID = "us_nchs_vital_statistics"

    # birth = natality PUF; death = mortality multiple-cause PUF
    TABLES = ["birth", "death", "dicionario"]
    PARTITIONED_TABLES = ["birth", "death"]

    # NCHS publishes the annual final files here. ftp.cdc.gov is authoritative but
    # very slow (~0.04 MB/s); NBER mirrors the identical raw zips far faster and is
    # used first, with CDC as the fallback.
    CDC_BASE = "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS"
    NBER_BASE = "https://data.nber.org/nvss"

    CDC_DIR = {"birth": "natality", "death": "mortality"}
    NBER_DIR = {"birth": "natality", "death": "mortality"}

    FIRST_YEAR = 1968
    # 1969 natality has no published record layout (CDC or NBER) -> excluded.
    BIRTH_YEARS_WITHOUT_LAYOUT = [1969]

    # Natality years that are a 50 percent sample but publish no `recwt`.
    # Verified: the file holds exactly 0.500 of published births in 1968 and 1970.
    BIRTH_SAMPLE_YEARS = (1968, 1971)

    HEADERS = {"User-Agent": "Mozilla/5.0"}

    # Layout + crosswalk tables live with the model code and are the single source
    # of truth for both the one-shot onboarding and this pipeline.
    CODE_DIR = (
        Path(__file__).resolve().parents[3] / "models" / DATASET_ID / "code"
    )
    LAYOUTS_CSV = CODE_DIR / "layouts" / "nchs_layouts.csv"
    ARCHITECTURE_DIR = CODE_DIR / "architecture"

    # ICD revision in force for the underlying-cause code, by death year.
    ICD_REVISIONS = [(1968, 1978, "8"), (1979, 1998, "9"), (1999, 9999, "10")]
