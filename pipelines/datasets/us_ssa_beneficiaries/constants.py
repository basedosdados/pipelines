"""Constants for the us_ssa_beneficiaries pipeline."""

from enum import Enum
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for SSA OASDI/SSI beneficiaries by state and county."""

    DATASET_ID = "us_ssa_beneficiaries"

    OASDI_BASE = "https://www.ssa.gov/policy/docs/statcomps/oasdi_sc/"
    SSI_BASE = "https://www.ssa.gov/policy/docs/statcomps/ssi_sc/"

    # www.ssa.gov sits behind Akamai, which 403s plain requests/urllib/curl.
    # curl_cffi's Chrome TLS fingerprint is admitted.
    IMPERSONATE = "chrome124"

    # (base_url_key, stem) for each flattened time-series file.
    SOURCE_FILES = (
        ("oasdi", "oasdi_state_county_table_1"),
        ("oasdi", "oasdi_state_county_table_2"),
        ("oasdi", "oasdi_state_county_table_3"),
        ("oasdi", "oasdi_state_county_table_4"),
        ("oasdi", "oasdi_state_county_table_5"),
        ("ssi", "ssi_state_county_table_1"),
        ("ssi", "ssi_state_county_table_2"),
        ("ssi", "ssi_state_county_table_3"),
    )

    TABLES = (
        "oasdi_county",
        "oasdi_state",
        "oasdi_population_share",
        "ssi_county",
        "ssi_state",
        "dicionario",
    )

    # Tables carrying a `year` partition and a date column, for coverage records.
    PARTITIONED_TABLES = (
        "oasdi_county",
        "oasdi_state",
        "oasdi_population_share",
        "ssi_county",
        "ssi_state",
    )

    ARCHITECTURE_DIR = str(
        REPO_ROOT / "models" / "us_ssa_beneficiaries" / "code" / "architecture"
    )
