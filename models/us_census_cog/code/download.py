"""Download every us_census_cog source archive into the scratch input tree.

    python download.py              # everything
    python download.py gus          # one family: gus | employment | finance

Files already present are left alone, so the script is resumable. Nothing here
writes to the repo.
"""

import sys

from common import INPUT, K

from pipelines.datasets.us_census_cog.utils import (
    download_employment,
    download_finance,
    download_gus,
)


def main(families: list[str]) -> None:
    """Download the requested source families."""
    if not families:
        families = ["gus", "employment", "finance"]
    if "gus" in families:
        paths = download_gus(INPUT, K.GUS_YEARS)
        print(f"gus: {len(paths)} archives")
    if "employment" in families:
        paths = download_employment(INPUT, K.EMPLOYMENT_YEARS)
        print(f"employment: {len(paths)} archives")
    if "finance" in families:
        paths = download_finance(INPUT, K.FINANCE_YEARS)
        print(f"finance: {len(paths)} archives")


if __name__ == "__main__":
    main(sys.argv[1:])
