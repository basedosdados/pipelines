"""Download the latest BDS release into the scratch input directory.

Usage:
    ~/.venvs/bd-pipelines/bin/python -m models.us_census_bds.code.download
"""

from pipelines.datasets.us_census_bds.constants import constants
from pipelines.datasets.us_census_bds.utils import (
    download_release,
    latest_release,
)


def main() -> None:
    release = latest_release()
    paths = download_release(release, constants.DATA_DIR.value / "input")
    print(f"release {release}: {len(paths)} files")


if __name__ == "__main__":
    main()
