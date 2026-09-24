"""One-shot onboarding bootstrap for br_mapbiomas_estatisticas.

Downloads the MapBiomas Collection 11 municipality statistics workbook and
writes the cleaned, hive-partitioned parquet. The transform itself lives in
`pipelines/datasets/br_mapbiomas_estatisticas/utils.py` and is shared with the
recurring pipeline, so there is exactly one copy of it.

    python models/br_mapbiomas_estatisticas/code/clean_data.py
    python models/br_mapbiomas_estatisticas/code/clean_data.py --skip-download
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.br_mapbiomas_estatisticas.constants import (  # noqa: E402
    constants,
)
from pipelines.datasets.br_mapbiomas_estatisticas.utils import (  # noqa: E402
    clean_all,
    download_biome_state_workbook,
    download_legend_csv,
    download_municipality_workbook,
    resolve_municipality_drive_id,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir", default=str(constants.DEFAULT_DATA_DIR.value)
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="reuse whatever is already in <data-dir>/input",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    input_dir, output_dir = data_dir / "input", data_dir / "output"

    if not args.skip_download:
        drive_id = resolve_municipality_drive_id()
        if drive_id != constants.MUNICIPALITY_DRIVE_ID.value:
            print(
                f"NOTE: the statistics page now serves Drive id {drive_id}, not the "
                f"pinned {constants.MUNICIPALITY_DRIVE_ID.value}. MapBiomas has most "
                "likely released a new collection -- check the legend before "
                "trusting the output."
            )
        print(f"downloading workbook to {input_dir}")
        download_municipality_workbook(input_dir, drive_id)
        download_biome_state_workbook(input_dir)
        download_legend_csv(input_dir)

    report = clean_all(input_dir, output_dir)
    years = constants.LAST_YEAR.value - constants.FIRST_YEAR.value + 1
    expected = report["grouped_rows"] * years
    if report["long_rows"] != expected:
        raise SystemExit(
            f"row count {report['long_rows']} does not equal grouped rows x years "
            f"({expected}); the workbook's year columns changed"
        )
    for key, value in report.items():
        print(f"  {key}: {value}")
    print(f"output written to {output_dir}")


if __name__ == "__main__":
    main()
