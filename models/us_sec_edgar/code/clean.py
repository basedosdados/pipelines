"""One-shot onboarding bootstrap for us_sec_edgar.

Downloads every quarterly Financial Statement Data Set ZIP the SEC publishes and
writes the partitioned all-STRING staging parquet. The transform itself lives in
`pipelines/datasets/us_sec_edgar/utils.py` and is shared with the recurring
Prefect pipeline — this script only drives it.

    uv run python models/us_sec_edgar/code/clean.py            # all quarters
    uv run python models/us_sec_edgar/code/clean.py 2025q1 2025q2

Scratch data goes to `$US_SEC_EDGAR_DATA_DIR` (default
`~/Downloads/us_sec_edgar_data`), never into the repo or Dropbox.
"""

import argparse
import json
import os
import re
import sys

from pipelines.datasets.us_sec_edgar.constants import constants
from pipelines.datasets.us_sec_edgar.utils import (
    clean_all,
    list_source_quarters,
)


def parse_quarter(text: str):
    match = re.fullmatch(r"(\d{4})[qQ]([1-4])", text)
    if not match:
        raise argparse.ArgumentTypeError(f"expected e.g. 2025q1, got {text!r}")
    return int(match.group(1)), int(match.group(2))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("quarters", nargs="*", type=parse_quarter)
    parser.add_argument("--keep-zip", action="store_true")
    parser.add_argument("--data-dir", default=constants.SCRATCH_DIR.value)
    args = parser.parse_args(argv)

    input_dir = os.path.join(args.data_dir, "input")
    output_dir = os.path.join(args.data_dir, "output")
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    quarters = args.quarters or list_source_quarters()
    print(
        f"{len(quarters)} quarters: {quarters[0]} .. {quarters[-1]}",
        flush=True,
    )

    totals = clean_all(quarters, input_dir, output_dir, keep_zip=args.keep_zip)

    print("\n=== totals ===")
    for table, count in sorted(totals.items()):
        print(f"{table}: {count:,}")
    with open(os.path.join(args.data_dir, "row_counts.json"), "w") as fh:
        json.dump(totals, fh, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
