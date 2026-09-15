"""Run the one-shot download step of the us_ffiec_bank_reporting onboarding.

The implementation lives in `pipelines/datasets/us_ffiec_bank_reporting/download.py`
so that the recurring Prefect flow and this script share one transform rather
than drifting apart. This file only puts the repository root on `sys.path` and
forwards the command line.

    python download.py [all|call|bhc|cra|mdrm] [--quarters N]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_ffiec_bank_reporting.download import main

if __name__ == "__main__":
    main()
