"""Re-export the table specification, which lives with the pipeline.

The cleaning transform and the architecture build must agree on renames, keys
and partition columns, so the spec has exactly one home:
``pipelines/datasets/us_fema_openfema/spec.py``. This shim lets the build
scripts in this directory import it without a path dance.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_fema_openfema.spec import (  # noqa: F401
    DERIVED,
    DROP,
    HARMONISE,
    TABLES,
    TYPE_OVERRIDE,
)
