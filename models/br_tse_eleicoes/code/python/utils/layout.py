"""Header-based column resolution for raw TSE files.

The Stata→Python builders historically mapped raw columns by *position*
(``v1..vN``). When the TSE republished files with inserted columns, those
positional maps silently drifted. This module lets a builder rename the
positional ``vN`` columns to the **official TSE variable names** for a given
``(family, ano)``, so builders can select columns by name instead of position.

The ordered official names come from the layout artifacts produced by the
diagnostics harness (``diagnostics/artifacts/layouts/{family}_{ano}.json``).
Reading those same JSON files here — rather than the file's own header — keeps
the runtime rename and the static harness check (``tier3_check._check_named``)
provably consistent: both consume the identical layout. Refresh the artifacts
with ``python -m diagnostics run --tier 2`` when the TSE changes a layout.
"""

from __future__ import annotations

import json
from functools import cache
from pathlib import Path

import pandas as pd

# .../code/python/utils/layout.py -> .../code/python/diagnostics/artifacts/layouts
_LAYOUTS_DIR = (
    Path(__file__).resolve().parent.parent
    / "diagnostics"
    / "artifacts"
    / "layouts"
)


@cache
def layout_columns(family: str, ano: int) -> tuple[str, ...] | None:
    """Ordered official TSE variable names for ``(family, ano)``.

    Position N (1-based) corresponds to raw column ``vN``. Returns ``None``
    when no layout artifact exists (e.g. headerless historical files without a
    transcribed override) so the caller can fall back to positional names.
    """
    path = _LAYOUTS_DIR / f"{family}_{ano}.json"
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    cols = data.get("columns") or []
    if len(cols) < 3:  # mirrors tier3's "no usable layout" guard
        return None
    return tuple(cols)


def resolve_columns(df: pd.DataFrame, family: str, ano: int) -> pd.DataFrame:
    """Rename positional ``vN`` columns to official TSE names.

    Renames ``v1..vN`` to ``layout_columns(family, ano)[0..N-1]``. Columns
    beyond the layout length (or the layout beyond the frame width) are left
    untouched. When no layout exists, the frame is returned unchanged so the
    builder keeps the positional names (the only viable mode for headerless
    historical files).
    """
    cols = layout_columns(family, ano)
    if cols is None:
        return df
    n = min(len(df.columns), len(cols))
    rename = {f"v{i + 1}": cols[i] for i in range(n)}
    return df.rename(columns=rename)
