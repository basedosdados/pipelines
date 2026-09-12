"""Normalise the per-table column specs into one shape.

``_spec.py`` (execucao) predates ``_spec_favorecido.py`` and has no
``has_sensitive_data`` field, so its tuples are one element shorter. Rather than
rewrite the older spec — and risk perturbing a table already published on prod —
both are normalised here into dicts, and the builders consume only these.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# pyrefly: ignore [missing-import]
from _spec import SPEC as _EXECUCAO

# pyrefly: ignore [missing-import]
from _spec_favorecido import SPEC as _FAVORECIDO

_KEYS = (
    "name",
    "bigquery_type",
    "source_header",
    "description_pt",
    "description_en",
    "description_es",
    "measurement_unit",
    "directory_column",
)


def _normalise(spec: list[tuple], has_sensitive_field: bool) -> list[dict]:
    out = []
    for row in spec:
        d = dict(zip(_KEYS, row[:8], strict=True))
        if has_sensitive_field:
            d["has_sensitive_data"], d["observations_pt"] = row[8], row[9]
        else:
            d["has_sensitive_data"], d["observations_pt"] = "no", row[8]
        out.append(d)
    return out


# table slug -> (columns, period column header)
TABLES = {
    "execucao": (_normalise(_EXECUCAO, False), "Ano e mês do lançamento"),
    "favorecido": (_normalise(_FAVORECIDO, True), "Ano e mês do lançamento"),
}
