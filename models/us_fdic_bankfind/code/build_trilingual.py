"""Add description_pt / description_en / description_es to the architecture CSVs.

Data Basis requires every column description in three languages, and
`bulk_upsert_columns` reads those three columns when they are present.  The
English text is the architecture's own `description`; the other two come from the
explicit translation tables, which cover every distinct description.

Writes `architecture_trilingual/<table>.csv`, which is what gets uploaded to
Drive and registered.  The plain `architecture/` CSVs stay the source of truth
for names, types and units.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from translations_financials import (  # noqa: E402  # pyrefly: ignore [missing-import]
    FINANCIALS,
)
from translations_structural import (  # noqa: E402  # pyrefly: ignore [missing-import]
    STRUCTURAL,
)

TRANSLATIONS = {**STRUCTURAL, **FINANCIALS}
SOURCE = HERE / "architecture"
TARGET = HERE / "architecture_trilingual"


def build(path: Path) -> tuple[int, int]:
    with path.open() as handle:
        rows = list(csv.DictReader(handle))
        fields = list(rows[0]) if rows else []

    out_fields = [
        *fields,
        "description_pt",
        "description_en",
        "description_es",
    ]
    missing = 0
    for row in rows:
        english = row["description"]
        pair = TRANSLATIONS.get(english)
        if pair is None:
            missing += 1
            pair = (english, english)  # never silently blank a description
        row["description_pt"], row["description_es"] = pair
        row["description_en"] = english

    TARGET.mkdir(parents=True, exist_ok=True)
    with (TARGET / path.name).open("w", newline="") as handle:
        # csv.writer defaults to CRLF; the repo's mixed-line-ending hook
        # rewrites that to LF, so pre-commit.ci reformatted all four files
        # after the first push. Emitting LF here keeps regeneration stable.
        writer = csv.DictWriter(
            handle, fieldnames=out_fields, lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)
    return len(rows), missing


if __name__ == "__main__":
    total_missing = 0
    for path in sorted(SOURCE.glob("*.csv")):
        count, missing = build(path)
        total_missing += missing
        flag = f"  {missing} UNTRANSLATED" if missing else ""
        print(f"{path.stem:<22} {count:>4} columns{flag}")
    if total_missing:
        raise SystemExit(f"{total_missing} descriptions have no translation")
    print("\nall descriptions translated in pt/en/es")
