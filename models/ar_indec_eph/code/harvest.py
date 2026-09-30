"""Harvest the column universe, labels and value labels across all 87 waves.

Produces, in code/:
  column_universe.json   per table: every column ever present, the waves it
                         appears in, and its source dtype per era
  source_labels.json     column -> Spanish label, from the Stata variable labels
  value_labels.json      column -> {code: label}, from the Stata value labels
  wave_rows.json         per wave per table row counts (a completeness guard)

Only the 2003-2015 Stata era carries labels; columns introduced from 2016 on
appear in column_universe.json with no label and are described from the
per-wave EPH_registro PDFs instead.
"""

import json
import shutil
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

import pyreadstat

sys.path.insert(0, str(Path(__file__).resolve().parent))
from archives import data_members, extract
from constants import CODE_DIR, TABLES, waves


def read_txt_header(path: Path) -> list[str]:
    with open(path, encoding="latin-1") as handle:
        header = handle.readline().rstrip("\r\n")
    return [c.strip().strip('"').upper() for c in header.split(";")]


def count_txt_rows(path: Path) -> int:
    with open(path, "rb") as handle:
        return sum(1 for _ in handle) - 1


def main() -> int:
    universe: dict[str, dict[str, dict]] = {t: {} for t in TABLES}
    labels: dict[str, dict[str, str]] = {t: {} for t in TABLES}
    value_labels: dict[str, dict[str, dict]] = {t: {} for t in TABLES}
    wave_rows: dict[str, dict[str, int]] = defaultdict(dict)

    for wave in waves():
        tag = f"{wave['year']}Q{wave['quarter']}"
        members = data_members(wave)
        tmp = Path(tempfile.mkdtemp(prefix="eph_"))
        try:
            for table, member in members.items():
                path = extract(wave, member, tmp)
                if wave["fmt"] == "dta":
                    _, meta = pyreadstat.read_dta(str(path), metadataonly=True)
                    cols = [c.upper() for c in meta.column_names]
                    rows = meta.number_rows
                    for col, lab in zip(
                        cols, meta.column_labels or [], strict=False
                    ):
                        if lab and col not in labels[table]:
                            labels[table][col] = lab.strip()
                    for var, mapping in (
                        meta.variable_value_labels or {}
                    ).items():
                        key = var.upper()
                        store = value_labels[table].setdefault(key, {})
                        for code, lab in mapping.items():
                            code_s = (
                                str(int(code))
                                if isinstance(code, float)
                                and code.is_integer()
                                else str(code)
                            )
                            store.setdefault(code_s, str(lab).strip())
                else:
                    cols = read_txt_header(path)
                    rows = count_txt_rows(path)
                wave_rows[tag][table] = rows
                for col in cols:
                    entry = universe[table].setdefault(
                        col, {"waves": [], "first": None, "last": None}
                    )
                    entry["waves"].append(tag)
                path.unlink(missing_ok=True)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        print(
            f"{tag:8s} "
            + "  ".join(
                f"{t.split('_')[-1]}={wave_rows[tag][t]:>6d}" for t in TABLES
            ),
            flush=True,
        )

    for table in TABLES:
        for entry in universe[table].values():
            ws = entry["waves"]
            entry["first"], entry["last"], entry["n_waves"] = (
                ws[0],
                ws[-1],
                len(ws),
            )

    (CODE_DIR / "column_universe.json").write_text(
        json.dumps(universe, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    (CODE_DIR / "source_labels.json").write_text(
        json.dumps(labels, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    (CODE_DIR / "value_labels.json").write_text(
        json.dumps(value_labels, ensure_ascii=False, indent=1),
        encoding="utf-8",
    )
    (CODE_DIR / "wave_rows.json").write_text(
        json.dumps(wave_rows, ensure_ascii=False, indent=1), encoding="utf-8"
    )

    print("\n=== summary ===")
    for table in TABLES:
        n_all = len(universe[table])
        n_full = sum(
            1 for e in universe[table].values() if e["n_waves"] == len(waves())
        )
        print(
            f"{table}: {n_all} columns in union, {n_full} present in all "
            f"{len(waves())} waves, {len(labels[table])} labelled, "
            f"{len(value_labels[table])} with value labels"
        )
        print(f"  total rows: {sum(r[table] for r in wave_rows.values()):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
