"""Derive the canonical zero-pad width for coded STRING columns.

INDEC zero-padded its code columns until some point between 2016 and 2024 and
then stopped, so the same decile group appears as "03" in early waves and "3"
in recent ones, and the same occupation code as "01234" and "1234". Left alone
that puts two spellings of one value into a single column across the 87 waves.

This pass measures, for every STRING column in the architecture and every wave,
whether values are zero-padded and to what length. A column is declared
paddable when some wave zero-pads it to a single constant width W and no wave
ever exceeds W. The result is written to pad_widths.json and applied by
eph_clean.py.

Columns whose length genuinely changes -- CODUSU, which moves from a 6-digit
number to a 29-character hash in 2016 -- are excluded, because padding them
would be wrong rather than merely cosmetic.
"""

import csv
import json
import shutil
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pyreadstat

sys.path.insert(0, str(Path(__file__).resolve().parent))
from archives import data_members, extract
from constants import ARCH_DIR, CODE_DIR, TABLES, waves

# Never padded, whatever the measurements say.
PAD_EXCLUDE = {"CODUSU"}


def read_wave(wave: dict, member: str) -> pd.DataFrame:
    tmp = Path(tempfile.mkdtemp(prefix="eph_pad_"))
    try:
        path = extract(wave, member, tmp)
        if wave["fmt"] == "dta":
            df, meta = pyreadstat.read_dta(str(path))
            df.columns = [c.upper() for c in meta.column_names]
            return df.astype("string")
        df = pd.read_csv(
            path, sep=";", encoding="latin-1", dtype=str, low_memory=False
        )
        df.columns = [str(c).strip().strip('"').upper() for c in df.columns]
        return df.astype("string")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> int:
    arch = {}
    for table in TABLES:
        with open(ARCH_DIR / f"{table}.csv", encoding="utf-8") as handle:
            arch[table] = {
                r["original_name"]: r
                for r in csv.DictReader(handle)
                if r["bigquery_type"] == "STRING"
            }

    # per table, per column: set of padded widths seen, and the max length seen
    padded_widths: dict[str, dict[str, set]] = {
        t: defaultdict(set) for t in TABLES
    }
    max_len: dict[str, dict[str, int]] = {t: defaultdict(int) for t in TABLES}
    free_text: dict[str, set] = {t: set() for t in TABLES}

    for wave in waves():
        for table, member in data_members(wave).items():
            df = read_wave(wave, member)
            for col in arch[table]:
                if col not in df.columns:
                    continue
                s = df[col].str.strip().replace({"": pd.NA, "nan": pd.NA})
                nn = s.dropna()
                if nn.empty:
                    continue
                lengths = nn.str.len()
                max_len[table][col] = max(
                    max_len[table][col], int(lengths.max())
                )
                # Anything non-numeric means this is not a zero-padded code.
                if not bool(nn.str.fullmatch(r"\d+").all()):
                    free_text[table].add(col)
                    continue
                padded = nn.str.match(r"^0\d+$")
                if bool(padded.any()):
                    padded_widths[table][col].update(
                        int(x) for x in lengths[padded].unique()
                    )
        print(f"scanned {wave['year']}Q{wave['quarter']}", flush=True)

    result: dict[str, dict[str, int]] = {}
    report: dict[str, list] = {}
    for table in TABLES:
        result[table] = {}
        report[table] = []
        for col, widths in padded_widths[table].items():
            if col in PAD_EXCLUDE or col in free_text[table]:
                continue
            # A single constant pad width, never exceeded anywhere.
            if len(widths) == 1:
                width = next(iter(widths))
                if max_len[table][col] <= width:
                    result[table][col] = width
                    continue
            report[table].append(
                {
                    "column": col,
                    "padded_widths": sorted(widths),
                    "max_len": max_len[table][col],
                }
            )

    (CODE_DIR / "pad_widths.json").write_text(
        json.dumps(
            {"widths": result, "ambiguous": report},
            ensure_ascii=False,
            indent=1,
        ),
        encoding="utf-8",
    )
    for table in TABLES:
        print(f"\n{table}: {len(result[table])} paddable columns")
        for col, w in sorted(result[table].items()):
            print(f"   {col:<14} -> width {w}")
        if report[table]:
            print(f"   ambiguous, left untouched: {report[table]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
