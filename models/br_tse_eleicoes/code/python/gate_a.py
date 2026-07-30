"""Gate A — full-cell parity between the Stata .dta outputs and the Python
parquets (fix_plan/04).

For every ``output/*.dta`` reference, pair the matching parquet(s) in
``output_python/`` and compare every cell, chunked (never whole-table in
RAM). Values are compared through the same normalization the March
validation used (``validate._normalize_col_to_str``): float32 rounding for
float columns, ``.0``-stripping for int-likes, NA -> "".

Columns are aligned by NAME; an order difference is reported as a note, not
a failure. Row ORDER is ignored — the March validation sorted both sides
before sampling, so .dta and parquet row orders were never aligned. The
comparison is a full multiset equality: sorted row-hash arrays decide
MATCH/MISMATCH, and per-column commutative hash digests attribute which
columns differ. Numeric-looking strings ("3000.00" vs "3000") are compared
through the float path.

The three perfil tables have a single all-years .dta; their per-year
parquets are streamed in ascending-year order to match Stata's append.

Results are appended, one line per pair as soon as it finishes, to
``fix_plan/parity_matrix.md`` (repo) and detailed JSON records to
``gate_a_results.jsonl`` next to it.

Run from ``code/python/``:

    TSE_DATA_DIR=... uv run python gate_a.py [--only STEM]

Sequential by design — never parallelize (RAM).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from config import OUTPUT_PYTHON, OUTPUT_STATA
from validate import _normalize_col_to_str

CHUNK = 2_000_000
FIX_PLAN = Path(__file__).resolve().parent.parent / "fix_plan"
MATRIX = FIX_PLAN / "parity_matrix.md"
DETAILS = FIX_PLAN / "gate_a_results.jsonl"

# perfil tables: one all-years .dta vs per-year parquets
CONCAT_STEMS = {
    "perfil_eleitorado_municipio_zona",
    "perfil_eleitorado_secao",
}


class ParquetSequence:
    """Row-ordered reader over one or more parquet files."""

    def __init__(self, paths: list[Path]):
        self.files = [pq.ParquetFile(p) for p in paths]
        self.columns = self.files[0].schema_arrow.names
        self.num_rows = sum(f.metadata.num_rows for f in self.files)
        self._fi = 0
        self._table = None
        self._offset = 0

    def next_rows(self, n: int) -> pd.DataFrame:
        parts = []
        need = n
        while need > 0 and self._fi < len(self.files):
            if self._table is None:
                self._table = self.files[self._fi].read()
                self._offset = 0
            avail = self._table.num_rows - self._offset
            take = min(avail, need)
            if take > 0:
                parts.append(self._table.slice(self._offset, take).to_pandas())
                self._offset += take
                need -= take
            if self._offset >= self._table.num_rows:
                self._table = None
                self._fi += 1
        if not parts:
            return pd.DataFrame(columns=self.columns)
        return pd.concat(parts, ignore_index=True)


def _log_line(text: str) -> None:
    with open(MATRIX, "a") as fh:
        fh.write(text + "\n")
    print(text, flush=True)


def _detail(rec: dict) -> None:
    rec["ts"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with open(DETAILS, "a") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _decide_float_cols(a: pd.DataFrame, b: pd.DataFrame, shared) -> set:
    """Float path if either side is float dtype, or both sides are
    numeric-parseable strings and either carries a decimal separator."""

    out = set()
    for col in shared:
        if pd.api.types.is_float_dtype(a[col]) or pd.api.types.is_float_dtype(
            b[col]
        ):
            out.add(col)
            continue
        sa = a[col].astype(str).str.strip()
        sb = b[col].astype(str).str.strip()
        sa = sa[(sa != "") & (sa != "nan") & (sa != "<NA>")].head(2000)
        sb = sb[(sb != "") & (sb != "nan") & (sb != "<NA>")].head(2000)
        if len(sa) == 0 or len(sb) == 0:
            continue
        na = pd.to_numeric(sa, errors="coerce")
        nb = pd.to_numeric(sb, errors="coerce")
        if (
            na.notna().mean() > 0.999
            and nb.notna().mean() > 0.999
            and (
                sa.str.contains(".", regex=False).any()
                or sb.str.contains(".", regex=False).any()
            )
        ):
            out.add(col)
    return out


def compare_pair(stem: str, dta_path: Path, parquet_paths: list[Path]):
    import numpy as np

    np.seterr(over="ignore")  # commutative digest wraps mod 2**64 by design

    seq = ParquetSequence(parquet_paths)
    reader = pd.read_stata(
        dta_path, convert_categoricals=False, chunksize=CHUNK
    )

    first = next(
        iter(
            pd.read_stata(
                dta_path, convert_categoricals=False, chunksize=10_000
            )
        )
    )
    dta_cols = list(first.columns)
    pq_cols = list(seq.columns)
    missing = [c for c in dta_cols if c not in pq_cols]
    extra = [c for c in pq_cols if c not in dta_cols]
    order_differs = dta_cols != pq_cols
    shared = [c for c in dta_cols if c in pq_cols]
    float_set = _decide_float_cols(first, seq.next_rows(10_000), shared)
    seq = ParquetSequence(parquet_paths)  # rewind

    def norm_frame(df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(
            {
                c: _normalize_col_to_str(
                    df[c].reset_index(drop=True), is_float=c in float_set
                )
                for c in shared
            }
        )

    row_hashes = {"stata": [], "python": []}
    col_digest = {
        "stata": {c: np.uint64(0) for c in shared},
        "python": {c: np.uint64(0) for c in shared},
    }
    positional_equal = True
    rows_dta = 0
    for chunk in reader:
        rows_dta += len(chunk)
        other = seq.next_rows(len(chunk))
        for side, frame in (("stata", chunk), ("python", other)):
            if side == "python" and len(frame) == 0:
                continue
            nf = norm_frame(frame)
            h = pd.util.hash_pandas_object(nf, index=False).to_numpy(
                dtype="uint64"
            )
            row_hashes[side].append(h)
            for c in shared:
                ch = pd.util.hash_pandas_object(nf[c], index=False).to_numpy(
                    dtype="uint64"
                )
                col_digest[side][c] += np.uint64(
                    np.bitwise_xor.reduce(ch) ^ np.uint64(len(ch))
                ) + ch.sum(dtype="uint64")
            del nf
        if positional_equal:
            a, b = (
                row_hashes["stata"][-1],
                (
                    row_hashes["python"][-1]
                    if len(row_hashes["python"]) == len(row_hashes["stata"])
                    else None
                ),
            )
            if b is None or len(a) != len(b) or not np.array_equal(a, b):
                positional_equal = False
        del chunk, other
    # drain any parquet rows beyond the dta length
    tail = seq.next_rows(CHUNK)
    while len(tail):
        nf = norm_frame(tail)
        h = pd.util.hash_pandas_object(nf, index=False).to_numpy(
            dtype="uint64"
        )
        row_hashes["python"].append(h)
        for c in shared:
            ch = pd.util.hash_pandas_object(nf[c], index=False).to_numpy(
                dtype="uint64"
            )
            col_digest["python"][c] += np.uint64(
                np.bitwise_xor.reduce(ch) ^ np.uint64(len(ch))
            ) + ch.sum(dtype="uint64")
        positional_equal = False
        del nf, tail
        tail = seq.next_rows(CHUNK)

    ha = np.sort(np.concatenate(row_hashes["stata"]))
    hb = (
        np.sort(np.concatenate(row_hashes["python"]))
        if row_hashes["python"]
        else np.array([], dtype="uint64")
    )
    del row_hashes
    rows_pq = seq.num_rows
    multiset_equal = len(ha) == len(hb) and bool(np.array_equal(ha, hb))
    diff_rows = 0
    if not multiset_equal:
        # count rows not matched 1:1 between the two sorted hash arrays
        common = 0
        i = j = 0
        while i < len(ha) and j < len(hb):
            if ha[i] == hb[j]:
                common += 1
                i += 1
                j += 1
            elif ha[i] < hb[j]:
                i += 1
            else:
                j += 1
        diff_rows = max(len(ha), len(hb)) - common
    del ha, hb

    diff_cols = [
        c for c in shared if col_digest["stata"][c] != col_digest["python"][c]
    ]

    status = "MATCH" if multiset_equal and not missing else "MISMATCH"
    notes = []
    if missing:
        notes.append(f"cols only in stata: {missing}")
    if extra:
        notes.append(f"cols only in python: {extra}")
    if rows_dta != rows_pq:
        notes.append(f"rows: stata={rows_dta} python={rows_pq}")
    if not multiset_equal:
        notes.append(f"{diff_rows} rows differ; cols: {diff_cols}")
    if status == "MATCH":
        notes.append(
            "row order identical"
            if positional_equal
            else "row order differs (content identical)"
        )
        if order_differs:
            notes.append("column order differs")

    _log_line(
        f"| {stem} | {status} | rows={rows_dta} | " + "; ".join(notes) + " |"
    )
    _detail(
        {
            "stem": stem,
            "status": status,
            "rows_stata": rows_dta,
            "rows_python": rows_pq,
            "missing_cols": missing,
            "extra_cols": extra,
            "order_differs": order_differs,
            "positional_equal": positional_equal,
            "diff_rows": diff_rows,
            "diff_cols": diff_cols,
        }
    )


def build_pairs() -> list[tuple[str, Path, list[Path]]]:
    pairs = []
    for dta in sorted(OUTPUT_STATA.glob("*.dta")):
        stem = dta.stem
        if stem in CONCAT_STEMS:
            pqs = sorted(
                OUTPUT_PYTHON.glob(f"{stem}_[0-9][0-9][0-9][0-9].parquet")
            )
            if pqs:
                pairs.append((stem, dta, pqs))
                continue
        p = OUTPUT_PYTHON / f"{stem}.parquet"
        if p.exists():
            pairs.append((stem, dta, [p]))
        elif stem not in _done_stems():
            _log_line(f"| {stem} | UNPAIRED | | no parquet in output_python |")
            _detail({"stem": stem, "status": "UNPAIRED"})
    # smallest .dta first so results accumulate early
    pairs.sort(key=lambda t: t[1].stat().st_size)
    return pairs


def _done_stems() -> set[str]:
    done = set()
    if DETAILS.exists():
        for line in DETAILS.read_text().splitlines():
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("status") in ("MATCH", "MISMATCH", "UNPAIRED"):
                done.add(rec["stem"])
    return done


def main():
    only = None
    if "--only" in sys.argv:
        only = sys.argv[sys.argv.index("--only") + 1]
    FIX_PLAN.mkdir(parents=True, exist_ok=True)
    done = _done_stems()
    if not MATRIX.exists():
        MATRIX.write_text(
            "# Gate A parity matrix — Stata .dta vs Python parquet\n\n"
            "Full-cell comparison (validate.py normalization: float32\n"
            'rounding on float columns, NA -> ""). Generated by\n'
            "`code/python/gate_a.py`; details in gate_a_results.jsonl.\n\n"
            "| pair | status | rows | notes |\n|---|---|---|---|\n"
        )
    for stem, dta, pqs in build_pairs():
        if only and only != stem:
            continue
        if stem in done:
            continue
        try:
            compare_pair(stem, dta, pqs)
        except Exception as e:  # keep the sweep going, record the failure
            _log_line(f"| {stem} | ERROR | | {type(e).__name__}: {e} |")
            _detail({"stem": stem, "status": "ERROR", "error": repr(e)})


if __name__ == "__main__":
    main()
