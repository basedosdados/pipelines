"""Count rows in the harvested TCE-MG CSVs, locally, without touching BigQuery.

WHY LOCAL COUNTING IS ENOUGH TO CATCH THE THING THAT MATTERS
------------------------------------------------------------
The expensive way to find out whether a re-download reproduces what MiDES already
publishes is to upload 26 GB to staging, run dbt, and diff the result. That costs
a staging dataset, a materialisation and a day. It is also the wrong first step:
the failure this harvest is most likely to have -- a municipality silently missing
from an exercise -- shows up in a row count long before it shows up in a model.

So this reads the zips where they sit, counts data rows per exercise and stream,
and prints them beside the published BigQuery totals. Nothing is uploaded, nothing
is decompressed to disk, and a full exercise takes a couple of minutes.

WHAT IT CAN AND CANNOT TELL YOU
-------------------------------
It compares RAW SOURCE rows against PUBLISHED MODEL rows. Those are not the same
thing and are not expected to match exactly:

*   `liquidacao` and `pagamento` in MiDES are each a UNION of the despesa stream
    with `rsp` (restos a pagar, which arrives in the *empenho* zip). So the
    published count should be compared against `liquidacao + rsp`, not against
    `liquidacao` alone. The mapping is in `clean_mg.py`.
*   The dbt models filter, cast and in places de-duplicate. A published count
    slightly below the raw count is normal; one far below, or above, is not.

Treat a match within a few percent as "the download reproduces the source", and
anything else as a lead to chase -- not as a pass/fail gate. The real gate is
`verify_mg_harvest.py`, which checks completeness against the portal's own file
counts.

CSV dialect, verified 2026-09-22: semicolon-delimited, LF, latin-1, and
**QUOTE_NONE** -- the `"` characters that do appear are literal data inside
free-text description fields (a measurement in inches, say), not field
delimiters. Checked on `2021.3100203.empenho`: one line of 6,477 contains a
quote, every line carries exactly the header's 33 semicolons, and no line has a
short field count.

That means counting newlines is exact, and ~40x cheaper than a CSV reader. But
"contains no quote character" is the WRONG thing to assert -- it rejects
perfectly good files, as a first version of this script did for 1,932 of 3,412
archives. The property newline-counting actually depends on is that **no record
spans two lines**. So each file is checked by delimiter arithmetic instead:
total semicolons must equal lines x semicolons-per-header. A record broken
across a newline shows up immediately as a shortfall, and the check rides along
with the byte scan already being done.

Usage:
    python audit_mg_local.py --year 2021
    python audit_mg_local.py                 # every exercise on disk
"""

from __future__ import annotations

import argparse
import sys
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
# pyrefly: ignore [missing-import]  # sibling module via sys.path
from harvest_mg import MG_INPUT

# Which CSV inside which category's zip feeds which MiDES table.
# (category, stream) -> label used in the report.
STREAMS = {
    ("empenho", "empenho"): "empenho",
    ("empenho", "rsp"): "rsp",
    ("despesa", "liquidacao"): "liquidacao",
    ("despesa", "pagamento"): "pagamento",
    ("licitacao", "licitacao"): "licitacao",
    ("licitacao", "itemLicitacao"): "licitacao_item",
    ("contrato", "contratos"): "contrato",
}

# Published MiDES row counts for MG, read from BigQuery 2026-09-22. Kept here so
# the audit runs offline; refresh with the query in the module docstring of
# verify_mg_harvest.py if prod changes.
PUBLISHED = {
    "empenho": {
        2014: 4775792,
        2015: 4603682,
        2016: 4519521,
        2017: 4863609,
        2018: 5025688,
        2019: 5239519,
        2020: 4911086,
        2021: 5076760,
    },
    "liquidacao": {
        2014: 7597286,
        2015: 7487418,
        2016: 7543456,
        2017: 7836317,
        2018: 8285728,
        2019: 8669477,
        2020: 8162459,
        2021: 8171258,
    },
    "pagamento": {
        2014: 7464821,
        2015: 7374472,
        2016: 7827370,
        2017: 7674993,
        2018: 8203128,
        2019: 8822744,
        2020: 8529850,
        2021: 8229759,
    },
    "licitacao": {
        2014: 87164,
        2015: 73266,
        2016: 64769,
        2017: 87520,
        2018: 79995,
        2019: 80281,
        2020: 81645,
        2021: 88802,
    },
    "licitacao_item": {
        2014: 2184326,
        2015: 2012538,
        2016: 1732261,
        2017: 2426236,
        2018: 2038189,
        2019: 2038614,
        2020: 1769557,
        2021: 2120129,
    },
}
# Published liquidacao/pagamento are unions with rsp -- compare against the sum.
UNION_WITH_RSP = ("liquidacao", "pagamento")

NL = b"\n"
SEP = b";"


def count_zip(path: Path, wanted: dict[str, str]) -> dict[str, int]:
    """Data rows per stream in one municipality's archive.

    Reads only the members asked for, and streams each rather than materialising
    it: a single despesa archive expands to ~30 MB and there are 853 per
    exercise.

    Rows are counted by newline -- exact for this source's QUOTE_NONE dialect --
    and validated by delimiter arithmetic rather than by the presence or absence
    of quote characters. See the module docstring for why that distinction cost
    a full re-run to discover.
    """
    out: dict[str, int] = {}
    try:
        with zipfile.ZipFile(path) as archive:
            for name in archive.namelist():
                parts = name.split(".")
                if len(parts) < 5 or parts[-1] != "csv":
                    continue
                label = wanted.get(parts[3])
                if label is None:
                    continue
                newlines = 0
                delimiters = 0
                header_delims = -1
                header = b""
                last = b""
                with archive.open(name) as handle:
                    while True:
                        block = handle.read(1 << 20)
                        if not block:
                            break
                        if header_delims < 0:
                            header += block
                            cut = header.find(NL)
                            if cut >= 0:
                                header_delims = header[:cut].count(SEP)
                        newlines += block.count(NL)
                        delimiters += block.count(SEP)
                        last = block[-1:]
                if header_delims < 0:
                    out[label] = 0  # empty stream, or a header with no newline
                    continue
                # A trailing newline means the final record already ended; without
                # one, the last line still holds a record.
                lines = newlines if last == NL else newlines + 1
                expected = header_delims * lines
                if delimiters != expected:
                    raise ValueError(
                        f"{path.name}:{name} field-count mismatch -- {delimiters} "
                        f"delimiters across {lines} lines, expected {expected} "
                        f"({header_delims}/line). A record spanning two lines "
                        f"makes a newline count wrong; investigate before "
                        f"trusting this number."
                    )
                out[label] = max(lines - 1, 0)
    except zipfile.BadZipFile as exc:
        raise ValueError(f"{path.name}: {exc}") from exc
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, action="append")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    years = args.year or sorted(
        int(p.name) for p in MG_INPUT.iterdir() if p.name.isdigit()
    )
    by_category: dict[str, dict[str, str]] = defaultdict(dict)
    for (category, stream), label in STREAMS.items():
        by_category[category][stream] = label

    grand: dict[int, dict[str, int]] = {}
    for year in years:
        totals: dict[str, int] = defaultdict(int)
        municipalities: dict[str, int] = defaultdict(int)
        bad: list[str] = []
        for category, wanted in by_category.items():
            directory = MG_INPUT / str(year) / category
            if not directory.exists():
                continue
            files = sorted(directory.glob("*.zip"))
            if not files:
                continue

            def work(path: Path, wanted=wanted):
                try:
                    return path, count_zip(path, wanted), None
                except ValueError as exc:
                    return path, {}, str(exc)

            with ThreadPoolExecutor(args.workers) as pool:
                for _path, counts, error in pool.map(work, files):
                    if error:
                        bad.append(error)
                        continue
                    for label, n in counts.items():
                        totals[label] += n
                        municipalities[label] += 1
        grand[year] = dict(totals)
        print(f"\n=== {year} ===")
        if bad:
            print(f"  {len(bad)} unreadable archive(s):")
            for line in bad[:5]:
                print(f"    {line}")
        if not totals:
            print("  nothing on disk")
            continue
        print(f"  {'stream':<16}{'municipalities':>15}{'rows':>14}")
        for label in sorted(totals):
            print(
                f"  {label:<16}{municipalities[label]:>15}{totals[label]:>14,}"
            )

        # Compare against what MiDES publishes, where the exercise overlaps.
        rows = []
        for table, published in PUBLISHED.items():
            if year not in published:
                continue
            local = totals.get(table, 0)
            if table in UNION_WITH_RSP:
                local += totals.get("rsp", 0)
            if not local:
                continue
            delta = local - published[year]
            pct = 100 * delta / published[year] if published[year] else 0
            rows.append((table, local, published[year], delta, pct))
        if rows:
            print(
                f"\n  {'table':<16}{'local':>14}{'published':>14}{'delta':>12}{'%':>8}"
            )
            for table, local, pub, delta, pct in rows:
                note = " (+rsp)" if table in UNION_WITH_RSP else ""
                print(
                    f"  {table:<16}{local:>14,}{pub:>14,}{delta:>+12,}{pct:>+7.1f}%{note}"
                )

    if len(grand) > 1:
        print("\n=== all exercises on disk ===")
        labels = sorted({k for v in grand.values() for k in v})
        print("  year  " + "".join(f"{label:>16}" for label in labels))
        for year in sorted(grand):
            print(
                f"  {year}  "
                + "".join(
                    f"{grand[year].get(label, 0):>16,}" for label in labels
                )
            )


if __name__ == "__main__":
    main()
