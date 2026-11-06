"""Build the ``dicionario`` table: one row per (table, column, code).

Covers every architecture column with ``covered_by_dictionary = yes``:

* PUMD coded variables: the BLS dictionary's ``Codes `` sheet, matched on file
  family and lowercased variable name. Coverage comes from First/Last year,
  clipped to 1996 (the first release loaded); codes retired before 1996 are
  dropped.
* PUMD data-quality flags (architecture observations mention flag codes): the
  standard BLS flag set, A-W for the Interview and A-E, T for the Diary.
* LABSTAT: ``series.statistic`` and ``footnote_codes`` from cx.process and
  cx.footnote.
* ``ucc``: grouping, row_type, factor and section, labelled from the stub file
  conventions; coverage from the years each code actually appears in.

Written as one unpartitioned, all-STRING parquet file.

Usage:
    uv run models/us_bls_cex/code/build_dicionario.py
"""

import logging
import re
from collections import defaultdict

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pads

from pipelines.datasets.us_bls_cex.pumd_files import (
    DICTIONARY_XLSX,
    FAMILIES,
    FIRST_RELEASE,
    LABSTAT_DIR,
    OUTPUT_DIR,
)
from pipelines.datasets.us_bls_cex.utils import (
    UNPADDED_EXCEPTIONS,
    labstat_dictionary_rows,
    normalize_code,
    read_arch,
    write_table,
)

log = logging.getLogger("build_dicionario")

TABLES = ["series", "annual", "ucc"] + [t for _, t in FAMILIES.values()]
COLUMNS = ["id_tabela", "nome_coluna", "chave", "cobertura_temporal", "valor"]

FLAGS_INTERVIEW = {
    "A": "Valid blank; a blank field where a response is not anticipated",
    "B": "Invalid blank due to invalid nonresponse",
    "C": "Blank due to don't know, refusal, or other nonresponse",
    "D": "Valid value; unadjusted",
    "E": "Valid value; allocated",
    "F": "Valid value; imputed or adjusted in some other way",
    "G": "Valid value; allocated and imputed",
    "H": "Valid blank for an expenditure that is a parent record",
    "T": "Valid value; topcoded or suppressed",
    "U": "Valid value; allocated then topcoded or suppressed",
    "V": "Valid value; imputed or adjusted then topcoded or suppressed",
    "W": "Valid value; allocated and imputed then topcoded or suppressed",
}
FLAGS_DIARY = {
    k: FLAGS_INTERVIEW[k] for k in ("A", "B", "C", "D", "E", "T")
} | {"B": "Blank due to invalid nonresponse"}

UCC_LABELS = {
    "hierarchy": {
        "integrated": "Integrated grouping: Interview and Diary sources combined (CE-HG-Integ)",
        "interview": "Interview Survey grouping (CE-HG-Inter)",
        "diary": "Diary Survey grouping (CE-HG-Diary)",
    },
    "row_type": {
        "H": "Header: stub parameter file line",
        "T": "Title: heading with no value of its own",
        "G": "Group: sum of the rows below it",
        "S": "Statistic: published statistic such as the number of consumer units",
        "I": "UCC collected in the Interview Survey",
        "D": "UCC collected in the Diary Survey",
        "*": "Untyped section heading without code or level (files up to 2003)",
    },
    "section": {
        "CUCHARS": "Consumer unit characteristics",
        "EXPEND": "Expenditures",
        "FOOD": "Food expenditures detail",
        "INCOME": "Income and taxes",
        "ASSETS": "Assets and liabilities",
        "ADDENDA": "Addenda: other money receipts, outlays and detail items",
    },
}


def coverage(start: int, end: int | None) -> str:
    return f"{start}(1){'' if end is None else end}"


UNDOCUMENTED = "Code not documented in the BLS dictionary"
_ITERATION = re.compile(r"^(?P<parent>.*\D)[1-5]$")


def observed(table: str, col: str) -> pd.DataFrame:
    """Distinct non-null values of a column with the first and last year seen.

    Read from the cleaned output, so values are already unpadded. Tables
    without a ``year`` column report NaN years.
    """
    ds = pads.dataset(OUTPUT_DIR / table, format="parquet")
    if "year" not in ds.schema.names:
        t = ds.to_table(columns=[col]).to_pandas().dropna()
        return pd.DataFrame({"v": t[col].unique(), "lo": pd.NA, "hi": pd.NA})
    t = ds.to_table(columns=[col, "year"])
    t = t.filter(pc.is_valid(t.column(col)))  # pyrefly: ignore
    g = (
        t.group_by(col)
        .aggregate([("year", "min"), ("year", "max")])
        .to_pandas()
    )
    return g.rename(columns={col: "v", "year_min": "lo", "year_max": "hi"})


def observed_coverage(lo, hi) -> str | None:
    if pd.isna(lo):
        return None
    return coverage(int(lo), int(hi))


def row(table, col, key, cov, label, rule, rank=(0, 0)) -> dict:
    return {
        "id_tabela": table,
        "nome_coluna": col,
        "chave": key if col in UNPADDED_EXCEPTIONS else normalize_code(key),
        "cobertura_temporal": cov,
        "valor": label,
        "_rule": rule,
        "_rank": rank,
    }


def load_codes() -> pd.DataFrame:
    codes = pd.read_excel(DICTIONARY_XLSX, sheet_name="Codes ", dtype=str)
    codes.columns = [c.strip() for c in codes.columns]
    codes["file"] = codes["File"].str.strip().str.upper()
    codes["var"] = codes["Variable"].str.strip().str.lower()
    codes["key"] = codes["Code value"].str.strip()
    codes["label"] = codes["Code description"].fillna("").str.strip()
    codes["first"] = pd.to_numeric(codes["First year"], errors="coerce")
    codes["firstq"] = pd.to_numeric(codes["First quarter"], errors="coerce")
    codes["last"] = pd.to_numeric(codes["Last year"], errors="coerce")
    return codes


def expn_file_labels() -> dict[str, str]:
    """Label per EXPN file code, from the Variables sheet's section fields.

    Used for ``rtype`` values the Codes sheet does not list yet. The most
    recent (section number, part, description) of the file's variables wins.
    """
    v = pd.read_excel(DICTIONARY_XLSX, sheet_name="Variables", dtype=str)
    v.columns = [c.strip() for c in v.columns]
    v["file"] = v["File"].str.strip().str.upper()
    v["last"] = pd.to_numeric(v["Last year"], errors="coerce").fillna(9999)
    out: dict[str, str] = {}
    for f, g in v.dropna(subset=["Section description"]).groupby("file"):
        r = g.sort_values("last").iloc[-1]
        num = str(r["Section number"]).strip()
        part = str(r["Section part"]).strip()
        where = f"Section {num}" if num not in ("", "nan") else ""
        if where and part not in ("", "nan"):
            where += f", Part {part}"
        desc = str(r["Section description"]).strip()
        out[str(f)] = f"{where}: {desc}" if where else desc
    return out


def stub_titles() -> dict[str, str]:
    """Latest BLS stub title per 6-digit UCC, preferring the integrated file.

    Labels UCCs that occur in the microdata but are missing from the Codes
    sheet (mostly codes introduced or retired between dictionary revisions).
    """
    t = pads.dataset(OUTPUT_DIR / "ucc").to_table().to_pandas()
    t = t[t["ucc"].fillna("").str.fullmatch(r"[0-9]{6}")]
    t["year"] = t["year"].astype(int)
    t["pref"] = (t["hierarchy"] == "integrated").astype(int)
    t["line"] = t["line_number"].astype(int)
    t = t.sort_values(["year", "pref", "line"])
    return t.groupby("ucc")["title"].last().to_dict()


def pumd_rows(stats: dict) -> list[dict]:
    """Codes-sheet rows, flags, inherited and observed-only codes per column.

    Order of precedence for a value observed in the data but not covered by a
    code valid from 1996 on: a pattern key (``2nn``), a code that BLS dates
    before 1996, the BLS stub title (``ucc`` only), an EXPN file label
    (``rtype`` only); anything left is added
    later by :func:`fill_undocumented`.
    """
    codes = load_codes()
    expn = expn_file_labels()
    stubs = stub_titles()
    rows = []
    for family, (survey, table) in FAMILIES.items():
        sub = codes[codes["file"] == family.upper()]
        for a in read_arch(table):
            if a["covered_by_dictionary"] != "yes":
                continue
            col = a["name"]
            labels = FLAGS_INTERVIEW if survey == "interview" else FLAGS_DIARY
            if "Flag codes" in a["observations"]:
                rows += [
                    row(table, col, k, None, v, "flag")
                    for k, v in labels.items()
                ]
            c = sub[sub["var"] == a["original_name"]]
            rule = "codes"
            m = _ITERATION.match(a["original_name"])
            if c.empty and m:
                c = sub[sub["var"] == m.group("parent")]
                rule = f"inherited:{m.group('parent')}"
            norm = (
                (lambda k: k) if col in UNPADDED_EXCEPTIONS else normalize_code
            )
            current = c[c["last"].isna() | (c["last"] >= FIRST_RELEASE)]
            old = c[c["last"] < FIRST_RELEASE]
            patterns = []
            for r in current.itertuples():
                rank = (r.first, 0 if pd.isna(r.firstq) else r.firstq)
                if re.fullmatch(r"[0-9]*n+[0-9n]*", r.key):
                    patterns.append(
                        (re.compile(r.key.replace("n", "[0-9]")), r)
                    )
                    continue
                start = max(int(r.first), FIRST_RELEASE)
                end = None if pd.isna(r.last) else int(r.last)
                rows.append(
                    row(
                        table,
                        col,
                        r.key,
                        coverage(start, end),
                        r.label,
                        rule,
                        rank,
                    )
                )
            covered = {norm(k) for k in current["key"]}
            if "Flag codes" in a["observations"]:
                covered |= set(labels)
            old_by_key = {}
            for r in old.sort_values(["first", "firstq"]).itertuples():
                old_by_key[norm(r.key)] = r  # latest-dated wins
            obs = observed(table, col)
            for o in obs.itertuples():
                if o.v in covered:
                    continue
                cov = observed_coverage(o.lo, o.hi)
                hit = next((r for p, r in patterns if p.fullmatch(o.v)), None)
                if hit is not None:
                    rows.append(
                        row(table, col, o.v, cov, hit.label, "pattern")
                    )
                elif o.v in old_by_key:
                    rows.append(
                        row(
                            table,
                            col,
                            o.v,
                            cov,
                            old_by_key[o.v].label,
                            "pre1996",
                        )
                    )
                elif col == "ucc" and o.v in stubs:
                    rows.append(
                        row(table, col, o.v, cov, stubs[o.v], "stub_title")
                    )
                elif col == "rtype" and o.v in expn:
                    rows.append(
                        row(table, col, o.v, cov, expn[o.v], "expn_file")
                    )
                elif col == "rtype":
                    rows.append(
                        row(
                            table,
                            col,
                            o.v,
                            cov,
                            f"Record from the {o.v} detailed expenditure (EXPN) file",
                            "expn_file_generic",
                        )
                    )
    return rows


def fill_undocumented(rows: list[dict], stats: dict) -> list[dict]:
    """Add a row for every observed value of a covered column still unlabelled."""
    have = defaultdict(set)
    for r in rows:
        have[(r["id_tabela"], r["nome_coluna"])].add(r["chave"])
    extra = []
    for table in TABLES:
        for a in read_arch(table):
            if a["covered_by_dictionary"] != "yes":
                continue
            col = a["name"]
            obs = observed(table, col)
            if obs.empty:
                stats["empty_columns"].append(f"{table}.{col}")
                continue
            for o in obs.itertuples():
                if o.v not in have[(table, col)]:
                    extra.append(
                        row(
                            table,
                            col,
                            o.v,
                            observed_coverage(o.lo, o.hi),
                            UNDOCUMENTED,
                            "undocumented",
                        )
                    )
    return extra


def ucc_rows() -> list[dict]:
    path = OUTPUT_DIR / "ucc"
    if not path.exists():
        raise SystemExit(
            "run clean_ucc.py first: ucc coverage is read from it"
        )
    t = pads.dataset(path).to_table().to_pandas()
    t["year"] = t["year"].astype(int)
    lo, hi = t["year"].min(), t["year"].max()
    rows = []
    distinct_factors = sorted(t["factor"].dropna().unique(), key=int)
    labels = UCC_LABELS | {
        "factor": {
            f: f"Multiplication factor {f}: the row's value is multiplied by {f} "
            "when it is summed into its group"
            for f in distinct_factors
        }
    }
    for col, mapping in labels.items():
        present = (
            t.dropna(subset=[col]).groupby(col)["year"].agg(["min", "max"])
        )
        for key, label in mapping.items():
            if key not in present.index:
                log.warning(f"ucc.{col}: {key!r} never appears in the data")
                rows.append(row("ucc", col, key, None, label, "stub"))
                continue
            a, b = present.loc[key, "min"], present.loc[key, "max"]
            cov = None if (a, b) == (lo, hi) else coverage(a, b)
            rows.append(row("ucc", col, key, cov, label, "stub"))
        unknown = set(present.index) - set(mapping)
        if unknown:
            raise ValueError(f"ucc.{col}: unlabelled values {unknown}")
    return rows


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    stats = {"empty_columns": []}
    rows = [
        row(t, c, k, cov, v, "labstat")
        for t, c, k, cov, v in labstat_dictionary_rows(LABSTAT_DIR)
    ]
    rows += ucc_rows() + pumd_rows(stats)
    rows += fill_undocumented(rows, stats)
    df = pd.DataFrame(rows)
    key = ["id_tabela", "nome_coluna", "chave", "cobertura_temporal"]
    # Duplicate keys: keep the most recently dated BLS row.
    df["_r"] = df["_rank"].map(lambda r: (-(r[0] or 0), -(r[1] or 0)))
    df = df.sort_values("_r", kind="stable")
    dup = df[df.duplicated(key, keep=False)]
    conflicts = dup.groupby(key)["valor"].nunique()
    conflicts = conflicts[conflicts > 1]
    log.info(
        f"{len(dup)} rows share a key ({dup.drop_duplicates(key).shape[0]} keys); "
        f"label conflicts: {conflicts.index.tolist()}"
    )
    for k in conflicts.index:
        d = dup.set_index(key).loc[[k]]
        log.info(
            f"  {k}: kept {d['valor'].iloc[0]!r}, dropped {d['valor'].iloc[1:].tolist()}"
        )
    df = df.drop_duplicates(key, keep="first")
    summary = (
        df[~df["_rule"].isin(["codes", "flag"])]
        .groupby(["_rule", "id_tabela", "nome_coluna"])
        .size()
        .reset_index(name="rows")
    )
    log.info(
        "rows by non-default rule (flags omitted):\n"
        + summary.to_string(index=False)
    )
    und = df[df["_rule"] == "undocumented"]
    log.info(
        "undocumented values:\n" + und[COLUMNS[:4]].to_string(index=False)
    )
    log.info(f"columns with no values in any year: {stats['empty_columns']}")
    df = df.sort_values(["id_tabela", "nome_coluna", "chave"], kind="stable")
    write_table(
        pa.Table.from_pandas(df[COLUMNS], preserve_index=False),
        "dicionario",
        OUTPUT_DIR,
    )
    log.info(df.groupby("id_tabela").size().to_string())


if __name__ == "__main__":
    main()
