"""Parser for the ABS Estimates of Industry Multifactor Productivity (5260.0.55.002).

Unlike the Australian System of National Accounts, this release is **not** an ABS
time-series workbook: it carries no Series IDs, and its 42 tables are
hand-formatted with a hierarchical row stub in column A. Two source conventions
make a single parser possible:

1. **Indentation encodes nesting**, using non-breaking spaces (U+00A0), two per
   level. A row carrying no numeric cells is a heading; a row carrying them is
   data.
2. **Two logical levels share indent 0**, and nothing in the file distinguishes
   them: Table 2 puts the block title ("Indexes of productivity and related
   measures") and its sub-group ("Productivity indexes") at the same indent, with
   no blank row between and inconsistent bolding — the same sub-group label is
   bold in the first block and not in the second. Collapsing the two levels would
   map the index *levels* and their *percentage changes* onto one key, so the
   sub-groups are named explicitly in ``NEST_AT_ROOT`` below. Every other indent-0
   heading opens a new ``section``.

   ``NEST_AT_ROOT`` is an allow-list derived from all 107 distinct indent-0
   headings in the 2024-25 release. If a later release adds a sub-group that is
   not listed, two series collide on one key and the duplicate check in ``main``
   fails the run — loudly, rather than silently overwriting data.

The column axis comes in three shapes, unified into two fact tables:

| Source shape | Tables | Indicator is | Fact table |
|---|---|---|---|
| Financial-year columns | 39 tables | the row path | ``observations`` |
| Growth-cycle span columns | 3, 5 | the row path | ``growth_cycles`` |
| Measure columns, span rows | 26 | row path minus its leaf, plus the column label | ``growth_cycles`` |

Output (all-STRING parquet, per the repo's BigQuery staging convention):

  indicator/indicator.parquet        one row per indicator (the dimension)
  observations/year=YYYY/...         long annual fact
  growth_cycles/growth_cycles.parquet long growth-cycle fact

Usage:
    python clean_data.py <input_dir_with_xlsx> <output_dir>
"""

import collections
import glob
import hashlib
import os
import re
import shutil
import sys

# pyrefly: ignore [untyped-import]
import openpyxl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

FY = re.compile(r"^\d{4}-\d{2}$")
SPAN = re.compile(r"^(\d{4}-\d{2})\s+to\s+(\d{4}-\d{2})")
ANZSIC = re.compile(r"^([A-S])\s+(\S.*)$")
FOOTNOTE = re.compile(r"\s*\([a-z]\)\s*$")

# Indent-0 headings that are sub-groups of the enclosing block rather than a new
# block. See the module docstring: the source gives no structural signal, so this
# is an explicit list checked against the release's 107 distinct indent-0 labels.
NEST_AT_ROOT = re.compile(
    r"^(productivity indexes|output measures|input measures"
    r"|contribution (to|of) )",
    re.IGNORECASE,
)

# ABS data-quality legend rows sit below the data and carry no numbers, so they
# would otherwise be read as headings ("na not available", "np not available for
# publication ...").
LEGEND = re.compile(r"^(na|np|nya|nec|nfd)\b", re.IGNORECASE)

# Tables 1-19 are the official estimates; the ABS publishes 20-42 as experimental.
TABLE_GROUPS = [
    (19, "standard"),
    (26, "experimental_industry"),
    (42, "experimental_state"),
]

# ANZSIC 2006 division names are spelled inconsistently across the workbooks
# ("Rental, hiring and real estate Services" in some tables, "services" in
# others). The division letter is authoritative; these are the canonical names.
INDUSTRY_NAMES = {
    "A": "Agriculture, forestry and fishing",
    "B": "Mining",
    "C": "Manufacturing",
    "D": "Electricity, gas, water and waste services",
    "E": "Construction",
    "F": "Wholesale trade",
    "G": "Retail trade",
    "H": "Accommodation and food services",
    "I": "Transport, postal and warehousing",
    "J": "Information, media and telecommunications",
    "K": "Financial and insurance services",
    "L": "Rental, hiring and real estate services",
    "M": "Professional, scientific and technical services",
    "N": "Administrative and support services",
    "R": "Arts and recreation services",
    "S": "Other services",
}

STATES = {
    "New South Wales": "1",
    "Victoria": "2",
    "Queensland": "3",
    "South Australia": "4",
    "Western Australia": "5",
    "Tasmania": "6",
    "Northern Territory": "7",
    "Australian Capital Territory": "8",
}


def _clean_label(raw: str) -> str:
    """Normalise a row-stub label: NBSP -> space, drop footnote markers."""
    txt = raw.replace("\xa0", " ").strip()
    txt = re.sub(r"\s+", " ", txt)
    while FOOTNOTE.search(txt):
        txt = FOOTNOTE.sub("", txt)
    return txt


def _indent(raw: str) -> int:
    """Leading-whitespace width; ABS indents two NBSP per level."""
    s = raw.replace("\xa0", " ")
    return len(s) - len(s.lstrip())


def _table_group(table_no: int) -> str:
    for ceiling, group in TABLE_GROUPS:
        if table_no <= ceiling:
            return group
    raise ValueError(f"table {table_no} out of range")


def _fy_year(fy: str) -> int:
    """ "2024-25" -> 2025, the calendar year the financial year ends in."""
    head, tail = fy.split("-")
    return int(head[:2] + tail) if int(tail) > int(head[2:]) else int(head) + 1


def _unit(section: str, title: str, path: list) -> str | None:
    """Derive the unit from the row path, the block heading and the table title.

    The ABS states the unit only in prose, and a single table mixes units across
    its blocks (Table 2 carries index levels, their percentage changes, growth
    contributions and income shares). Order matters: a row whose own label is a
    growth rate is a percentage change even inside a contributions block, where
    it is the total the contributions sum to.
    """
    item = (path[-1] if path else "").lower()
    ctx = " | ".join([section or "", *path]).lower()
    ttl = (title or "").lower()

    if item.endswith("growth") or item.endswith("growth rate"):
        return "Percentage change"
    if "contribution" in ctx or "contribution" in ttl:
        return "Percentage point contribution"
    if any(s in ctx for s in ("income share", "cost share", "input shares")):
        return "Proportion"
    if "rental price" in ttl:
        return "$ per unit of capital"
    if "$million" in ttl or "$ million" in ttl:
        return "$ million"
    if "percentage change" in ctx:
        return "Percentage change"
    if "index" in ctx or "index" in ttl:
        return "Index"
    return None


def _parse_industry(candidates: list):
    """Return (division letter, canonical name) from the first ANZSIC-shaped label."""
    for c in candidates:
        m = ANZSIC.match(c)
        if not m:
            continue
        letter = m.group(1)
        if letter in INDUSTRY_NAMES and len(c) > 4:
            return letter, INDUSTRY_NAMES[letter]
    return None, None


def _parse_state(candidates: list):
    for c in candidates:
        for name, code in STATES.items():
            if name.lower() in c.lower():
                return code, name
    return None, None


def _parse_basis(candidates: list):
    hay = " | ".join(candidates).lower()
    if "quality adjusted hours worked" in hay:
        return "Quality adjusted hours worked"
    if "hours worked basis" in hay:
        return "Hours worked"
    return None


def _header_row(rows):
    """Locate the column-header row and classify the column axis."""
    for i, r in enumerate(rows[:12]):
        vals = [
            (j, _clean_label(str(c)))
            for j, c in enumerate(r)
            if j > 0 and c not in (None, "")
        ]
        if not vals:
            continue
        labels = [v for _, v in vals]
        if all(FY.match(v) for v in labels):
            return i, "YEARS", dict(vals)
        if any(SPAN.match(v) for v in labels):
            return i, "SPANS", dict(vals)
        if len(vals) >= 3 and r[0] in (None, "") and i >= 5:
            return i, "MEASURES", dict(vals)
    return None, None, {}


def parse_sheet(ws, table_no: int, title: str):
    """Walk one table sheet; return (indicator rows, annual rows, cycle rows)."""
    cells = [list(r) for r in ws.iter_rows()]
    rows = [[c.value for c in r] for r in cells]
    # Bold on the stub cell. Tables 21-23 have no indentation at all and mark the
    # component a block of industries belongs to with a bold data row, so bold is
    # load-bearing there. It is *not* a reliable marker of heading level (Table 2
    # bolds a sub-group in one block and not in the next), which is why the
    # heading levels come from NEST_AT_ROOT instead.
    bolds = [bool(r[0].font.bold) if r and r[0].font else False for r in cells]
    hdr_i, kind, col_labels = _header_row(rows)
    if hdr_i is None:
        raise ValueError(f"table {table_no}: no column header row found")

    # The in-sheet title spans one or two rows and, unlike the Contents entry,
    # usually states the unit ("..., $million") or the transformation
    # ("Contributions to Growth"). Used only as a unit hint.
    sheet_title = " ".join(
        _clean_label(r[0])
        for r in rows[3:6]
        if r
        and isinstance(r[0], str)
        and r[0].strip()
        and not r[0].strip().startswith("Released at")
    )

    indicators, annual, cycles = {}, [], []
    # Each entry is [indent, label, child_indent, is_bold_group]; child_indent is
    # the indent at which this heading's data rows appear, learned from its first
    # data row, and is_bold_group marks a group opened by a bold data row.
    stack: list = []
    section = None

    for r, bold in zip(rows[hdr_i + 1 :], bolds[hdr_i + 1 :], strict=True):
        a = r[0]
        nums = {
            j: float(c)
            for j, c in enumerate(r)
            if j in col_labels and isinstance(c, (int, float))
        }
        if not isinstance(a, str) or not a.strip():
            continue

        label = _clean_label(a)
        if (
            not label
            or label.startswith("©")
            or re.match(r"^\([a-z]\)", label)
            or LEGEND.match(label)
        ):
            continue
        indent = _indent(a)

        if not nums:  # heading
            if indent == 0 and not NEST_AT_ROOT.match(label):
                section, stack = label, []
            else:
                while stack and stack[-1][0] >= indent:
                    stack.pop()
                stack.append([indent, label, None, False])
            continue

        # Close whatever the data row has moved out of. Anything deeper than the
        # row is always closed. At the row's own indent it is ambiguous, and two
        # source conventions resolve it: Table 2 indents "Labour productivity"'s
        # children two further, so a data row back at its indent ("Capital
        # productivity") is a sibling and closes it, while Tables 21-23 do not
        # indent at all and a bold data row groups the plain rows beneath it, so
        # only another bold row closes that group.
        while stack and stack[-1][0] > indent:
            stack.pop()
        while (
            stack
            and stack[-1][0] == indent
            and (
                (stack[-1][3] and bold)
                or (
                    not stack[-1][3]
                    and stack[-1][2] is not None
                    and stack[-1][2] > indent
                )
            )
        ):
            stack.pop()
        if stack and stack[-1][2] is None:
            stack[-1][2] = indent
        path = [s[1] for s in stack] + [label]

        for j, value in sorted(nums.items()):
            col = col_labels[j]
            if kind == "MEASURES":
                # Table 26: the period is the row leaf, the measure the column.
                span, ind_path = path[-1], [*path[:-1], col]
            else:
                span, ind_path = (col if kind == "SPANS" else None), path

            item_path = " > ".join(ind_path)
            key = f"{table_no}|{section or ''}|{item_path}"
            indicator_id = hashlib.sha1(key.encode()).hexdigest()[:16]

            if indicator_id not in indicators:
                fields = ([section] if section else []) + ind_path + [title]
                industry_code, industry_name = _parse_industry(fields)
                state_id, state_name = _parse_state(fields)
                indicators[indicator_id] = {
                    "indicator_id": indicator_id,
                    "table_no": str(table_no),
                    "industry_code": industry_code,
                    "state_id": state_id,
                    "table_name": title,
                    "table_group": _table_group(table_no),
                    "section": section,
                    "item": ind_path[-1],
                    "item_path": item_path,
                    "industry_name": industry_name,
                    "state_name": state_name,
                    "basis": _parse_basis(ind_path),
                    "unit": _unit(
                        section or "", sheet_title or title, ind_path
                    ),
                }

            if kind == "YEARS":
                annual.append(
                    {
                        "year": _fy_year(col),
                        "financial_year": col,
                        "indicator_id": indicator_id,
                        "value": value,
                    }
                )
            else:
                m = SPAN.match(span or "")
                cycles.append(
                    {
                        "indicator_id": indicator_id,
                        "period": span,
                        "period_start_financial_year": m.group(1)
                        if m
                        else None,
                        "period_end_financial_year": m.group(2) if m else None,
                        "value": value,
                    }
                )

        if bold:
            # This row is also the group the plain rows below it belong to.
            stack.append([indent, label, None, True])

    return list(indicators.values()), annual, cycles


def parse_workbook(path: str):
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    titles = {}
    ws = wb["Contents"]
    for r in ws.iter_rows(values_only=True):
        cells = [c for c in r if c not in (None, "")]
        if len(cells) >= 2 and isinstance(cells[0], (int, float)):
            titles[int(cells[0])] = _clean_label(str(cells[1]))

    inds, ann, cyc = [], [], []
    for sh in [s for s in wb.sheetnames if s.startswith("Table ")]:
        table_no = int(sh.split()[1])
        title = titles.get(table_no)
        if title is None:
            raise ValueError(f"{sh}: no title in the Contents sheet")
        i, a, c = parse_sheet(wb[sh], table_no, title)
        inds.extend(i)
        ann.extend(a)
        cyc.extend(c)
    wb.close()
    return inds, ann, cyc


def _all_string(df: pd.DataFrame, cols: list) -> pa.Table:
    """Cast to an all-STRING arrow table with a stable column order.

    Staging is all-STRING by house convention and the dbt model safe_casts each
    column, so the schema carries order, not types. The cast goes through arrow
    rather than ``astype(str)``, which would render NULL as the literal "nan".
    """
    schema = pa.schema([(c, pa.string()) for c in cols])
    tbl = pa.Table.from_pandas(df[cols], preserve_index=False)
    return tbl.cast(schema)


def main(input_dir: str, output_dir: str):
    files = sorted(glob.glob(os.path.join(input_dir, "*.xlsx")))
    if not files:
        raise SystemExit(f"no .xlsx found in {input_dir}")
    print(f"Parsing {len(files)} workbooks")

    all_inds, all_ann, all_cyc = [], [], []
    for f in files:
        i, a, c = parse_workbook(f)
        print(
            f"  {os.path.basename(f)}: {len(i)} indicators, "
            f"{len(a)} annual, {len(c)} cycle"
        )
        all_inds.extend(i)
        all_ann.extend(a)
        all_cyc.extend(c)

    ind = pd.DataFrame(all_inds)
    ann = pd.DataFrame(all_ann)
    cyc = pd.DataFrame(all_cyc)

    # pyrefly: ignore [unnecessary-type-conversion]
    dup = int(ind["indicator_id"].duplicated().sum())
    ind = ind.drop_duplicates(subset="indicator_id").reset_index(drop=True)

    # First/last financial year per indicator, from the annual fact.
    span = ann.groupby("indicator_id")["year"].agg(["min", "max"])
    fy = ann.drop_duplicates("year").set_index("year")["financial_year"]
    ind["first_financial_year"] = ind["indicator_id"].map(span["min"]).map(fy)
    ind["last_financial_year"] = ind["indicator_id"].map(span["max"]).map(fy)

    # ---- validation ----
    ann_dup = int(ann.duplicated(subset=["year", "indicator_id"]).sum())
    cyc_dup = int(cyc.duplicated(subset=["indicator_id", "period"]).sum())
    # pyrefly: ignore [unnecessary-type-conversion]
    orphan_a = int((~ann["indicator_id"].isin(ind["indicator_id"])).sum())
    # pyrefly: ignore [unnecessary-type-conversion]
    orphan_c = int((~cyc["indicator_id"].isin(ind["indicator_id"])).sum())
    print(
        f"\nindicators:        {len(ind)}  (collapsed {dup} cross-table repeats)"
    )
    print(
        f"annual obs:        {len(ann)}  duplicate (year, indicator_id): {ann_dup}"
    )
    print(
        f"growth-cycle obs:  {len(cyc)}  duplicate (indicator_id, period): {cyc_dup}"
    )
    print(f"orphan fact rows:  annual {orphan_a}, cycle {orphan_c}")
    print(f"year range:        {ann['year'].min()}..{ann['year'].max()}")
    print(f"tables covered:    {ind['table_no'].nunique()} of 42")
    print(f"industries:        {ind['industry_name'].nunique()} distinct")
    print(f"states:            {ind['state_name'].nunique()} distinct")
    print(
        f"units:             {dict(collections.Counter(ind['unit'].fillna('(none)')))}"
    )
    missing_unit = ind[ind["unit"].isna()]
    if len(missing_unit):
        print(f"\n{len(missing_unit)} indicators with no unit derived:")
        for _, row in missing_unit.head(15).iterrows():
            print(
                f"    T{row['table_no']}: {row['section']} :: {row['item_path']}"
            )
    if ann_dup or cyc_dup or orphan_a or orphan_c:
        raise SystemExit("FAILED validation: duplicate or orphan fact rows")

    # ---- write ----
    # pyarrow's write_to_dataset appends a randomly-named file per run, so a
    # re-run would leave the previous run's partition files in place and the
    # upload would double-count every row. Clear the tables first.
    os.makedirs(output_dir, exist_ok=True)
    for name in ("indicator", "observations", "growth_cycles"):
        shutil.rmtree(os.path.join(output_dir, name), ignore_errors=True)
    ind_cols = [
        "indicator_id",
        "table_no",
        "industry_code",
        "state_id",
        "table_name",
        "table_group",
        "section",
        "item",
        "item_path",
        "industry_name",
        "state_name",
        "basis",
        "unit",
        "first_financial_year",
        "last_financial_year",
    ]
    ind_dir = os.path.join(output_dir, "indicator")
    os.makedirs(ind_dir, exist_ok=True)
    pq.write_table(
        _all_string(ind, ind_cols),
        os.path.join(ind_dir, "indicator.parquet"),
        compression="snappy",
    )

    ann_cols = ["year", "financial_year", "indicator_id", "value"]
    pq.write_to_dataset(
        _all_string(ann, ann_cols),
        root_path=os.path.join(output_dir, "observations"),
        partition_cols=["year"],
        compression="snappy",
    )

    cyc_cols = [
        "indicator_id",
        "period",
        "period_start_financial_year",
        "period_end_financial_year",
        "value",
    ]
    cyc_dir = os.path.join(output_dir, "growth_cycles")
    os.makedirs(cyc_dir, exist_ok=True)
    pq.write_table(
        _all_string(cyc, cyc_cols),
        os.path.join(cyc_dir, "growth_cycles.parquet"),
        compression="snappy",
    )
    print(
        f"\nWrote indicator, observations/year=*/ and growth_cycles to {output_dir}"
    )


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
