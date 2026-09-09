"""Check every mapped cell against the cleaned parquet before publishing it.

    python verify_measures.py

For each measure and form version, reports how many reports on that form carry
a value at the mapped address. A measure the mapping places in the wrong cell
shows up as a coverage of zero, which a mapping taken on trust would not.
Writes ``measured.json``, which ``gen_architecture.py`` reads to publish the
temporal coverage the data actually shows rather than the coverage the sources
claim.
"""

import json

from common import OUTPUT, connect
from measures import FORM_1996, FORM_2010, MEASURES

FORM_TABLE = {FORM_2010: "2552-10", FORM_1996: "2552-96"}


def main() -> None:
    """Report per-measure coverage and write measured.json."""
    # Spill outside the repository: duckdb's default `.tmp/` is relative to
    # the working directory, which here is inside the checkout.
    con = connect(OUTPUT.parent)
    con.execute(
        f"create view rv as select * from read_parquet('{OUTPUT}/report_value/*/*.parquet')"
    )
    con.execute(
        f"create view rp as select * from read_parquet('{OUTPUT}/report/*/*.parquet')"
    )
    denom = dict(
        con.execute(
            "select form_version, count(*) from rp group by 1"
        ).fetchall()
    )
    print("reports per form:", denom, "\n")

    measured: dict[str, dict] = {}
    problems: list[str] = []
    for m in MEASURES:
        row: dict[str, object] = {}
        for form, cells in m.by_form.items():
            preds = " or ".join(
                f"(worksheet_code = '{c.worksheet}' and column_number = '{c.column}'"
                + (
                    f" and line_number between '{c.line}' and '{c.line_end}')"
                    if c.line_end
                    else f" and line_number = '{c.line}')"
                )
                for c in cells
            )
            col = "numeric_value" if m.kind == "numeric" else "alpha_value"
            n, ymin, ymax = con.execute(f"""
                select count(distinct report_id), min(year), max(year)
                from rv where form_version = '{form}' and {col} is not null and ({preds})
            """).fetchone() or (0, None, None)
            share = n / denom.get(form, 1)
            row[form] = {
                "reports": n,
                "share": round(share, 4),
                "year_min": ymin,
                "year_max": ymax,
            }
            flag = (
                "  <-- ZERO"
                if n == 0
                else ("  <-- thin" if share < 0.02 else "")
            )
            print(
                f"{m.name:<42} {form}  {n:>8,}  {share:6.1%}  {ymin}-{ymax}{flag}"
            )
            if n == 0:
                problems.append(f"{m.name} [{form}]")
        measured[m.name] = row

    (OUTPUT.parent / "measured.json").write_text(
        json.dumps(measured, indent=1)
    )
    with open("measured.json", "w") as fh:
        json.dump(measured, fh, indent=1)
    print(f"\n{len(problems)} measure/form pairs with zero coverage")
    for p in problems:
        print("  ", p)


if __name__ == "__main__":
    main()
