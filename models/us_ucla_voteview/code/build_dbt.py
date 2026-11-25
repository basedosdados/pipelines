"""Write the dbt models and schema.yml for us_ucla_voteview from the architecture.

Usage:
    uv run --no-sync python models/us_ucla_voteview/code/build_dbt.py

Column order, casts and descriptions come from ``architecture/*.csv``; the tests
are declared here.
"""

import csv
from pathlib import Path

DS = "us_ucla_voteview"
CODE = Path(__file__).parent
MODELS = CODE.parent
TABLES = ["members", "parties", "rollcalls", "votes", "dictionary"]
PARTITIONED = {"rollcalls", "votes"}

DESCRIPTIONS = {
    "members": (
        "Members of the US Congress and presidents, one row per member, chamber "
        "and Congress, with biographical fields and DW-NOMINATE and Nokken-Poole "
        "ideal-point estimates."
    ),
    "parties": (
        "Congressional parties, one row per party, chamber and Congress, with the "
        "number of members and the mean and median DW-NOMINATE scores."
    ),
    "rollcalls": (
        "Roll-call votes of the US House and Senate, one row per roll call, with "
        "the result, the vote counts and the DW-NOMINATE midpoint and spread."
    ),
    "votes": (
        "How each member of the US Congress voted in each roll call, one row per "
        "member and roll call, with the NOMINATE probability of the recorded vote."
    ),
    "dictionary": "Labels of the coded columns of the us_ucla_voteview tables.",
}

# The congress_member directory is a static snapshot, so members who enter
# Congress after it was built (three special-election winners of 2026 at
# onboarding) have no row there yet. Warn rather than fail the weekly pipeline.

# Scope the 26M-row votes table's tests to the last two years, so the weekly
# pipeline does not rescan the full history every run.
RECENT = "date >= date_sub(current_date(), interval 2 year)"

TESTS = {
    "members": [
        ("unique", ["congress", "chamber", "icpsr"]),
        ("nonnull", ["conditional"]),
        ("dict", ["last_means", "conditional"]),
    ],
    "parties": [
        ("unique", ["congress", "chamber", "party_code"]),
        ("nonnull", []),
    ],
    "rollcalls": [
        ("unique", ["congress", "chamber", "rollnumber"]),
        ("nonnull", []),
    ],
    "votes": [
        ("unique", ["congress", "chamber", "rollnumber", "icpsr"]),
        ("nonnull", []),
        ("dict", ["cast_code"]),
    ],
    "dictionary": [("unique", ["table_id", "column_name", "key"])],
}

COLUMN_TESTS = {
    ("members", "congress"): ["  - not_null"],
    ("members", "icpsr"): ["  - not_null"],
    ("members", "state_id"): [
        "  - relationships:",
        "      to: ref('br_bd_diretorios_us__state')",
        "      field: id_state",
    ],
    ("members", "bioguide_id"): [
        "  - relationships:",
        "      to: ref('br_bd_diretorios_us__congress_member')",
        "      field: id_bioguide",
        "      config:",
        "        severity: warn",
    ],
    ("members", "party_code"): [
        "  - relationships:",
        f"      to: ref('{DS}__parties')",
        "      field: party_code",
    ],
    ("parties", "congress"): ["  - not_null"],
    ("rollcalls", "congress"): ["  - not_null"],
    ("rollcalls", "date"): ["  - not_null"],
    ("votes", "congress"): [
        "  - not_null:",
        "      config:",
        f"        where: {RECENT}",
    ],
    ("votes", "date"): ["  - not_null"],
    ("votes", "icpsr"): [
        "  - relationships:",
        f"      to: ref('{DS}__members')",
        "      field: icpsr",
        "      config:",
        f"        where: {RECENT}",
    ],
}


def arch(table):
    with open(CODE / "architecture" / f"{table}.csv") as f:
        return list(csv.DictReader(f))


def sql(table):
    cols = arch(table)
    part = ""
    if table in PARTITIONED:
        part = (
            "        partition_by={\n"
            '            "field": "congress",\n'
            '            "data_type": "int64",\n'
            '            "range": {"start": 1, "end": 130, "interval": 1},\n'
            "        },\n"
        )
    body = ",\n".join(
        f"    safe_cast({c['name']} as {c['bigquery_type'].lower()}) {c['name']}"
        for c in cols
    )
    return (
        "{{\n    config(\n"
        f'        alias="{table}",\n        schema="{DS}",\n'
        f'        materialized="table",\n{part}    )\n}}}}\n'
        f"select\n{body}\n"
        f'from {{{{ set_datalake_project("{DS}_staging.{table}") }}}} as t\n'
    )


def yml_desc(text, indent):
    pad = " " * indent
    words, lines, cur = text.split(), [], ""
    for w in words:
        if cur and len(cur) + 1 + len(w) > 80 - indent:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    lines.append(cur)
    return ">\n" + "\n".join(pad + ln for ln in lines)


def model_tests(table):
    out = []
    for kind, arg in TESTS[table]:
        where = (
            ["      config:", f"        where: {RECENT}"]
            if table == "votes"
            else []
        )
        if kind == "unique":
            out += [
                "  - dbt_utils.unique_combination_of_columns:",
                f"      combination_of_columns: [{', '.join(arg)}]",
                *where,
            ]
        elif kind == "nonnull":
            out += [
                "  - not_null_proportion_multiple_columns:",
                "      at_least: 0.05",
            ]
            if arg:
                out.append(f"      ignore_values: [{', '.join(arg)}]")
            out += where
        elif kind == "dict":
            out += [
                "  - custom_dictionary_coverage_eng:",
                f"      dictionary_model: ref('{DS}__dictionary')",
                "      columns_covered_by_dictionary:",
                *[f"        - {c}" for c in arg],
                *where,
            ]
    return out


def schema():
    lines = ["---", "version: 2", "models:"]
    for table in TABLES:
        lines += [
            f"  - name: {DS}__{table}",
            f"    description: {yml_desc(DESCRIPTIONS[table], 6)}",
            "    tests:",
            *["    " + t for t in model_tests(table)],
            "    columns:",
        ]
        for c in arch(table):
            lines += [
                f"      - name: {c['name']}",
                f"        description: {yml_desc(c['description'], 10)}",
            ]
            tests = COLUMN_TESTS.get((table, c["name"]))
            if tests:
                lines += ["        tests:", *["        " + t for t in tests]]
    return "\n".join(lines) + "\n"


def main():
    for table in TABLES:
        (MODELS / f"{DS}__{table}.sql").write_text(sql(table))
    (MODELS / "schema.yml").write_text(schema())
    print(f"wrote {len(TABLES)} models and schema.yml to {MODELS}")


if __name__ == "__main__":
    main()
