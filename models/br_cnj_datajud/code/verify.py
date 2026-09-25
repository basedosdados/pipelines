"""Verify the cleaned parquet against the raw CSV.

Checks the properties a dbt test cannot reach because they compare against the
source file rather than against the loaded table.
"""

from __future__ import annotations

import collections
import sys

import pyarrow.dataset as ds
from clean import (
    INPUT_DIR,
    OUTPUT_DIR,
    RAMO_NORMALISATION,
    SENTINELS,
    clean_value,
    read_source,
)

failures: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    print(
        f"  [{'PASS' if ok else 'FAIL'}] {label}{(' — ' + detail) if detail else ''}"
    )
    if not ok:
        failures.append(label)


def main() -> int:
    header, rows = read_source(sorted(INPUT_DIR.glob("JN_*.csv"))[-1])
    lower = [h.lower() for h in header]

    fact = (
        ds.dataset(OUTPUT_DIR / "tribunal_ano", partitioning="hive")
        .to_table()
        .to_pydict()
    )
    trib = (
        ds.dataset(OUTPUT_DIR / "tribunal", partitioning="hive")
        .to_table()
        .to_pydict()
    )
    dic = ds.dataset(OUTPUT_DIR / "dicionario").to_table().to_pydict()

    print("\n== row counts ==")
    expected_grid = len(rows) * len(dic["chave"])
    populated = sum(
        1
        for r in rows
        for j, name in enumerate(lower)
        if name in set(dic["chave"]) and clean_value(r[j]) is not None
    )
    check(
        "fact rows == populated source cells",
        len(fact["valor"]) == populated,
        f"{len(fact['valor']):,} vs {populated:,} (grid {expected_grid:,})",
    )
    check(
        "tribunal rows == source rows",
        len(trib["sigla_tribunal"]) == len(rows),
        f"{len(trib['sigla_tribunal']):,} vs {len(rows):,}",
    )

    print("\n== key uniqueness ==")
    key = collections.Counter(
        zip(
            fact["ano"],
            fact["sigla_tribunal"],
            fact["ramo_justica"],
            fact["sigla_indicador"],
            strict=True,
        )
    )
    dups = [k for k, c in key.items() if c > 1]
    check(
        "(ano, sigla_tribunal, ramo_justica, sigla_indicador) unique",
        not dups,
        f"{len(dups)} duplicates",
    )

    narrow = collections.Counter(
        zip(
            fact["ano"],
            fact["sigla_tribunal"],
            fact["sigla_indicador"],
            strict=True,
        )
    )
    collisions = sum(1 for c in narrow.values() if c > 1)
    check(
        "ramo_justica is load-bearing in the key",
        collisions > 0,
        f"{collisions} rows would collide without it — dropping it would lose data",
    )

    tkey = collections.Counter(
        zip(
            trib["ano"],
            trib["sigla_tribunal"],
            trib["ramo_justica"],
            strict=True,
        )
    )
    check("tribunal key unique", not [k for k, c in tkey.items() if c > 1])

    print("\n== value fidelity (spot check vs raw CSV) ==")
    index = {
        (
            r[lower.index("ano")],
            r[lower.index("sigla")],
            RAMO_NORMALISATION.get(r[1].strip(), r[1].strip()),
        ): r
        for r in rows
    }
    mismatches = 0
    checked = 0
    for i in range(0, len(fact["valor"]), max(1, len(fact["valor"]) // 5000)):
        # `ano` comes back from the hive partition key as an int.
        row = index[
            (
                str(fact["ano"][i]),
                fact["sigla_tribunal"][i],
                fact["ramo_justica"][i],
            )
        ]
        raw = row[lower.index(fact["sigla_indicador"][i])]
        checked += 1
        if clean_value(raw) != fact["valor"][i]:
            mismatches += 1
    check(
        "sampled values match source",
        mismatches == 0,
        f"{checked:,} sampled, {mismatches} mismatched",
    )

    print("\n== sentinels and parsing ==")
    check(
        "no sentinel leaked into valor",
        not [
            v
            for v in fact["valor"]
            if v is None or v.strip().lower() in SENTINELS
        ],
    )
    check(
        "every valor parses as a float",
        all(_is_float(v) for v in fact["valor"]),
    )
    check(
        "no comma decimal survived", not [v for v in fact["valor"] if "," in v]
    )

    print("\n== normalisation ==")
    ramos = set(fact["ramo_justica"])
    check(
        "'Militar Uniao' normalised away",
        "Militar Uniao" not in ramos,
        f"ramos={sorted(ramos)}",
    )
    check("sigla_uf_sede has no 'BR'", "BR" not in set(trib["sigla_uf_sede"]))

    print("\n== dictionary coverage ==")
    check(
        "every sigla_indicador is in dicionario",
        set(fact["sigla_indicador"]) <= set(dic["chave"]),
        f"{len(set(fact['sigla_indicador']))} used / {len(dic['chave'])} defined",
    )
    check("no dictionary label is empty", all(v.strip() for v in dic["valor"]))

    print("\n== coverage ==")
    print(
        f"  anos {min(fact['ano'])}-{max(fact['ano'])}, "
        f"{len(set(fact['sigla_tribunal']))} tribunals, "
        f"{len(set(fact['sigla_indicador']))} indicators"
    )

    print(
        "\n"
        + (
            "ALL CHECKS PASSED"
            if not failures
            else f"{len(failures)} FAILED: {failures}"
        )
    )
    return 1 if failures else 0


def _is_float(v: str) -> bool:
    try:
        float(v)
        return True
    except (TypeError, ValueError):
        return False


if __name__ == "__main__":
    sys.exit(main())
