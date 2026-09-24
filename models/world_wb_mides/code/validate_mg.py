"""Gate the MG staging parquet before anyone uploads it. Non-zero exit on failure.

This runs against what `clean_mg.py` wrote on disk, not against BigQuery. Everything it
checks is something that would otherwise be discovered after 8 GB had been uploaded over
27,294 live objects, or not discovered at all:

1.  **Object names.** `<phase>_<year>_<ibge7>.parquet`, flat, no hive partitioning --
    100% of the objects already in the bucket parse against that pattern, and the upload
    replaces by name. A name that does not match either lands beside the history and
    double-counts, or collides and replaces a year that is not the one intended.
2.  **Schema identity.** Column names, order and all-STRING type against the pinned
    staging schema in `clean_mg.SPECS`. A drifted schema does not fail the upload; it
    fails later, inside dbt, as `Parquet column ... does not match the target cpp_type`.
3.  **Municipality census.** 853 per exercise per mirror. A short exercise looks exactly
    like a real one until somebody sums it.
4.  **`id_municipio` against the file name.** For liquidação, pagamento and restos a
    pagar the source CSV carries no municipality at all, so this column is the one thing
    in the mirror with no upstream to check it against. It is the join key of the whole
    pipeline.
5.  **Dates are ISO.** The source publishes `YYYYMMDD`; the models apply
    `safe_cast(data as date)`, which returns NULL for that form. A mirror that reloaded
    verbatim would pass every dbt test with `data` and `mes` empty for every row.
6.  **Values cast to a number**, and **restos a pagar resolve**: the liquidação and
    pagamento models LEFT JOIN this mirror on `id_rsp` to recover the original empenho,
    and a broken join empties `id_empenho_bd` for every restos-a-pagar row.

Usage:
    uv run python models/world_wb_mides/code/validate_mg.py [--year 2022] [--full]
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
# pyrefly: ignore [missing-import]  # sibling module via sys.path
from clean_mg import MIRROR, SPECS, schema_for

# pyrefly: ignore [missing-import]  # sibling module via sys.path
from constants import (
    MG_FIRST_YEAR,
    MG_MUNICIPALITIES,
    OUTPUT_DIR,
)

NAME_RE = re.compile(
    r"^(?P<phase>[a-z]+)_(?P<year>\d{4})_(?P<ibge>\d{7})\.parquet$"
)
ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parsed(path: Path) -> tuple[int, str]:
    """(exercise, IBGE-7) out of an object name.

    Only ever called on names that already matched during the inventory pass, so a miss
    here is a logic error in this file rather than a defect in the data.
    """
    match = NAME_RE.match(path.name)
    if match is None:
        raise AssertionError(f"{path.name} reached the content pass unparsed")
    return int(match.group("year")), match.group("ibge")


# Which staging column carries the exercise, per mirror. Restos a pagar keeps the source
# name; the other three are renamed to `ano`.
YEAR_COLUMN = {
    "empenho": "ano",
    "liquidacao": "ano",
    "pagamento": "ano",
    "rsp": "num_ano_referencia",
}
DATE_COLUMNS = {
    phase: [name for name, kind, _ in SPECS[phase] if kind == "date"]
    for phase in SPECS
}
VALUE_COLUMNS = {
    "empenho": ["valor_empenho_original", "valor_reforco", "valor_anulacao"],
    "liquidacao": ["valor_liquidacao_original", "valor_anulado"],
    "pagamento": [
        "valor_pagamento_original",
        "vlr_ret_fonte",
        "vlr_ant_fonte",
        "vlr_anu_fonte",
    ],
    "rsp": ["valor_original", "valor_processado", "valor_nao_processado"],
}
# Every column the dbt models in models/world_wb_mides/states/ actually read. An
# all-NULL one here is a silent hole in the published table.
MODEL_COLUMNS = {
    "empenho": [
        "ano",
        "mes",
        "data",
        "id_municipio",
        "orgao",
        "id_unidade_gestora",
        "id_licitacao",
        "id_empenho",
        "numero_empenho",
        "descricao",
        "dsc_modalidade",
        "dsc_funcao",
        "dsc_subfuncao",
        "dsc_programa",
        "dsc_acao",
        "elemento_despesa",
        "valor_empenho_original",
        "valor_reforco",
        "valor_anulacao",
    ],
    "liquidacao": [
        "ano",
        "mes",
        "data",
        "id_municipio",
        "orgao",
        "id_unidade_gestora",
        "id_empenho",
        "id_liquidacao",
        "numero_liquidacao",
        "nome_responsavel",
        "documento_responsavel",
        "id_rsp",
        "valor_liquidacao_original",
        "valor_anulado",
    ],
    "pagamento": [
        "ano",
        "mes",
        "data",
        "sigla_uf",
        "id_municipio",
        "orgao",
        "id_unidade_gestora",
        "id_empenho",
        "numero_empenho",
        "id_liquidacao",
        "numero_liquidacao",
        "id_pagamento",
        "numero_pagamento",
        "nome_credor",
        "documento_credor",
        "id_rsp",
        "fonte",
        "valor_pagamento_original",
        "vlr_ret_fonte",
        "vlr_anu_fonte",
    ],
    # Read through the join in the liquidação and pagamento models.
    "rsp": [
        "id_rsp",
        "orgao",
        "id_municipio",
        "num_ano_emp_origem",
        "id_empenho_origem",
        "numero_empenho",
    ],
}
# Share of non-sentinel `id_rsp` values in liquidação and pagamento that must be found in
# the restos a pagar mirror for the same municipality and exercise. Not 100%: a payment
# can settle a commitment carried from an exercise we do not hold, and the historical
# mirrors are full of those at the 2014 boundary.
RSP_JOIN_FLOOR = 0.90


def _floatable(value: str | None) -> bool:
    if value is None:
        return True
    try:
        float(value.strip())
    except ValueError:
        return False
    return True


def main(
    years: set[int] | None = None,
    full: bool = False,
    sample: int = 25,
    tolerate_missing: int = 0,
) -> None:
    failures: list[str] = []
    notes: list[str] = []
    census: dict[tuple[str, int], set[str]] = defaultdict(set)
    files: dict[tuple[str, int], list[Path]] = defaultdict(list)
    # Files whose schema already failed. The content pass reads columns by name, so
    # reading one of these raises instead of reporting -- and a traceback in place of
    # the schema diff is the least useful way to learn the schema drifted.
    quarantined: set[Path] = set()

    print("=== inventory and schema ===")
    for phase, mirror in MIRROR.items():
        directory = OUTPUT_DIR / mirror
        if not directory.is_dir():
            failures.append(f"{mirror}: directory absent")
            continue
        found = sorted(directory.glob("*.parquet"))
        if not found:
            failures.append(f"{mirror}: no parquet written")
            continue
        expected = schema_for(phase)
        stray = sorted(
            p.name for p in directory.iterdir() if p.suffix == ".part"
        )
        if stray:
            failures.append(
                f"{mirror}: {len(stray)} unfinished .part files, e.g. {stray[:3]}"
            )
        bad_names, bad_schema, rows = 0, [], 0
        for path in found:
            match = NAME_RE.match(path.name)
            if not match or match.group("phase") != phase:
                bad_names += 1
                continue
            year, ibge = int(match.group("year")), match.group("ibge")
            if years is not None and year not in years:
                continue
            if not ibge.startswith("31"):
                failures.append(
                    f"{path.name}: IBGE {ibge} is not a Minas Gerais code"
                )
            if not MG_FIRST_YEAR <= year <= 2100:
                failures.append(f"{path.name}: implausible exercise {year}")
            # Footer only: no data is read, so this is affordable over 27,000 objects.
            meta = pq.read_schema(path)
            if meta.names != expected.names or any(
                str(f.type) != "string" for f in meta
            ):
                bad_schema.append(path.name)
                quarantined.add(path)
                missing_cols = [
                    c for c in expected.names if c not in meta.names
                ]
                extra_cols = [c for c in meta.names if c not in expected.names]
                if missing_cols or extra_cols:
                    failures.append(
                        f"{path.name}: schema drift -- missing {missing_cols}, "
                        f"unexpected {extra_cols}"
                    )
            census[(phase, year)].add(ibge)
            files[(phase, year)].append(path)
            rows += pq.read_metadata(path).num_rows
        if bad_names:
            failures.append(
                f"{mirror}: {bad_names} objects do not match <phase>_<year>_<ibge7>.parquet"
            )
        if bad_schema:
            failures.append(
                f"{mirror}: {len(bad_schema)} files do not match the staging schema "
                f"(names, order, all-STRING), e.g. {bad_schema[:3]}"
            )
        print(
            f"  {mirror:20} {len(found):>6} files  {rows:>14,} rows  "
            f"{len(expected.names):>2} cols"
        )

    print(
        f"\n=== municipality census (expected {MG_MUNICIPALITIES} per exercise) ==="
    )
    for phase, year in sorted(census):
        count = len(census[(phase, year)])
        flag = (
            "OK"
            if count == MG_MUNICIPALITIES
            else f"SHORT by {MG_MUNICIPALITIES - count}"
        )
        short = MG_MUNICIPALITIES - count
        if short > 0:
            # Name the missing codes where another mirror of the same exercise has
            # them. A bare count says an exercise is short; the codes say whether one
            # municipality filed nothing that year or the unpack dropped a batch, and
            # only the first of those is the source's doing. The existing bucket has
            # one real instance: pagamento 2016 holds 851, the other three hold 853.
            fuller = max(
                (census[(other, y)] for (other, y) in census if y == year),
                key=len,
                default=set(),
            )
            absent = sorted(fuller - census[(phase, year)])
            if absent:
                flag += (
                    f" (absent here, present in another mirror: {absent[:5]})"
                )
            message = (
                f"{MIRROR[phase]} {year}: {count} municipalities, expected "
                f"{MG_MUNICIPALITIES}"
            )
            if short <= tolerate_missing:
                notes.append(
                    message
                    + f" -- within --tolerate-missing {tolerate_missing}"
                )
            else:
                failures.append(message)
        print(f"  {MIRROR[phase]:20} {year}  {count:>4}  {flag}")

    print("\n=== content ===")
    rsp_keys: dict[tuple[int, str], set[str]] = defaultdict(set)
    for (phase, year), paths in sorted(files.items()):
        if phase != "rsp":
            continue
        for path in paths:
            if path in quarantined:
                continue
            _, ibge = parsed(path)
            rsp_keys[(year, ibge)].update(
                v
                for v in pq.read_table(path, columns=["id_rsp"])
                .column(0)
                .to_pylist()
                if v is not None
            )

    for (phase, year), paths in sorted(files.items()):
        usable = [p for p in paths if p not in quarantined]
        chosen = (
            usable
            if full
            else usable[:: max(1, len(usable) // sample or 1)][:sample]
        )
        checked = 0
        null_rate: dict[str, tuple[int, int]] = {
            c: (0, 0) for c in MODEL_COLUMNS[phase]
        }
        rsp_hits = rsp_misses = 0
        for path in chosen:
            year_in_name, ibge = parsed(path)
            need = sorted(
                set(MODEL_COLUMNS[phase])
                | set(DATE_COLUMNS[phase])
                | set(VALUE_COLUMNS[phase])
                | {"id_municipio", YEAR_COLUMN[phase]}
            )
            table = pq.read_table(path, columns=need)
            checked += 1

            municipios = set(table.column("id_municipio").to_pylist())
            if municipios - {ibge}:
                failures.append(
                    f"{path.name}: id_municipio holds {sorted(municipios)[:3]} but the "
                    f"file name says {ibge}. This is the pipeline's join key."
                )
            exercises = set(table.column(YEAR_COLUMN[phase]).to_pylist()) - {
                None
            }
            if exercises - {str(year_in_name)}:
                failures.append(
                    f"{path.name}: {YEAR_COLUMN[phase]} holds {sorted(exercises)[:3]}, "
                    f"file name says {year_in_name}"
                )
            for column in DATE_COLUMNS[phase]:
                values = table.column(column).to_pylist()
                bad = [
                    v for v in values if v is not None and not ISO_RE.match(v)
                ]
                if bad:
                    failures.append(
                        f"{path.name}: {column} has {len(bad)} non-ISO values, e.g. "
                        f"{bad[:3]}. safe_cast(... as date) returns NULL for those."
                    )
            for column in VALUE_COLUMNS[phase]:
                values = table.column(column).to_pylist()
                bad = [v for v in values if not _floatable(v)]
                if bad:
                    failures.append(
                        f"{path.name}: {column} has {len(bad)} non-numeric values, "
                        f"e.g. {bad[:3]}"
                    )
            for column in MODEL_COLUMNS[phase]:
                values = table.column(column).to_pylist()
                nulls, total = null_rate[column]
                null_rate[column] = (
                    nulls + sum(1 for v in values if v is None),
                    total + len(values),
                )
            if phase in ("liquidacao", "pagamento"):
                known = rsp_keys.get((year_in_name, ibge), set())
                for value in table.column("id_rsp").to_pylist():
                    if value in (None, "-1"):
                        continue
                    if value in known:
                        rsp_hits += 1
                    else:
                        rsp_misses += 1

        empty = [c for c, (n, t) in null_rate.items() if t and n == t]
        if empty:
            failures.append(
                f"{MIRROR[phase]} {year}: columns entirely NULL across {checked} files: "
                f"{empty}. The dbt model reads every one of them."
            )
        worst = sorted(
            ((n / t, c) for c, (n, t) in null_rate.items() if t), reverse=True
        )[:2]
        detail = ", ".join(f"{c} {r:.1%} null" for r, c in worst if r)
        line = f"  {MIRROR[phase]:20} {year}  {checked:>4} files checked"
        if detail:
            line += f"   {detail}"
        if phase in ("liquidacao", "pagamento") and (rsp_hits + rsp_misses):
            share = rsp_hits / (rsp_hits + rsp_misses)
            line += f"   rsp join {share:.1%}"
            if share < RSP_JOIN_FLOOR:
                notes.append(
                    f"{MIRROR[phase]} {year}: only {share:.1%} of non-sentinel id_rsp "
                    f"resolve in raw_rsp_mg ({rsp_misses:,} misses). Expected below "
                    f"{RSP_JOIN_FLOOR:.0%} only where the originating exercise is not "
                    "held; the models empty id_empenho_bd for every miss."
                )
        print(line)

    if notes:
        print("\n=== reported, not enforced ===")
        for note in notes:
            print(f"  - {note}")
    if failures:
        print(f"\nFAILED: {len(failures)} problems")
        for failure in failures[:40]:
            print(f"  - {failure}")
        if len(failures) > 40:
            print(f"  ... and {len(failures) - 40} more")
        raise SystemExit(1)
    print("\nMG validation PASSED")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, action="append")
    parser.add_argument(
        "--full",
        action="store_true",
        help="read every file, not a sample. Schema and census always cover every file; "
        "this extends the row-level checks to all of them.",
    )
    parser.add_argument("--sample", type=int, default=25)
    parser.add_argument(
        "--tolerate-missing",
        type=int,
        default=0,
        help="accept up to N municipalities short of 853 in an exercise, reported "
        "rather than failed. Use only after looking at which codes are absent.",
    )
    args = parser.parse_args()
    main(
        years=set(args.year) if args.year else None,
        full=args.full,
        sample=args.sample,
        tolerate_missing=args.tolerate_missing,
    )
