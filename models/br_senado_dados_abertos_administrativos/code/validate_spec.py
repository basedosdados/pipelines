"""Check architecture_spec.py against the repo's house rules.

Run before generating dbt or registering metadata:
  uv run python models/br_senado_dados_abertos_administrativos/code/validate_spec.py
"""

from __future__ import annotations

import re
import sys

# pyrefly: ignore [missing-import]
from architecture_spec import TABLES

NUMERIC = {"int", "float"}
# Temporal columns that legitimately lead a table, ahead of its key columns.
LEADING = {"data_extracao", "ano", "mes", "data", "data_referencia"}
VALID_TYPES = {"str", "int", "float", "date", "datetime"}
SNAKE = re.compile(r"^[a-z][a-z0-9_]*$")
ACCENTS = re.compile(r"[^\x00-\x7f]")

# Composite keys whose components legitimately span the row: the trailing
# component is a natural attribute of the entity, not a leading identifier
# (e.g. an intern is keyed by name *and* the unit they sit in). Reviewed
# 2026-08-24; listed here so the ordering check stays a clean gate.
ACCEPTED_ORDERING = {
    "senador_escritorio_apoio",
    "servidor_ativo",
    "servidor_hora_extra_dia",
    "servidor_exonerado",
    "menor_aprendiz",
    "estagiario",
    "diretor_coordenador",
    "contratacao_orgao_gestor",
}

errors: list[str] = []
warnings: list[str] = []


def check() -> None:
    for slug, t in TABLES.items():
        if not SNAKE.match(slug):
            errors.append(f"{slug}: table slug is not snake_case")

        names = [c[0] for c in t["cols"]]
        if len(names) != len(set(names)):
            dupes = {n for n in names if names.count(n) > 1}
            errors.append(f"{slug}: duplicate column names {sorted(dupes)}")

        part_cols = []
        for col in t["cols"]:
            name, typ = col[0], col[1]
            pt, en, es = col[2], col[3], col[4]
            opts = col[5] if len(col) > 5 else {}

            if not SNAKE.match(name):
                errors.append(f"{slug}.{name}: not snake_case")
            if ACCENTS.search(name):
                errors.append(f"{slug}.{name}: column name has accents")
            if typ not in VALID_TYPES:
                errors.append(f"{slug}.{name}: unknown type {typ!r}")

            for lang, desc in (("pt", pt), ("en", en), ("es", es)):
                if not desc:
                    errors.append(f"{slug}.{name}: empty {lang} description")
                    continue
                # Column descriptions must not end with a period.
                if desc.rstrip().endswith("."):
                    errors.append(
                        f"{slug}.{name}: {lang} description ends with a period"
                    )
                # First letter must be capitalised.
                if desc[0] != desc[0].upper():
                    errors.append(
                        f"{slug}.{name}: {lang} description not capitalised"
                    )

            # Every numeric quantity carries a measurement unit; year and
            # month FKs are calendar labels, not quantities, so they are exempt.
            if typ in NUMERIC and not opts.get("unit") and not opts.get("dir"):
                errors.append(
                    f"{slug}.{name}: {typ} column has no measurement_unit"
                )
            # A unit on a non-numeric column is meaningless.
            if opts.get("unit") and typ not in NUMERIC:
                errors.append(f"{slug}.{name}: unit set on a {typ} column")
            # covered_by_dictionary only ever applies to STRING.
            if opts.get("dict") and typ != "str":
                errors.append(f"{slug}.{name}: dict=True on a {typ} column")

            if opts.get("part"):
                part_cols.append(name)

        # Partition declared in _t must exist and be marked part=True.
        part = t["partition"]
        if part is None:
            if part_cols:
                errors.append(
                    f"{slug}: part=True on {part_cols} but partition=None"
                )
        else:
            if part not in names:
                errors.append(f"{slug}: partition {part!r} is not a column")
            if part_cols != [part]:
                errors.append(
                    f"{slug}: partition={part!r} but part=True on {part_cols}"
                )
            if names[0] != part:
                errors.append(
                    f"{slug}: partition {part!r} must be the first column, "
                    f"got {names[0]!r}"
                )

        # Unique key must reference real columns.
        for key in t["unique"]:
            if key not in names:
                errors.append(f"{slug}: unique key {key!r} is not a column")

        for lang in ("pt", "en", "es"):
            if not t[f"name_{lang}"] or not t[f"desc_{lang}"]:
                errors.append(f"{slug}: missing {lang} name or description")

        # Column ordering: leading temporal/partition group, then the
        # columns forming the table's unique key, then descriptive columns.
        # "Identifier" means *identifies this row* — a column in the unique
        # key — not merely a column whose name starts with sigla_ or codigo_.
        # `sigla_lotacao` is an attribute of the row's entity and belongs
        # beside `lotacao`, not hoisted to the front.
        lead = 0
        while lead < len(names) and names[lead] in LEADING:
            lead += 1
        key = [n for n in t["unique"] if n not in LEADING]
        body = names[lead:]
        for k in key:
            if k not in body:
                continue
            pos = body.index(k)
            earlier_non_key = [n for n in body[:pos] if n not in key]
            if earlier_non_key and slug not in ACCEPTED_ORDERING:
                warnings.append(
                    f"{slug}: key column {k!r} appears after non-key "
                    f"column(s) {earlier_non_key}"
                )
                break


check()

print(
    f"{len(TABLES)} tables, {sum(len(t['cols']) for t in TABLES.values())} columns"
)
part_ano = [s for s, t in TABLES.items() if t["partition"] == "ano"]
part_snap = [s for s, t in TABLES.items() if t["partition"] == "data_extracao"]
print(f"  partitioned by ano:           {len(part_ano)}")
print(f"  partitioned by data_extracao: {len(part_snap)}")
print(
    f"  unpartitioned:                {len(TABLES) - len(part_ano) - len(part_snap)}"
)

if warnings:
    print(f"\n{len(warnings)} WARNING(S):")
    for w in warnings:
        print("  ~", w)
if errors:
    print(f"\n{len(errors)} ERROR(S):")
    for e in errors:
        print("  ✗", e)
    sys.exit(1)
print("\nAll house-rule checks passed.")
