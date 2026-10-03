"""Generate `models/world_wb_mides/schema_mg.yml` from the 43 MG-only models.

Column descriptions come from `mg_column_glossary.py`; table descriptions are in
TABLES below. Re-run after changing either, or after adding/removing a model:

    uv run python -m models.world_wb_mides.code.gen_mg_schema
    uv run pre-commit run --files models/world_wb_mides/schema_mg.yml

The second step is not optional bookkeeping: pre-commit reflows the long
description lines, so generating alone leaves a diff that the hook will rewrite.
Generate-then-format round-trips to a fixed point, so running both is idempotent.
"""

from __future__ import annotations

import os
import pathlib
import re

import models.world_wb_mides.code.mg_column_glossary as glossary
import models.world_wb_mides.code.mg_table_glossary as tables

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
OUT = os.path.join(ROOT, "schema_mg.yml")

DIRECTORY_TESTS: dict[str, tuple[str, str]] = {
    "ano": ("br_bd_diretorios_data_tempo__ano", "ano.ano"),
    "id_municipio": ("br_bd_diretorios_brasil__municipio", "id_municipio"),
    "sigla_uf": ("br_bd_diretorios_brasil__uf", "sigla"),
}


# Tables where `ano + <own key>` is not unique even though the key is non-NULL --
# genuine duplicate rows in the source, distinct from the orphan-key tables in
# `mg_table_glossary.ORPHAN_PARENT_KEY`. Both groups use the proportional
# uniqueness test instead of the strict dbt_utils one; the orphan tables need it
# because their NULLs collapse into a single large group, these need it because
# the source repeats rows. Measured from the 2026-09-25 dev test run.
RELAXED_UNIQUENESS: frozenset[str] = frozenset(
    {
        "contrato_contabilizacao",
        "despesa_dotacao",
        "dispensa",
        "dispensa_dotacao",
        "dispensa_responsavel",
        "liquidacao_fonte",
        "nota_fiscal",
        "nota_fiscal_item",
        "registro_preco_adesao",
        "restos_pagar",
        "restos_pagar_credor",
    }
)

# Columns that are legitimately more than 95% empty in the MG source, per the
# 2026-09-25 test run. Listed here so `not_null_proportion_multiple_columns`
# skips them rather than failing the whole table; every other column in these
# tables is still held to the 0.05 floor.
IGNORE_NULL_COLUMNS: dict[str, list[str]] = {
    "contrato_credito": ["subacao"],
    "contrato_item": ["codigo_item_sicro"],
    "contrato_termo_aditivo_item": ["codigo_item_sicro"],
    "despesa_dotacao": ["subacao"],
    "dispensa_dotacao": ["subacao"],
    "liquidacao_nota_fiscal": ["id_liquidacao_bd"],
    "lei_decreto": ["data_lei_alt", "data_pub_lei_alt", "numero_lei_alt"],
    "licitacao_dotacao": ["subacao"],
    "licitacao_julgamento": ["ind_desonera_folha"],
    "pagamento_movimento": ["numero_aplicacao"],
    "registro_preco_adesao": ["numero_modalidade"],
}


def columns_of(path: str) -> list[str]:
    """Published column names, in the model's own order.

    DEPTH-AWARE ON PURPOSE. `sqlfmt` (via pre-commit) reflows a long
    `safe_cast(concat(...)) as id_x_bd,` across a dozen lines, so a line-anchored
    regex silently under-counts and the caller's own-key assertion is the only
    thing that notices. Parse by parenthesis depth instead: capture the final
    top-level select list, split it on depth-zero commas, and take each item's
    trailing alias.
    """
    text = pathlib.Path(path).read_text(encoding="utf-8")
    lines = [line.split("--")[0] for line in text.split("\n")]

    # Find the final top-level `select` and the `from` that closes it.
    depth = 0
    start = end = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if depth == 0 and stripped == "select":
            start, end = i + 1, None
        elif (
            depth == 0
            and start is not None
            and end is None
            # sqlfmt puts a bare `from` on its own line
            and (stripped == "from" or stripped.startswith("from "))
        ):
            end = i
        depth += line.count("(") - line.count(")")
    if start is None or end is None:
        raise AssertionError(f"{path}: could not locate the final select list")

    # Split that block on depth-zero commas.
    items, buf, depth = [], [], 0
    for line in lines[start:end]:
        for char in line:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            if char == "," and depth == 0:
                items.append("".join(buf))
                buf = []
                continue
            buf.append(char)
        buf.append(" ")
    if "".join(buf).strip():
        items.append("".join(buf))

    names = []
    for item in items:
        match = re.search(r"\bas\s+([a-z_0-9]+)\s*$", item.strip())
        if match:
            names.append(match.group(1))
    return names


def block(table: str, columns: list[str]) -> list[str]:
    key = f"id_{table}_bd"
    if key not in columns:
        raise AssertionError(f"{table}: own key {key} is not among {columns}")
    relaxed = table in tables.ORPHAN_PARENT_KEY or table in RELAXED_UNIQUENESS
    out = [
        f"  - name: world_wb_mides__{table}",
        "    description: >",
        f"      {tables.description(table)}",
        "    tests:",
    ]
    if relaxed:
        out += [
            "      - custom_unique_combinations_of_columns:",
            f"          combination_of_columns: [ano, {key}]",
            "          proportion_allowed_failures: 0.05",
        ]
    else:
        out += [
            "      - dbt_utils.unique_combination_of_columns:",
            f"          combination_of_columns: [ano, {key}]",
        ]
    out += [
        "      - not_null_proportion_multiple_columns:",
        "          at_least: 0.05",
    ]
    if table in IGNORE_NULL_COLUMNS:
        ignored = ", ".join(IGNORE_NULL_COLUMNS[table])
        out.append(f"          ignore_values: [{ignored}]")
    out += [
        "          config:",
        "            where: __most_recent_year__",
        "    columns:",
    ]
    for column in columns:
        out.append(f"      - name: {column}")
        out.append(
            f"        description: {glossary.build_description(column)}"
        )
        tests: list[str] = []
        required = ["ano", "sigla_uf", "id_municipio"]
        if table not in tables.ORPHAN_PARENT_KEY:
            required.append(key)
        if column in required:
            tests.append("not_null")
        if column in DIRECTORY_TESTS:
            out.append("        tests:")
            for t in tests:
                out.append(f"          - {t}")
            ref, field = DIRECTORY_TESTS[column]
            out.append("          - relationships:")
            out.append(f"              to: ref('{ref}')")
            out.append(f"              field: {field}")
        elif tests:
            out.append(f"        tests: [{', '.join(tests)}]")
    return out


def main() -> None:
    # The 43 MG models share `models/world_wb_mides/` with the 9 original
    # multi-state ones, so the directory no longer names this set --
    # `mg_table_glossary.MG_TABLES` does, and it is the same 43 by construction
    # (the loop below still asserts each one has an entry).
    files = [f"world_wb_mides__{t}.sql" for t in sorted(tables.MG_TABLES)]
    lines = [
        "---",
        "# GENERATED by code/gen_mg_schema.py -- edit the glossary or TABLES there,",
        "# then re-run. Hand edits here are overwritten.",
        "version: 2",
        "models:",
    ]
    for fn in files:
        table = fn[len("world_wb_mides__") : -len(".sql")]
        if table not in tables.MG_TABLES:
            raise AssertionError(f"no description for table {table}")
        lines += block(table, columns_of(os.path.join(ROOT, fn)))
    with open(OUT, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")
    missed = glossary.unresolved()
    print(f"wrote {OUT}: {len(files)} models")
    print("columns with a mechanical fallback description:", missed or "none")


if __name__ == "__main__":
    main()
