"""Check that the MG arm of each procurement model lines up with the national one.

    uv run dbt compile --target-path /tmp/dbt_mides \
        --select world_wb_mides__licitacao world_wb_mides__licitacao_item \
                 world_wb_mides__licitacao_participante
    ~/.venvs/bd-pipelines/bin/python models/world_wb_mides/code/verify_mg_unions.py \
        --compiled /tmp/dbt_mides/compiled/basedosdados/models/world_wb_mides

WHY THIS EXISTS
---------------
`licitacao`, `licitacao_item` and `licitacao_participante` are unions: the
national arm, then `select *` from the MG state model. `union all` resolves
POSITIONALLY, not by name, and `select *` hands over whatever order the state
model happens to emit. Insert a column in one arm and every column after it
shifts against the other -- silently, and with the FIRST arm's names on the
result, so the published table still looks right.

Nothing else catches this:

  * `dbt compile` never checks SQL semantics.
  * A dry run of the whole model checks that the arms are type-COMPATIBLE. Two
    swapped STRING columns are type-compatible, so it passes.
  * A uniqueness or not-null test on the published table passes just as well,
    because the values are real values -- they are simply in the wrong column.

So the check has to compare the two arms' output schemas, name by name, in
order. This asks BigQuery for each arm's schema with a dry run, which is exact
(it is BigQuery's own parser, not a regex over SQL) and bills nothing.

Run it after touching either arm of any of the three models.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account

CREDENTIALS = Path.home() / ".basedosdados/credentials/staging.json"
MODELS = ("licitacao", "licitacao_item", "licitacao_participante")


def split_top_level_union(sql: str) -> tuple[str, str] | None:
    """Split at the LAST `union all` sitting outside every parenthesis.

    The MG state models contain unions of their own (licitacao_mg has a
    competitiva arm and a dispensa arm), but those are nested inside the CTE
    that dbt inlines, so a paren-depth scan separates the two arms that matter.
    """
    depth, index, positions = 0, 0, []
    lowered = sql.lower()
    while index < len(sql):
        char = sql[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif depth == 0 and lowered.startswith("union all", index):
            positions.append(index)
            index += len("union all")
            continue
        index += 1
    if not positions:
        return None
    cut = positions[-1]
    return sql[:cut], sql[cut + len("union all") :]


def cte_prefix(sql: str) -> str:
    """The compiled model's leading `with ...` block, which both arms need.

    dbt inlines every ephemeral model as a CTE there, so the MG arm -- which is
    only `select * from __dbt__cte__..._mg` -- does not resolve without it.
    """
    depth, index, lowered = 0, 0, sql.lower()
    while index < len(sql):
        char = sql[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif (
            depth == 0
            and lowered.startswith("select", index)
            and (index == 0 or sql[index - 1] == "\n")
        ):
            return sql[:index]
        index += 1
    return ""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--compiled",
        required=True,
        help="dbt's compiled .../models/world_wb_mides directory",
    )
    args = parser.parse_args()

    creds = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS)
    )
    client = bigquery.Client(
        credentials=creds,
        project=json.loads(CREDENTIALS.read_text())["project_id"],
    )
    config = bigquery.QueryJobConfig(dry_run=True)

    def schema_of(sql: str) -> list[tuple[str, str]]:
        job = client.query(f"select * from (\n{sql}\n)", job_config=config)
        # A dry run always resolves a schema for a valid SELECT; the client
        # types it Optional because a DDL statement has none.
        return [(f.name, f.field_type) for f in job.schema or []]

    compiled = Path(args.compiled)
    problems = 0
    for name in MODELS:
        path = compiled / f"world_wb_mides__{name}.sql"
        if not path.exists():
            print(f"SKIP  {name}: not compiled at {path}")
            continue
        parts = split_top_level_union(path.read_text())
        if not parts:
            problems += 1
            print(
                f"FAIL  {name}: no top-level `union all` -- did the shape change?"
            )
            continue
        national = schema_of(parts[0])
        mg = schema_of(cte_prefix(parts[0]) + parts[1])
        if national == mg:
            print(
                f"OK    {name:<24} {len(national)} columns, identical names "
                f"and types in order"
            )
            continue
        problems += 1
        print(
            f"FAIL  {name}: national {len(national)} vs MG {len(mg)} columns"
        )
        for position, (left, right) in enumerate(
            zip(national, mg, strict=False)
        ):
            if left != right:
                print(f"   position {position}: national {left} vs MG {right}")
        if len(national) != len(mg):
            extra = national[len(mg) :] or mg[len(national) :]
            print(f"   length differs, extra: {extra}")

    print()
    if problems:
        raise SystemExit(f"{problems} model(s) misaligned")
    print("every union arm aligned")


if __name__ == "__main__":
    sys.exit(main())
