"""Build the column metadata payload for br_bd_execucao_estadual.

Three sources are joined here rather than transcribed by hand, so they cannot drift:

* `schema.yml` -- the authoritative Portuguese description of every column.
* the live BigQuery table -- the authoritative `bigquery_type`. Reading the real type
  matters more than it looks: `bigquery_type` is honoured only on CREATE, so a column
  registered with the wrong type cannot be patched, only deleted and recreated.
* `translations.py` -- English and Spanish, keyed by the Portuguese string so a
  description shared across tables is translated once.

Emits the `columns_json` payload for `bulk_upsert_columns`. It writes nothing itself:
run it, read the summary, then feed the JSON to the MCP tool.

Usage:
    uv run python models/br_bd_execucao_estadual/code/register_columns.py [--table T]
"""

from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).resolve().parent))

import yaml  # noqa: E402
from google.cloud import bigquery  # noqa: E402
from translations import DESCRIPTIONS, OBSERVATIONS  # noqa: E402

DATASET = "br_bd_execucao_estadual"
PROJECT = "basedosdados-dev"
SCHEMA_YML = Path(__file__).resolve().parents[1] / "schema.yml"
PREFIX = f"{DATASET}__"

# Money is in current reais throughout; the source publishes no deflated series.
BRL = "BRL"

# `quantidade` deliberately carries no unit: the unit varies per item and is published
# in the `unidade_medida` column beside it, so naming one here would be wrong.
UNITLESS = {"quantidade"}

# Columns whose values are codes needing the `dicionario` to read. Function and
# subfunction follow the federal functional classification; the rest are defined by each
# state in its own budget law, which is why `dicionario` is keyed by (state, code).
CODED = {
    "funcao",
    "subfuncao",
    "programa",
    "acao",
    "categoria_economica",
    "grupo_despesa",
    "modalidade_aplicacao",
    "elemento_despesa",
    "subelemento_despesa",
    "item_despesa",
    "fonte_recurso",
    "destinacao_recurso",
}

# The federative unit resolves through the BD state directory.
DIRECTORY = {"sigla_uf": "br_bd_diretorios_brasil.uf:sigla"}

# Personal identifiers of natural persons. The states publish CPFs partially masked, but
# the column still carries person-level identification.
SENSITIVE = {
    "documento_credor",
    "documento_credor_formatado",
    "documento",
    "documento_formatado",
    "documento_vencedor",
    "documento_credor_empenho",
}


def portuguese() -> dict[str, dict[str, str]]:
    """{table_slug: {column: pt description}} from schema.yml."""
    doc = yaml.safe_load(SCHEMA_YML.read_text())
    out: dict[str, dict[str, str]] = {}
    for model in doc["models"]:
        slug = model["name"].removeprefix(PREFIX)
        out[slug] = {
            c["name"]: " ".join(str(c.get("description", "")).split())
            for c in model.get("columns", [])
        }
    return out


def bq_types(client: bigquery.Client, slug: str) -> dict[str, str]:
    table = client.get_table(f"{PROJECT}.{DATASET}.{slug}")
    # BigQuery reports the legacy spellings; the backend wants the standard ones.
    alias = {"INTEGER": "INT64", "FLOAT": "FLOAT64", "BOOL": "BOOLEAN"}
    return {
        f.name: alias.get(f.field_type, f.field_type) for f in table.schema
    }


def build(slug: str, pt: dict[str, str], types: dict[str, str]) -> list[dict]:
    missing_translation, missing_type = [], []
    payload = []
    for name, desc in pt.items():
        if name not in types:
            missing_type.append(name)
            continue
        if desc not in DESCRIPTIONS:
            missing_translation.append(name)
            continue
        en, es = DESCRIPTIONS[desc]
        col: dict[str, object] = {
            "name": name,
            "bigquery_type": types[name],
            "description_pt": desc,
            "description_en": en,
            "description_es": es,
            "covered_by_dictionary": name in CODED,
            "has_sensitive_data": name in SENSITIVE,
        }
        # ano/mes are calendar labels, not quantities, and take no unit either.
        if (
            types[name] in ("INT64", "FLOAT64")
            and name not in UNITLESS
            and name not in ("ano", "mes")
        ):
            col["measurement_unit"] = BRL
        if name in DIRECTORY:
            col["directory_column"] = DIRECTORY[name]
        # Caveats go in observations, never in the description: a description says what
        # the column holds, a caveat is a note about the data.
        note = OBSERVATIONS.get((slug, name))
        if note:
            (
                col["observations_pt"],
                col["observations_en"],
                col["observations_es"],
            ) = note
        payload.append(col)

    if missing_translation:
        raise SystemExit(
            f"{slug}: no EN/ES for {missing_translation}. Add them to translations.py "
            "-- do not register a column in Portuguese only."
        )
    if missing_type:
        raise SystemExit(
            f"{slug}: columns in schema.yml but not in BigQuery: {missing_type}. "
            "Rebuild the model first."
        )
    return payload


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table")
    ap.add_argument("--out", default="/tmp/columns_payload.json")
    args = ap.parse_args()

    client = bigquery.Client(project=PROJECT)
    pt_all = portuguese()
    tables = [args.table] if args.table else sorted(pt_all)

    result = {}
    for slug in tables:
        payload = build(slug, pt_all[slug], bq_types(client, slug))
        result[slug] = payload
        units = sum(1 for c in payload if c.get("measurement_unit"))
        coded = sum(1 for c in payload if c["covered_by_dictionary"])
        sens = sum(1 for c in payload if c["has_sensitive_data"])
        obs = sum(1 for c in payload if c.get("observations_pt"))
        print(
            f"{slug:24} {len(payload):>3} cols  unit={units:>2} "
            f"dict={coded:>2} sensitive={sens} obs={obs}"
        )

    Path(args.out).write_text(json.dumps(result, ensure_ascii=False, indent=1))
    print(f"\n{sum(len(v) for v in result.values())} columns -> {args.out}")


if __name__ == "__main__":
    main()
