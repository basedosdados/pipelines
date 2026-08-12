"""Generate the fr_insee_sirene dbt SQL models from schema_map.py.

Emits models/fr_insee_sirene/fr_insee_sirene__<table>.sql for the 4 large tables.
`geometria` (GEOGRAPHY) is built in-SQL from longitude/latitude, inserted right
after `longitude`. dicionario has its own hand-written model.
"""

from pathlib import Path

import schema_map as sm  # pyrefly: ignore [missing-import]

MODELS_DIR = Path(__file__).resolve().parents[1]  # models/fr_insee_sirene
CLUSTER = {
    "unite_legale": "siren",
    "etablissement": "siret",
    "unite_legale_historico": "siren",
    "etablissement_historico": "siret",
}
# Output/prod table slug (French). Staging source keeps the schema_map key name.
OUT_ALIAS = {
    "unite_legale_historico": "unite_legale_historique",
    "etablissement_historico": "etablissement_historique",
}
CAST = {
    "STRING": "string",
    "DATE": "date",
    "INT64": "int64",
    "FLOAT64": "float64",
}


def cast_line(target, ttype):
    # source column in staging is named `target` (cleaner already renamed)
    return f"    safe_cast({target} as {CAST[ttype]}) {target},"


for table, spec in sm.TABLES.items():
    lines = []
    for target, _src, ttype in spec["columns"]:
        lines.append(cast_line(target, ttype))
        if table == "etablissement" and target == "longitude":
            lines.append(
                "    case when longitude is not null and latitude is not null "
                "then st_geogpoint(safe_cast(longitude as float64), "
                "safe_cast(latitude as float64)) end geometria,"
            )
    body = "\n".join(lines).rstrip(",")
    alias = OUT_ALIAS.get(table, table)
    sql = f"""{{{{
    config(
        schema="fr_insee_sirene",
        alias="{alias}",
        materialized="table",
        partition_by={{
            "field": "data",
            "data_type": "date",
        }},
        cluster_by=["{CLUSTER[table]}"],
    )
}}}}
select
{body}
from {{{{ set_datalake_project("fr_insee_sirene_staging.{table}") }}}} as t
"""
    out = MODELS_DIR / f"fr_insee_sirene__{alias}.sql"
    out.write_text(sql)
    print(
        f"wrote {out.name} ({len(spec['columns']) + (1 if table == 'etablissement' else 0)} cols)"
    )
