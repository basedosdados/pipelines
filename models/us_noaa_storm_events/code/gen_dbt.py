"""Write the dbt models and schema.yml for us_noaa_storm_events.

Generated from the architecture TSVs so the SQL cast list, the column order and
the documented descriptions cannot drift from the schema they are supposed to
implement.

Run: uv run python models/us_noaa_storm_events/code/gen_dbt.py
"""

from common import DATA_TABLES, REPO_ROOT, load_cols

MODELS = REPO_ROOT / "models" / "us_noaa_storm_events"
DATASET = "us_noaa_storm_events"

# 2026 is the last year present; the partition range runs five years past it per
# the house convention.
PARTITION_START, PARTITION_END = 1950, 2031

# Columns whose non-null share is below the 0.05 floor of
# not_null_proportion_multiple_columns, measured over the full corpus by
# verify_parquet.py. Every one is a tornado- or hurricane-specific field that is
# empty by construction on the other 96%+ of events.
SPARSE = [
    "hurricane_category",
    "tornado_scale",
    "tornado_other_wfo",
    "tornado_other_state_abbreviation",
    "tornado_other_cz_fips",
    "tornado_other_cz_name",
]

TABLE_DESCRIPTIONS = {
    "event": """
      Um registro por evento meteorológico severo registrado pelo National
      Weather Service nos Estados Unidos e em suas águas costeiras desde 1950,
      com início e fim, localização, tipo de evento, magnitude, mortos, feridos
      e estimativas de danos materiais e às lavouras. Fonte do NOAA Storm Data.
      A cobertura por tipo de evento não é uniforme ao longo do período: apenas
      tornados são registrados de 1950 a 1954; tornado, vento de tempestade e
      granizo de 1955 a 1995; e o conjunto completo de tipos somente a partir de
      1996. Uma contagem de eventos por ano ao longo de todo o período, sem
      separar por tipo, mede a expansão do registro e não a ocorrência de
      eventos. Os valores de danos são publicados pela fonte como texto com
      sufixo de magnitude e aqui decodificados para dólares correntes, com a
      forma original preservada nas colunas damage_property_source e
      damage_crops_source. Complementa us_fema_openfema: este conjunto traz o
      evento meteorológico tal como observado pelo serviço de meteorologia,
      enquanto o FEMA traz a decisão administrativa de declarar desastre e o
      auxílio pago em decorrência dela.
    """,
    "fatality": """
      Um registro por morte atribuída a um evento meteorológico severo, ligada
      ao evento por event_id, com idade, sexo, tipo de local e se a morte foi
      causada direta ou indiretamente pelo evento. Fonte do NOAA Storm Data. O
      identificador fatality_id não é único em todo o período porque a fonte
      reiniciou a numeração; a chave da tabela é o par (event_id, fatality_id).
    """,
    "event_location": """
      Um registro por ponto geográfico associado a um evento meteorológico
      severo, ligado ao evento por event_id, com latitude e longitude e a
      localidade de referência a partir da qual a distância e o azimute são
      medidos. Fonte do NOAA Storm Data. Um mesmo evento pode ter vários pontos,
      numerados em location_index. A cobertura começa em 1996, com dois registros
      isolados em 1972 e nenhum nos demais anos anteriores.
    """,
}

TABLE_NAMES = {
    "event": ("Evento", "Event", "Evento"),
    "fatality": ("Morte", "Fatality", "Muerte"),
    "event_location": (
        "Localização do evento",
        "Event location",
        "Ubicación del evento",
    ),
}

# The logical key of each table, used for the uniqueness test.
KEYS = {
    "event": ["event_id"],
    "fatality": ["event_id", "fatality_id"],
    "event_location": ["event_id", "location_index"],
}

CAST = {
    "INT64": "safe_cast({c} as int64) {c}",
    "FLOAT64": "safe_cast({c} as float64) {c}",
    "STRING": "safe_cast({c} as string) {c}",
    "DATE": "safe_cast({c} as date) {c}",
    "DATETIME": "safe_cast({c} as datetime) {c}",
}


def write_model(table: str) -> None:
    cols = load_cols(table)
    casts = ",\n    ".join(CAST[c.bq_type].format(c=c.name) for c in cols)
    sql = f"""{{{{
    config(
        schema="{DATASET}",
        alias="{table}",
        materialized="table",
        partition_by={{
            "field": "year",
            "data_type": "int64",
            "range": {{"start": {PARTITION_START}, "end": {PARTITION_END}, "interval": 1}},
        }},
    )
}}}}


select
    {casts}
from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t
"""
    (MODELS / f"{DATASET}__{table}.sql").write_text(sql)
    print(f"wrote {DATASET}__{table}.sql ({len(cols)} columns)")


def write_dicionario() -> None:
    sql = f"""{{{{
    config(
        schema="{DATASET}",
        alias="dicionario",
        materialized="table",
    )
}}}}


select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor
from {{{{ set_datalake_project("{DATASET}_staging.dicionario") }}}} as t
"""
    (MODELS / f"{DATASET}__dicionario.sql").write_text(sql)
    print(f"wrote {DATASET}__dicionario.sql")


def _block(text: str, indent: int) -> str:
    """Render a description as a folded YAML block at the given indent."""
    pad = " " * indent
    words = " ".join(text.split())
    out, line = [], pad
    for w in words.split(" "):
        if len(line) + len(w) + 1 > 78 and line != pad:
            out.append(line)
            line = pad + w
        else:
            line = w if line == pad else line + " " + w
            if line == w:
                line = pad + w
    out.append(line)
    return "\n".join(out)


def write_schema() -> None:
    from pipelines.datasets.us_noaa_storm_events.constants import constants

    lines = ["---", "version: 2", "models:"]
    for table in DATA_TABLES:
        cols = load_cols(table)
        lines.append(f"  - name: {DATASET}__{table}")
        lines.append("    description: >-")
        lines.append(_block(TABLE_DESCRIPTIONS[table], 6))
        lines.append("    tests:")
        lines.append("      - dbt_utils.unique_combination_of_columns:")
        lines.append("          combination_of_columns:")
        for k in KEYS[table]:
            lines.append(f"            - {k}")
        lines.append("      - not_null_proportion_multiple_columns:")
        lines.append("          at_least: 0.05")
        if table == "event":
            lines.append(
                "          # Empty by construction on non-tornado and"
            )
            lines.append(
                "          # non-hurricane events, which are 96%+ of the table."
            )
            lines.append("          ignore_values:")
            for v in SPARSE:
                lines.append(f"            - {v}")
        dict_cols = constants.DICT_COLUMNS.value.get(table)
        if dict_cols:
            lines.append("      - custom_dictionary_coverage:")
            lines.append(
                f"          dictionary_model: ref('{DATASET}__dicionario')"
            )
            lines.append("          columns_covered_by_dictionary:")
            for c in dict_cols:
                lines.append(f"            - {c}")
        lines.append("    columns:")
        for c in cols:
            lines.append(f"      - name: {c.name}")
            lines.append("        description: >-")
            lines.append(_block(c.description_pt, 10))
            tests = []
            if c.name in ("year", *KEYS[table]):
                tests.append("not_null")
            rels = []
            if c.name == "year":
                rels.append(("br_bd_diretorios_data_tempo__ano", "ano.ano"))
            elif c.name == "month":
                rels.append(("br_bd_diretorios_data_tempo__mes", "mes.mes"))
            elif c.name == "state_id":
                rels.append(("br_bd_diretorios_us__state", "id_state"))
            elif c.name == "event_id" and table != "event":
                rels.append((f"{DATASET}__event", "event_id"))
            if tests or rels:
                lines.append("        tests:")
                for t in tests:
                    lines.append(f"          - {t}")
                for to, field in rels:
                    lines.append("          - relationships:")
                    lines.append(f"              to: ref('{to}')")
                    lines.append(f"              field: {field}")
    # dicionario
    lines.append(f"  - name: {DATASET}__dicionario")
    lines.append("    description: >-")
    lines.append(
        _block(
            """
        Registro dos valores assumidos pelas colunas codificadas das tabelas
        event e fatality, com a cobertura temporal de cada valor. A fonte publica
        rótulos legíveis na maior parte dessas colunas, de modo que valor repete
        chave onde não há código a traduzir; a coluna cobertura_temporal é o que
        expõe que o vocabulário de event_type não é estável ao longo do período.
    """,
            6,
        )
    )
    lines.append("    tests:")
    lines.append("      - dbt_utils.unique_combination_of_columns:")
    lines.append("          combination_of_columns:")
    for k in ("id_tabela", "nome_coluna", "chave"):
        lines.append(f"            - {k}")
    lines.append("    columns:")
    dic = [
        ("id_tabela", "Nome da tabela a que a coluna codificada pertence"),
        ("nome_coluna", "Nome da coluna codificada"),
        ("chave", "Valor armazenado na coluna"),
        (
            "cobertura_temporal",
            "Anos em que o valor aparece, na notação início(intervalo)fim",
        ),
        ("valor", "Significado do valor armazenado"),
    ]
    for name, desc in dic:
        lines.append(f"      - name: {name}")
        lines.append(f"        description: {desc}")
        if name in ("id_tabela", "nome_coluna", "chave"):
            lines.append("        tests: [not_null]")
    (MODELS / "schema.yml").write_text("\n".join(lines) + "\n")
    print(f"wrote schema.yml ({len(lines)} lines)")


def main() -> None:
    MODELS.mkdir(parents=True, exist_ok=True)
    for table in DATA_TABLES:
        write_model(table)
    write_dicionario()
    write_schema()


if __name__ == "__main__":
    main()
