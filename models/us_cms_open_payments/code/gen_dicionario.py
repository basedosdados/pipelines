"""Build the dicionario table from the profiled value sets.

Open Payments stores readable English labels rather than opaque codes, so the
dictionary here is a gloss: ``chave`` is the value as published and ``valor``
its Portuguese rendering, which is what makes the column legible to the site's
Portuguese and Spanish readers. Columns whose value set turned out to be free
text or a classification system were demoted by profile_data and never reach
this step.

    uv run --with duckdb python gen_dicionario.py
"""

import json
import sys

import constants as c
import layout
import profile_data
from glossary import GLOSS


def coverage(table: str) -> str:
    years = layout.COVERAGE.get(table)
    if not years:
        return ""
    return f"{years[0]}(1){years[-1]}"


def rows() -> tuple[list[dict[str, str]], list[tuple[str, str, str]]]:
    with open(profile_data.PROFILE_PATH) as fh:
        prof = json.load(fh)

    out, missing = [], []
    for table, data in prof.items():
        for column, values in sorted(data.get("values", {}).items()):
            for value in values:
                gloss = GLOSS.get(value)
                if gloss is None:
                    missing.append((table, column, value))
                    continue
                out.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": column,
                        "chave": value,
                        "cobertura_temporal": coverage(table),
                        "valor": gloss,
                    }
                )
    return out, missing


def main() -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    data, missing = rows()
    if missing:
        print(f"{len(missing)} value(s) without a Portuguese gloss:")
        for table, column, value in missing[:60]:
            print(f"  {table}.{column}: {value!r}")
        print("\nAdd them to glossary.py and re-run.")
        sys.exit(1)

    schema = pa.schema(
        [pa.field(name, pa.string()) for name in layout.LAYOUT["dicionario"]]
    )
    target = c.OUTPUT_DIR / "dicionario"
    target.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(data, schema=schema),
        target / "data.parquet",
        compression="snappy",
    )

    tables = {row["id_tabela"] for row in data}
    columns = {(row["id_tabela"], row["nome_coluna"]) for row in data}
    print(
        f"dicionario: {len(data):,} rows covering {len(columns)} column(s) "
        f"across {len(tables)} table(s)"
    )


if __name__ == "__main__":
    main()
