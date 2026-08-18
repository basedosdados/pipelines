"""Force every cleaned parquet column to VARCHAR.

Staging is all-STRING by house convention: BigQuery builds the external table
from one file's schema, so a column typed INT32 in one partition and STRING in
another makes dbt read the whole table against the wrong type. Columns that
are entirely NULL in a given program year are how that happens -- CMS had not
started collecting them, and a bare NULL literal is not a string.

clean.py now casts those explicitly, so this is a repair for partitions
written before that fix and a safety net afterwards.

    uv run --with duckdb python repair_string_schema.py
"""

import constants as c
import pyarrow.parquet as pq


def main() -> None:
    import duckdb

    con = duckdb.connect(
        config={"memory_limit": "8GB", "preserve_insertion_order": "false"}
    )
    con.execute(f"SET temp_directory='{c.DATA_ROOT / 'duckdb_tmp'}'")

    repaired = 0
    for path in sorted(c.OUTPUT_DIR.rglob("*.parquet")):
        schema = pq.read_schema(path)
        offenders = [
            name
            for name, kind in zip(schema.names, schema.types, strict=True)
            if str(kind) != "string"
        ]
        if not offenders:
            continue
        casts = ", ".join(
            f'CAST("{name}" AS VARCHAR) AS "{name}"' for name in schema.names
        )
        temp = path.with_suffix(".repaired.parquet")
        con.execute(
            f"COPY (SELECT {casts} FROM read_parquet('{path}')) TO '{temp}' "
            "(FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 200000)"
        )
        before = pq.ParquetFile(path).metadata.num_rows
        after = pq.ParquetFile(temp).metadata.num_rows
        if before != after:
            temp.unlink()
            raise ValueError(f"{path}: row count changed {before} -> {after}")
        temp.replace(path)
        repaired += 1
        print(
            f"  {path.relative_to(c.OUTPUT_DIR)}  cast: {', '.join(offenders[:6])}"
        )
    con.close()
    print(f"\n{repaired} file(s) repaired")


if __name__ == "__main__":
    main()
