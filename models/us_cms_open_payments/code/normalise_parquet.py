"""Force every cleaned parquet to the layout's column order and all-STRING types.

Two things BigQuery cares about and neither is self-correcting:

* **Types.** Staging is all-STRING by house convention. BigQuery builds the
  external table from one file's schema, so a column typed INT32 in one
  partition and STRING in another makes dbt read the whole table against the
  wrong type. Columns entirely NULL in a given program year are how that
  happens -- a bare NULL literal is not a string.
* **Order.** The dbt model selects columns positionally from the layout, so a
  partition written in a different order would silently misalign.

clean.py gets both right now; this is the repair for partitions written before
those fixes, and the safety net afterwards. Row counts are checked before any
file is replaced.

    uv run --with duckdb python normalise_parquet.py
"""

import constants as c
import layout
import pyarrow.parquet as pq


def main() -> None:
    import duckdb

    con = duckdb.connect(
        config={"memory_limit": "8GB", "preserve_insertion_order": "false"}
    )
    con.execute(f"SET temp_directory='{c.DATA_ROOT / 'duckdb_tmp'}'")

    repaired = 0
    for table, columns in layout.LAYOUT.items():
        root = c.OUTPUT_DIR / table
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.parquet")):
            schema = pq.read_schema(path)
            mistyped = [
                name
                for name, kind in zip(schema.names, schema.types, strict=True)
                if str(kind) != "string"
            ]
            misordered = schema.names != columns
            if not mistyped and not misordered:
                continue
            if sorted(schema.names) != sorted(columns):
                raise ValueError(
                    f"{path}: column set differs from the layout, not just its order"
                )

            casts = ", ".join(
                f'CAST("{name}" AS VARCHAR) AS "{name}"' for name in columns
            )
            temp = path.with_suffix(".normalised.parquet")
            con.execute(
                f"COPY (SELECT {casts} FROM read_parquet('{path}')) TO '{temp}' "
                "(FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 200000)"
            )
            before = pq.ParquetFile(path).metadata.num_rows
            after = pq.ParquetFile(temp).metadata.num_rows
            if before != after:
                temp.unlink()
                raise ValueError(
                    f"{path}: row count changed {before} -> {after}"
                )
            temp.replace(path)
            repaired += 1
            reasons = []
            if mistyped:
                reasons.append(
                    f"cast {len(mistyped)}: {', '.join(mistyped[:4])}"
                )
            if misordered:
                reasons.append("reordered")
            print(f"  {path.relative_to(c.OUTPUT_DIR)}  {'; '.join(reasons)}")
    con.close()
    print(f"\n{repaired} file(s) normalised")


if __name__ == "__main__":
    main()
