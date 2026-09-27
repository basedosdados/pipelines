"""Apply geography column renames + directory FK links to the two LAR architecture TSVs.
Keeps original_name (raw header) unchanged. Prints the rename map for the parquet step."""

from pathlib import Path

# table -> { old_name: (new_name, directory_column) }
RENAMES = {
    "sheet_loan_application_register.tsv": {
        "state_code": (
            "state_abbreviation",
            "br_bd_diretorios_us.state:abbreviation",
        ),
        "county_code": ("county_id", "br_bd_diretorios_us.county:id_county"),
        "census_tract": (
            "census_tract_id",
            "br_bd_diretorios_us.census_tract_2020:id_census_tract",
        ),
        "derived_msa_md": ("msa_md_id", ""),
    },
    "sheet_loan_application_register_legacy.tsv": {
        "state_code": ("state_id", "br_bd_diretorios_us.state:id_state"),
        "county_code": ("county_id", "br_bd_diretorios_us.county:id_county"),
        "census_tract": (
            "census_tract_id",
            "br_bd_diretorios_us.census_tract_2020:id_census_tract",
        ),
        "msa_md": ("msa_md_id", ""),
    },
}
HDR_I = {"name": 0, "directory_column": 5}

for fn, rmap in RENAMES.items():
    p = Path(fn)
    rows = [
        ln.split("\t")
        for ln in p.read_text(encoding="utf-8").rstrip("\n").split("\n")
    ]
    changed = []
    for r in rows[1:]:
        if r[0] in rmap:
            new, fk = rmap[r[0]]
            old = r[0]
            r[0] = new
            r[5] = fk
            changed.append((old, new, fk))
    p.write_text(
        "\n".join("\t".join(r) for r in rows) + "\n", encoding="utf-8"
    )
    print(f"\n{fn}:")
    for old, new, fk in changed:
        print(f"   {old:14s} -> {new:20s} dir='{fk}'")
