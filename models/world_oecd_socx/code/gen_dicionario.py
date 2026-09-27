"""Build the dicionario: every coded value that appears, with its OECD label.

The fact tables carry only codes; the labels live in the SDMX codelists, which
are structure metadata rather than data. So unlike a dataset whose source ships
code and label side by side, this dictionary cannot be a plain ``select distinct``
over the fact models -- there is no label column to select.

It is therefore emitted as a literal map, restricted to the codes that actually
occur in the cleaned output. Two things keep that honest:

* The restriction is computed from the **complete** cleaned corpus, not from a
  lookback window. This is a static onboarding with no recurring pipeline, so the
  run's output is the whole table -- the one case where harvesting from your own
  output is correct.
* ``custom_dictionary_coverage`` in schema.yml enforces the invariant at test
  time: every value present in a dictionary-covered column must appear here. If
  the facts ever gain a code this map lacks, the test fails rather than the
  dictionary quietly under-describing the data.

Emitting the full codelists instead would be 70,202 rows, 39,907 of them
CL_REGIONAL codes that never appear in any of these cubes.

Run: ``python gen_dicionario.py``   (after clean.py)
"""

import csv
import xml.etree.ElementTree as ET

import pyarrow.parquet as pq
from common import ARCH_DIR, DATASET_ID, OUTPUT, REPO_ROOT, STRUCTURE
from tables import TABLES

S = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}"
C = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}"
LANG = "{http://www.w3.org/XML/1998/namespace}lang"
MODELS = REPO_ROOT / "models" / DATASET_ID


def labels_for(dsd):
    """{component_id: {code: english label}} for a DSD, across all its versions.

    Merged across every cached version rather than read from the target one,
    because OECD *removes* codes between editions while the data that used them
    stays published. CL_AREA in DSD_EAG_SAL_ACT v1.1 documents BFL and BFR (the
    Flemish and French Communities of Belgium); v2.0 drops both, and the tables
    that union v1.1 still contain them. Newer versions are applied last, so a
    relabelled code takes its current label while a retired code keeps its old
    one instead of going undocumented.
    """
    merged = {}
    # The version glob is explicit: "dsd_DSD_EAG_UOE_FIN_*" would also match
    # DSD_EAG_UOE_FIN_ANNEX and DSD_EAG_UOE_FIN_ENR, which are different cubes.
    for path in sorted(STRUCTURE.glob(f"dsd_{dsd}_[0-9].[0-9].xml")):
        for comp, codes in _labels_one(path).items():
            merged.setdefault(comp, {}).update(codes)
    return merged


def _labels_one(path):
    """{component_id: {code: english label}} for one cached DSD file."""
    root = ET.parse(path).getroot()
    struct = next(root.iter(f"{S}DataStructure"))
    codelist_of = {}
    for tag in (f"{S}Dimension", f"{S}Attribute"):
        for comp in struct.iter(tag):
            ref = comp.find(f"{S}LocalRepresentation/{S}Enumeration/Ref")
            if comp.get("id") and ref is not None:
                codelist_of[comp.get("id")] = ref.get("id")
    codes = {}
    for cl in root.iter(f"{S}Codelist"):
        entries = {}
        for code in cl.iter(f"{S}Code"):
            name = next(
                (
                    n.text
                    for n in code.findall(f"{C}Name")
                    if (n.get(LANG) or "").startswith("en")
                ),
                "",
            )
            entries[code.get("id")] = name or ""
        codes[cl.get("id")] = entries
    return {comp: codes.get(cl, {}) for comp, cl in codelist_of.items()}


def dictionary_columns(slug):
    """[(column, sdmx component)] covered by the dictionary."""
    with (ARCH_DIR / f"{slug}.csv").open() as f:
        return [
            (r["name"], r["original_name"])
            for r in csv.DictReader(f)
            if r["covered_by_dictionary"] == "yes"
        ]


def observed(slug, columns):
    """{column: set of codes present} across the table's cleaned parquet."""
    seen = {c: set() for c, _ in columns}
    for path in sorted((OUTPUT / slug).rglob("*.parquet")):
        table = pq.ParquetFile(path).read(columns=[c for c, _ in columns])
        for name in seen:
            seen[name].update(
                v
                for v in table.column(name).to_pylist()
                if v not in (None, "")
            )
    return seen


def sql_literal(text):
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def main():
    rows = []
    undocumented = []
    for slug, spec in TABLES.items():
        columns = dictionary_columns(slug)
        if not columns:
            continue
        label_map = labels_for(spec["dsd"])
        for column, component in columns:
            codes = observed(slug, [(column, component)])[column]
            labels = label_map.get(component, {})
            for code in sorted(codes):
                label = labels.get(code)
                if label is None:
                    undocumented.append((slug, column, code))
                    continue
                rows.append((slug, column, code, label))
        print(f"  {slug:24s} {len(columns):2d} coded columns", flush=True)

    structs = ",\n        ".join(
        f"struct({sql_literal(t)} as id_tabela, {sql_literal(c)} as nome_coluna, {sql_literal(k)} as chave, "
        f"'' as cobertura_temporal, {sql_literal(v)} as valor)"
        for t, c, k, v in rows
    )
    model = f'''{{{{
    config(
        alias="dicionario",
        schema="{DATASET_ID}",
        materialized="table",
    )
}}}}

-- Every coded value that appears in this dataset, with the label the OECD
-- publishes for it in the SDMX codelists. The facts carry codes only -- the
-- labels are structure metadata, not data -- so this is a literal map rather
-- than a select over the fact models. custom_dictionary_coverage in schema.yml
-- enforces that it covers every value actually present.

select
    id_tabela,
    nome_coluna,
    chave,
    cobertura_temporal,
    valor
from
    unnest([
        {structs}
    ])
'''
    path = MODELS / f"{DATASET_ID}__dicionario.sql"
    path.write_text(model)
    print(
        f"\n{len(rows):,} dictionary entries -> {path.name} ({path.stat().st_size / 1e6:.1f} MB)"
    )
    if undocumented:
        print(
            f"{len(undocumented)} observed codes have no label in the codelist:"
        )
        for t, c, k in undocumented[:15]:
            print(f"    {t}.{c} = {k!r}")
        if len(undocumented) > 15:
            print(f"    ... and {len(undocumented) - 15} more")


if __name__ == "__main__":
    main()
