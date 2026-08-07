# ruff: noqa: SIM115, E741
"""Assemble the ANES CDF architecture from parsed.json + en_final.json + the
merged translations (build/chunks/tr_*.tsv). Emits:
  code/architecture/cumulative.csv   (12-col BD architecture schema)
  code/architecture/dicionario.csv   (5 meta-columns, described)
  code/build/dicionario_data.csv      (value->label long table for the parquet)
  code/build/columns.json             (ordered manifest for cleaning + dbt)
"""

import csv
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
B = ROOT / "build"
ARCH = ROOT / "architecture"
ARCH.mkdir(exist_ok=True)

parsed = json.load(open(B / "parsed.json"))
en = json.load(open(B / "en_final.json"))
csv_cols = [c.strip() for c in open(B / "csv_cols.txt") if c.strip()]

# merge translations
tr = {}
for f in sorted((B / "chunks").glob("tr_*.tsv")):
    for row in csv.reader(open(f), delimiter="\t"):
        if len(row) >= 3:
            tr[row[0]] = (row[1].strip(), row[2].strip())
missing_tr = [c for c in en if c not in tr]
if missing_tr:
    raise SystemExit(
        f"Missing translations for {len(missing_tr)}: {missing_tr[:15]}"
    )

# ---- casing / terminology normalization ------------------------------------
# Applied to all three languages: surnames and place names (spelling-invariant).
GAZ_ALL = [
    "Wallace",
    "Reagan",
    "Carter",
    "Nixon",
    "Ford",
    "Bush",
    "Clinton",
    "Gore",
    "Kerry",
    "Obama",
    "McCain",
    "Romney",
    "Trump",
    "Biden",
    "Dukakis",
    "Mondale",
    "Anderson",
    "Perot",
    "Jackson",
    "Robertson",
    "Buchanan",
    "Nader",
    "Kennedy",
    "Johnson",
    "Humphrey",
    "Goldwater",
    "Eisenhower",
    "Stevenson",
    "Roosevelt",
    "Truman",
    "Dewey",
    "Watergate",
    "Iraq",
    "Russia",
    "China",
    "Cuba",
    "Israel",
]
# English-only: institutions and demonyms (PT/ES have their own forms already).
GAZ_EN = {
    "house": "House",
    "senate": "Senate",
    "congress": "Congress",
    "supreme court": "Supreme Court",
    "social security": "Social Security",
    "vietnam": "Vietnam",
    "bible": "Bible",
    "catholic": "Catholic",
    "protestant": "Protestant",
    "jewish": "Jewish",
    "christian": "Christian",
    "muslim": "Muslim",
    "hispanic": "Hispanic",
    "asian": "Asian",
    "new deal": "New Deal",
    "persian gulf": "Persian Gulf",
    "cold war": "Cold War",
    "democrats": "Democrats",
    "republicans": "Republicans",
    "democratic": "Democratic",
    "republican": "Republican",
    "democrat": "Democrat",
}
GAZ_ALL_MAP = {w.lower(): w for w in GAZ_ALL}


def _apply(s, mapping):
    keys = sorted(mapping, key=len, reverse=True)
    pat = re.compile(
        # pyrefly: ignore [bad-specialization, no-matching-overload]
        r"(?<![\w'])(" + "|".join(re.escape(k) for k in keys) + r")(?![\w'])",
        re.I,
    )
    return pat.sub(lambda m: mapping[m.group(0).lower()], s)


def norm_en(s):
    return _apply(_apply(s, GAZ_ALL_MAP), GAZ_EN)


def norm_ptes(s):
    s = re.sub(
        r"\bcolapsad([oa]s?)\b",
        lambda m: "agrupad" + m.group(1),
        s,
        flags=re.I,
    )
    return _apply(s, GAZ_ALL_MAP)


RANGE = re.compile(r"^\d+-\d+$")


def dict_pairs(v):
    """Enumerated (non-range) value->label pairs from Valid+Missing."""
    out = []
    for k, l in v["valid"] + v["missing"]:
        if not RANGE.match(k):
            out.append((k, l))
    return out


def missing_keys(v):
    return [k for k, l in v["missing"] if not RANGE.match(k)]


# ---- column order: year (partition), ids, then original CSV order -----------
ordered = ["VCF0004", "VCF0006", "VCF0006a"]
for c in csv_cols:
    if c == "Version" or c in ordered:
        continue
    ordered.append(c)

# Fail fast (like the translations check above) if any column lacks codebook
# coverage — otherwise parsed[code]/en[code] below would raise a bare KeyError.
no_entry = [c for c in ordered if c not in parsed or c not in en]
if no_entry:
    raise SystemExit(f"No codebook entry for {len(no_entry)}: {no_entry[:15]}")


def name_of(code):
    return "year" if code == "VCF0004" else code


ARCH_HEADER = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
    "description_en",
    "description_es",
]

rows = []
manifest = []
dict_data = []  # (id_tabela, nome_coluna, chave, cobertura_temporal, valor)

for code in ordered:
    v = parsed[code]
    nm = name_of(code)
    btype = v["type"]
    pt, es = tr[code]
    pt, es = norm_ptes(pt), norm_ptes(es)
    en_desc = norm_en(en[code])
    pairs = dict_pairs(v)
    is_id = code in ("VCF0006", "VCF0006a")
    is_num = btype in ("INT64", "FLOAT64")
    covered = "yes" if (btype == "STRING" and not is_id and pairs) else "no"

    directory = (
        "br_bd_diretorios_data_tempo.ano:ano" if code == "VCF0004" else ""
    )
    unit = {"VCF0101": "years"}.get(code, "")
    if "thermometer" in en_desc.lower() and is_num:
        unit = "degrees"

    obs = []
    if code == "VCF0004":
        obs.append("Renamed from VCF0004 (study year); table partition column")
    if re.match(r"VCF00(09|10|11)", code) or code == "VCF9999":
        obs.append(
            "Dimensionless survey weight; see codebook appendix on weights in the CDF"
        )
    if is_num and missing_keys(v):
        obs.append(
            "Missing sentinels set to NULL: " + ", ".join(missing_keys(v))
        )
    if btype == "STRING" and not pairs and not is_id:
        obs.append("Coded per ANES codebook; full value list in the codebook")

    rows.append(
        [
            nm,
            btype,
            pt,
            "",
            covered,
            directory,
            unit,
            "no",
            "; ".join(obs),
            code,
            en_desc,
            es,
        ]
    )
    manifest.append(
        {
            "name": nm,
            "code": code,
            "type": btype,
            "covered": covered,
            "null_sentinels": missing_keys(v) if is_num else [],
        }
    )

    # dictionary data rows (English only, one key -> one value)
    if covered == "yes":
        for k, l in pairs:
            dict_data.append(["cumulative", nm, k, "", l])

# write cumulative architecture
with open(ARCH / "cumulative.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(ARCH_HEADER)
    w.writerows(rows)

# dicionario architecture (describe the 5 meta-columns) — mirror us_census_cps
DIC_META = [
    (
        "id_tabela",
        "Nome da tabela à qual a coluna pertence",
        "Name of the table the column belongs to",
        "Nombre de la tabla a la que pertenece la columna",
    ),
    (
        "nome_coluna",
        "Nome da coluna categórica descrita pelo dicionário",
        "Name of the categorical column described by the dictionary",
        "Nombre de la columna categórica descrita por el diccionario",
    ),
    (
        "chave",
        "Código ou valor assumido pela coluna categórica",
        "Code or value taken by the categorical column",
        "Código o valor que toma la columna categórica",
    ),
    (
        "cobertura_temporal",
        "Cobertura temporal da chave",
        "Temporal coverage of the key",
        "Cobertura temporal de la clave",
    ),
    (
        "valor",
        "Rótulo correspondente ao código (em inglês, conforme o codebook ANES)",
        "Label corresponding to the code (in English, per the ANES codebook)",
        "Etiqueta correspondiente al código (en inglés, según el codebook de ANES)",
    ),
]
with open(ARCH / "dicionario.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(ARCH_HEADER)
    for nm, pt, en_d, es in DIC_META:
        w.writerow(
            [nm, "STRING", pt, "", "no", "", "", "no", "", nm, en_d, es]
        )

with open(B / "dicionario_data.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(
        ["id_tabela", "nome_coluna", "chave", "cobertura_temporal", "valor"]
    )
    w.writerows(dict_data)

json.dump(manifest, open(B / "columns.json", "w"), indent=0)

nnum = sum(1 for m in manifest if m["type"] != "STRING")
print(
    f"cumulative.csv rows: {len(rows)} (numeric={nnum}, string={len(rows) - nnum})"
)
print(f"covered_by_dictionary=yes: {sum(1 for r in rows if r[4] == 'yes')}")
print(f"dicionario_data rows: {len(dict_data)}")
print("sample rows:")
for r in rows[:4]:
    print("  ", r[:7], "| en=", r[10])
