"""Generate architecture CSVs and trilingual columns_json for au_dcceew_greenhouse.

Single source of truth for the column design of all three inventory tables.
Emits, per table:
  - code/architecture/<table>.csv    (repo source-of-truth, data-language descriptions)
  - code/columns_json/<table>.json    (PT/EN/ES for bulk_upsert_columns)

Column order matches clean.py output exactly (architecture is the source of truth).
"""

import csv
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
CJSON = HERE / "columns_json"

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
]

# ---- shared column specs: (name, type, pt, en, es, extra{}) --------------------
YEAR = (
    "year",
    "INT64",
    "Ano de referência do inventário (ano fiscal australiano encerrado; 1990 = 1989-90)",
    "Reference year of the inventory (Australian financial year ending; 1990 = 1989-90)",
    "Año de referencia del inventario (año fiscal australiano finalizado; 1990 = 1989-90)",
    {
        "directory_column": "br_bd_diretorios_data_tempo.ano:ano",
        "measurement_unit": "year",
        "observations": "Partition column. OData InventoryYear_ID.",
        "original_name": "InventoryYear_ID",
    },
)
GEOGRAPHY = (
    "geography",
    "STRING",
    "Área geográfica: 'australia' (nacional) ou um dos oito estados/territórios, além de 'et' (territórios externos)",
    "Geographic area: 'australia' (national) or one of the eight states/territories, plus 'et' (external territories)",
    "Área geográfica: 'australia' (nacional) o uno de los ocho estados/territorios, además de 'et' (territorios externos)",
    {
        "observations": "Australia is the national aggregate; states/territories follow the ASGS. "
        "Not directory-linked because the national aggregate has no id_state. 'et' = external/other territories."
    },
)
EMISSIONS = (
    "emissions_gg",
    "FLOAT64",
    "Emissões em gigagramas (Gg). Para o gás 'CO2-e - AR5' a unidade é Gg de CO2-equivalente (Mt CO2-e = valor/1000); para um gás físico é Gg desse gás",
    "Emissions in gigagrams (Gg). For gas 'CO2-e - AR5' the unit is Gg CO2-equivalent (Mt CO2-e = value/1000); for a physical gas it is Gg of that gas",
    "Emisiones en gigagramos (Gg). Para el gas 'CO2-e - AR5' la unidad es Gg de CO2-equivalente (Mt CO2-e = valor/1000); para un gas físico es Gg de ese gas",
    {
        "measurement_unit": "gigagram",
        "observations": "Source measure, not converted. LULUCF (a UNFCCC category) can be negative (net sink).",
        "original_name": "Gg",
    },
)


def cat_col(
    prefix_lower,
    idx,
    scheme_pt,
    scheme_en,
    scheme_es,
    top_pt="",
    top_en="",
    top_es="",
):
    name = f"{prefix_lower}_level_{idx}"
    if idx == 1 and top_en:
        pt, en, es = top_pt, top_en, top_es
    else:
        pt = f"{scheme_pt}, nível {idx} (subcategoria; NULO onde o ramo é mais raso)"
        en = f"{scheme_en}, level {idx} (sub-category; NULL where the branch is shallower)"
        es = f"{scheme_es}, nivel {idx} (subcategoría; NULO donde la rama es menos profunda)"
    return (
        name,
        "STRING",
        pt,
        en,
        es,
        {
            "observations": f"OData {prefix_of[prefix_lower]}_Level_{idx}",
            "original_name": f"{prefix_of[prefix_lower]}_Level_{idx}",
        },
    )


prefix_of = {"unfccc": "UNFCCC", "anzsic": "ANZSIC", "scopetwo": "ScopeTwo"}

GAS = [
    (
        "gas_level_0",
        "STRING",
        "Gás ou métrica: um gás/poluente físico (CO2, CH4, N2O, ...) ou o agregado 'CO2-e - AR5' (CO2-equivalente, GWP AR5 de 100 anos)",
        "Gas or metric: a physical gas/pollutant (CO2, CH4, N2O, ...) or the aggregate 'CO2-e - AR5' (CO2-equivalent, AR5 100-year GWP)",
        "Gas o métrica: un gas/contaminante físico (CO2, CH4, N2O, ...) o el agregado 'CO2-e - AR5' (CO2-equivalente, GWP AR5 de 100 años)",
        {"original_name": "Gas_Level_0"},
    ),
    (
        "gas_level_1",
        "STRING",
        "Para linhas 'CO2-e - AR5', o gás contribuinte (CO2, CH4, N2O, Other); NULO para linhas de gás físico",
        "For 'CO2-e - AR5' rows, the contributing gas (CO2, CH4, N2O, Other); NULL for physical-gas rows",
        "Para filas 'CO2-e - AR5', el gas contribuyente (CO2, CH4, N2O, Other); NULO para filas de gas físico",
        {"original_name": "Gas_Level_1"},
    ),
    (
        "gas_level_2",
        "STRING",
        "Subgrupo da contribuição 'Other' de CO2-e (HFC, PFC, SF6); NULO caso contrário",
        "Sub-group of the CO2-e 'Other' contribution (HFC, PFC, SF6); NULL otherwise",
        "Subgrupo de la contribución 'Other' de CO2-e (HFC, PFC, SF6); NULO en caso contrario",
        {"original_name": "Gas_Level_2"},
    ),
    (
        "gas_level_3",
        "STRING",
        "Espécie específica dentro do subgrupo de CO2-e (por exemplo, HFC-134a, CF4); NULO caso contrário",
        "Specific species within the CO2-e sub-group (e.g. HFC-134a, CF4); NULL otherwise",
        "Especie específica dentro del subgrupo de CO2-e (por ejemplo, HFC-134a, CF4); NULO en caso contrario",
        {"original_name": "Gas_Level_3"},
    ),
]

UNFCCC_TOP = (
    "Categoria do inventário UNFCCC/Paris (setor IPCC de topo: Energy, Agriculture, Industrial Processes, LULUCF, Waste)",
    "UNFCCC/Paris inventory category (top IPCC sector: Energy, Agriculture, Industrial Processes, LULUCF, Waste)",
    "Categoría del inventario UNFCCC/Paris (sector IPCC principal: Energy, Agriculture, Industrial Processes, LULUCF, Waste)",
)
ANZSIC_TOP = (
    "Classificação por setor econômico ANZSIC (divisão: por exemplo, B Mining, C Manufacturing)",
    "ANZSIC economic-sector classification (division: e.g. B Mining, C Manufacturing)",
    "Clasificación por sector económico ANZSIC (división: por ejemplo, B Mining, C Manufacturing)",
)
SCOPE2_TOP = (
    "Setor econômico (divisão ANZSIC) das emissões de Escopo 2 (eletricidade indireta)",
    "Economic sector (ANZSIC division) of Scope 2 (indirect electricity) emissions",
    "Sector económico (división ANZSIC) de las emisiones de Alcance 2 (electricidad indirecta)",
)

SCHEME_LABEL = {
    "unfccc": (
        "Categoria do inventário UNFCCC/Paris",
        "UNFCCC/Paris inventory category",
        "Categoría del inventario UNFCCC/Paris",
    ),
    "anzsic": (
        "Classificação por setor econômico ANZSIC",
        "ANZSIC economic-sector classification",
        "Clasificación por sector económico ANZSIC",
    ),
    "scopetwo": (
        "Setor econômico do Escopo 2 (ANZSIC)",
        "Scope 2 economic sector (ANZSIC)",
        "Sector económico del Alcance 2 (ANZSIC)",
    ),
}

# per-table category level counts (from clean.py discovery)
TABLES = {
    "inventory_unfccc": {
        "prefix": "unfccc",
        "levels": 10,
        "gas": True,
        "top": UNFCCC_TOP,
    },
    "inventory_anzsic": {
        "prefix": "anzsic",
        "levels": 3,
        "gas": True,
        "top": ANZSIC_TOP,
    },
    "inventory_scope2": {
        "prefix": "scopetwo",
        "levels": 2,
        "gas": False,
        "top": SCOPE2_TOP,
    },
}


def build_columns(cfg):
    cols = [YEAR, GEOGRAPHY]
    p = cfg["prefix"]
    spt, sen, ses = SCHEME_LABEL[p]
    for i in range(1, cfg["levels"] + 1):
        cols.append(
            cat_col(p, i, spt, sen, ses, *cfg["top"])
            if i == 1
            else cat_col(p, i, spt, sen, ses)
        )
    if cfg["gas"]:
        cols.extend(GAS)
    cols.append(EMISSIONS)
    return cols


def main():
    ARCH.mkdir(parents=True, exist_ok=True)
    CJSON.mkdir(parents=True, exist_ok=True)
    for table, cfg in TABLES.items():
        cols = build_columns(cfg)
        # architecture CSV (English data-language description)
        with open(ARCH / f"{table}.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(ARCH_HEADER)
            for name, typ, _pt, en, _es, extra in cols:
                w.writerow(
                    [
                        name,
                        typ,
                        en,
                        extra.get("temporal_coverage", ""),
                        "no",
                        extra.get("directory_column", ""),
                        extra.get("measurement_unit", ""),
                        "no",
                        extra.get("observations", ""),
                        extra.get("original_name", ""),
                    ]
                )
        # columns_json for bulk_upsert (trilingual)
        cj = []
        for name, typ, pt, en, es, extra in cols:
            entry = {
                "name": name,
                "bigquery_type": typ,
                "description": pt,  # bare description -> PT
                "description_pt": pt,
                "description_en": en,
                "description_es": es,
                "covered_by_dictionary": False,
                "has_sensitive_data": False,
                "is_partition": name == "year",
                "is_primary_key": False,
            }
            if extra.get("directory_column"):
                entry["directory_column"] = extra["directory_column"]
            if extra.get("measurement_unit"):
                entry["measurement_unit"] = extra["measurement_unit"]
            cj.append(entry)
        (CJSON / f"{table}.json").write_text(
            json.dumps(cj, ensure_ascii=False, indent=2)
        )
        print(
            f"{table}: {len(cols)} columns -> architecture/{table}.csv, columns_json/{table}.json"
        )


if __name__ == "__main__":
    main()
