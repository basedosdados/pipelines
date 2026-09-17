"""Generate metadata/registration_spec.json for us_bls_qcew.

Per-table trilingual names + descriptions, observation-level entities, coverage
years, and the BD-Pro flag — the input for backend metadata registration. Pure
string assembly; reference-object UUIDs are supplied at registration time.

Run: uv run python models/us_bls_qcew/code/build_registration_spec.py
"""

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE.parent / "metadata"

CLS_LABEL = {"naics": "NAICS", "sic": "SIC"}
FREQ = {
    "quarterly": {
        "en": "Quarterly",
        "pt": "Trimestral",
        "es": "Trimestral",
        "en_w": "quarterly",
        "pt_w": "trimestrais",
        "es_w": "trimestrales",
    },
    "annual": {
        "en": "Annual",
        "pt": "Anual",
        "es": "Anual",
        "en_w": "annual-average",
        "pt_w": "de médias anuais",
        "es_w": "de promedios anuales",
    },
}
GEO = {
    "national": {
        "en": "National",
        "pt": "Nacional",
        "es": "Nacional",
        "en_w": "national",
        "pt_w": "nacional",
        "es_w": "nacional",
        "ol": "country",
    },
    "state": {
        "en": "State",
        "pt": "Estado",
        "es": "Estado",
        "en_w": "state",
        "pt_w": "estadual",
        "es_w": "estatal",
        "ol": "state",
    },
    "county": {
        "en": "County",
        "pt": "Condado",
        "es": "Condado",
        "en_w": "county",
        "pt_w": "de condado",
        "es_w": "de condado",
        "ol": "county",
    },
    "metro": {
        "en": "Metropolitan Area",
        "pt": "Área Metropolitana",
        "es": "Área Metropolitana",
        "en_w": "metropolitan-area",
        "pt_w": "de área metropolitana",
        "es_w": "de área metropolitana",
        "ol": "metropolitan_area",
    },
}
COVERAGE = {"naics": (1990, 2025), "sic": (1975, 2000)}


def table_spec(cls, freq, geo):
    c, f, g = CLS_LABEL[cls], FREQ[freq], GEO[geo]
    y0, y1 = COVERAGE[cls]
    slug = f"{cls}_{freq}_{geo}"
    names = {
        "en": f"{c}, {f['en']}, {g['en']}",
        "pt": f"{c}, {f['pt']}, {g['pt']}",
        "es": f"{c}, {f['es']}, {g['es']}",
    }
    extra_en = (
        ", plus location quotients and over-the-year changes"
        if cls == "naics"
        else ""
    )
    extra_pt = (
        ", além de quocientes locacionais e variações interanuais"
        if cls == "naics"
        else ""
    )
    extra_es = (
        ", además de cocientes de localización y variaciones interanuales"
        if cls == "naics"
        else ""
    )
    emp_en = (
        "monthly employment"
        if freq == "quarterly"
        else "annual average employment"
    )
    emp_pt = "emprego mensal" if freq == "quarterly" else "emprego médio anual"
    emp_es = "empleo mensual" if freq == "quarterly" else "empleo medio anual"
    descs = {
        "en": (
            f"QCEW {c}-based {f['en_w']} employment and wage totals at the {g['en_w']} level, "
            f"{y0}-{y1}. One row per area, ownership sector, industry, aggregation level, and size "
            f"class, with establishment counts, {emp_en}, and wages{extra_en}."
        ),
        "pt": (
            f"Totais {f['pt_w']} de emprego e salários do QCEW baseados em {c} no nível {g['pt_w']}, "
            f"{y0}-{y1}. Uma linha por área, setor de propriedade, indústria, nível de agregação e "
            f"classe de tamanho, com contagens de estabelecimentos, {emp_pt} e salários{extra_pt}."
        ),
        "es": (
            f"Totales {f['es_w']} de empleo y salarios del QCEW basados en {c} a nivel {g['es_w']}, "
            f"{y0}-{y1}. Una fila por área, sector de propiedad, industria, nivel de agregación y "
            f"clase de tamaño, con conteos de establecimientos, {emp_es} y salarios{extra_es}."
        ),
    }
    # Observation levels = temporal grain (year, plus quarter for quarterly) then
    # the geographic entity (omitted for national, where it is constant) then
    # industry. National tables therefore carry no geo OL.
    ol_entities = (
        ["year"]
        + (["quarter"] if freq == "quarterly" else [])
        + ([g["ol"]] if geo != "national" else [])
        + ["industry"]
    )
    return {
        "slug": slug,
        "classification": cls,
        "freq": freq,
        "geo": geo,
        "names": names,
        "descriptions": descs,
        "coverage": {"start_year": y0, "end_year": y1},
        "ol_entities": ol_entities,
        "update_entity": "quarter" if freq == "quarterly" else "year",
        "bdpro": cls == "naics" and freq == "quarterly",
        "columns_json": f"code/columns_json/{slug}.json",
        "cloud_table": {
            "gcp_project_id": "basedosdados-dev",
            "gcp_dataset_id": "us_bls_qcew",
            "gcp_table_id": slug,
        },
    }


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    tables = []
    for cls in ("naics", "sic"):
        for freq in ("quarterly", "annual"):
            for geo in ("national", "state", "county", "metro"):
                tables.append(table_spec(cls, freq, geo))
    tables.append(
        {
            "slug": "dicionario",
            "classification": None,
            "freq": None,
            "geo": None,
            "names": {
                "en": "Dictionary",
                "pt": "Dicionário",
                "es": "Diccionario",
            },
            "descriptions": {
                "en": "Dictionary mapping the coded columns of us_bls_qcew (ownership, aggregation level, size class, industry, and area) to their human-readable labels.",
                "pt": "Dicionário que mapeia as colunas codificadas do us_bls_qcew (propriedade, nível de agregação, classe de tamanho, indústria e área) para seus rótulos legíveis.",
                "es": "Diccionario que asigna las columnas codificadas de us_bls_qcew (propiedad, nivel de agregación, clase de tamaño, industria y área) a sus etiquetas legibles.",
            },
            # pyrefly: ignore [bad-assignment]
            "coverage": None,
            "ol_entities": [],
            # pyrefly: ignore [bad-assignment]
            "update_entity": None,
            "bdpro": False,
            "columns_json": "code/columns_json/dicionario.json",
            "cloud_table": {
                "gcp_project_id": "basedosdados-dev",
                "gcp_dataset_id": "us_bls_qcew",
                "gcp_table_id": "dicionario",
            },
        }
    )
    spec = {
        "dataset": {
            "slug": "qcew",
            "id": "441d8d57-bd23-400d-9f52-664155ae9296",
        },
        "raw_sources": {
            "naics": "180c2c08-a8a7-4b30-88a8-e200c77acf92",
            "sic": "afa5ebe0-8d1a-450a-870b-83cacdb084d1",
        },
        "ref_ids": {
            "org_us_bls": "b80682c0-f655-4ca7-ad17-b5b6bfc8ee64",
            "theme_economics": "ad6a413a-e882-4dd6-a497-8a62eec8511b",
            "area_us": "61a2c232-c649-4b41-a5a3-1467b7393e11",
            "status_under_review": "47208305-325a-4da9-9222-ac6849405b78",
            "status_published": "e16221de-ac30-4926-83d3-de219998dab3",
            "license_cc0": "7fb71004-2abe-4fc8-a258-e2aac27c71d9",
            "availability_online": "dd396d7d-0264-4c1f-bf0d-6efe2dc89cbe",
            "account": "57",
            "entity": {
                "country": "b9bfd6a6-bc3f-460c-8a93-f695891d64d3",
                "state": "839765a7-9c7a-44bd-bb88-357cedba03f6",
                "county": "01069a8a-5c20-4969-b295-a55082a828e8",
                "metropolitan_area": "69b4d185-0a4a-43e2-9b06-dc9068eb58f2",
                "industry": "a964e728-02af-4023-8760-b8663d2b3ef7",
                "quarter": "7b7f7bf4-785f-4d8e-8ffd-10ef825ebf44",
                "year": "e1bf146e-b6bb-4b65-bee7-c800876e80a5",
            },
        },
        "tables": tables,
    }
    (OUT / "registration_spec.json").write_text(
        json.dumps(spec, ensure_ascii=False, indent=2)
    )
    print(f"wrote {OUT / 'registration_spec.json'} with {len(tables)} tables")
    for t in tables:
        print(
            f"  {t['slug']}: bdpro={t['bdpro']} ol={t['ol_entities']} name_en={t['names']['en']!r}"
        )


if __name__ == "__main__":
    main()
