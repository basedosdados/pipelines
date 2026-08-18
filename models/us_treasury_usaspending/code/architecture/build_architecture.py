"""Build the us_treasury_usaspending architecture CSVs.

Emits one CSV per table, in the Data Basis architecture schema:

    name, bigquery_type, description, temporal_coverage, covered_by_dictionary,
    directory_column, measurement_unit, has_sensitive_data, observations,
    original_name

Column inventory comes from the headers of the USAspending Award Data Archive
files; types, units and dictionary coverage are assigned here; wording comes
from ``labels.py`` and ``descriptions.py``.

Also emits ``dicionario.csv`` (the architecture of the dictionary table) and
``dicionario_data.csv`` (its actual rows, built from the DATA Act element
dictionary domain values — the dictionary table is reference metadata, not bulk
data, so it is produced here rather than by the cleaning step).

Usage:
    uv run python models/us_treasury_usaspending/code/architecture/build_architecture.py
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from descriptions import CODE_PAIRS, DESCRIPTIONS  # noqa: E402
from labels import FLAG_TEMPLATE, FLAGS  # noqa: E402

DATA_DIR = Path(
    os.environ.get(
        "USASPENDING_DATA_DIR",
        Path.home() / "Downloads" / "us_treasury_usaspending_data",
    )
)
DICT_URL = "https://api.usaspending.gov/api/v2/references/data_dictionary/"
DICT_CACHE = DATA_DIR / "ref" / "data_dictionary.json"

# Headers of the two archive families, in source order. Kept in the repo so the
# build does not require a multi-gigabyte download.
HEADERS_FILE = HERE / "source_headers.json"

# --------------------------------------------------------------------------
# Column naming
# --------------------------------------------------------------------------
PARTITION = "fiscal_year"

RENAMES = {
    "action_date_fiscal_year": PARTITION,
    # BigQuery rejects hyphens and leading digits in column names.
    "outlayed_amount_from_COVID-19_supplementals_for_overall_award": "outlayed_amount_from_covid19_supplementals_for_overall_award",
    "obligated_amount_from_COVID-19_supplementals_for_overall_award": "obligated_amount_from_covid19_supplementals_for_overall_award",
    "1862_land_grant_college": "land_grant_college_1862",
    "1890_land_grant_college": "land_grant_college_1890",
    "1994_land_grant_college": "land_grant_college_1994",
}

# --------------------------------------------------------------------------
# Types
# --------------------------------------------------------------------------
MONEY = {
    "federal_action_obligation",
    "total_dollars_obligated",
    "total_obligated_amount",
    "total_outlayed_amount_for_overall_award",
    "base_and_exercised_options_value",
    "current_total_value_of_award",
    "base_and_all_options_value",
    "potential_total_value_of_award",
    "outlayed_amount_from_COVID-19_supplementals_for_overall_award",
    "obligated_amount_from_COVID-19_supplementals_for_overall_award",
    "outlayed_amount_from_IIJA_supplemental_for_overall_award",
    "obligated_amount_from_IIJA_supplemental_for_overall_award",
    "indirect_cost_federal_share_amount",
    "non_federal_funding_amount",
    "total_non_federal_funding_amount",
    "face_value_of_loan",
    "total_face_value_of_loan",
    "original_loan_subsidy_cost",
    "total_loan_subsidy_cost",
    "generated_pragmatic_obligations",
    *(f"highly_compensated_officer_{i}_amount" for i in range(1, 6)),
}

DATES = {
    "action_date",
    "period_of_performance_start_date",
    "period_of_performance_current_end_date",
    "period_of_performance_potential_end_date",
    "ordering_period_end_date",
    "solicitation_date",
}

# 'YYYY-MM-DD HH:MM:SS+00' in the source — TIMESTAMP, not DATETIME, because the
# value carries a UTC offset that a DATETIME cast would reject.
TIMESTAMPS = {"initial_report_date", "last_modified_date"}

COUNTS = {
    "number_of_actions": "action",
    "number_of_offers_received": "offer",
}

PERCENTS = {"price_evaluation_adjustment_preference_percent_difference"}

# --------------------------------------------------------------------------
# Directory links (shared entities live in br_bd_diretorios_us)
# --------------------------------------------------------------------------
DIRECTORY = {
    "prime_award_transaction_recipient_state_fips_code": "br_bd_diretorios_us.state:id_state",
    "prime_award_transaction_place_of_performance_state_fips_code": "br_bd_diretorios_us.state:id_state",
    "prime_award_transaction_recipient_county_fips_code": "br_bd_diretorios_us.county:id_county",
    "prime_award_transaction_place_of_performance_county_fips_code": "br_bd_diretorios_us.county:id_county",
}

# --------------------------------------------------------------------------
# Dictionary coverage beyond the code columns detected from the source's own
# enumerated domain values.
# --------------------------------------------------------------------------
DICT_EXTRA = {
    "award_or_idv_flag",
    "primary_place_of_performance_scope",
    "organizational_type",
    "hospital_flag",
    "small_disadvantaged_business",
    "small_business_competitiveness_demonstration_program",
    "disaster_emergency_fund_codes_for_overall_award",
}

# Columns whose "domain" in the source dictionary is a pointer to an external
# reference system rather than an enumerated list — not dictionary-covered.
DICT_NEVER = {
    "awarding_agency_code",
    "awarding_agency_name",
    "awarding_sub_agency_code",
    "awarding_sub_agency_name",
    "awarding_office_code",
    "awarding_office_name",
    "funding_agency_code",
    "funding_agency_name",
    "funding_sub_agency_code",
    "funding_sub_agency_name",
    "funding_office_code",
    "funding_office_name",
    "treasury_accounts_funding_this_award",
    "federal_accounts_funding_this_award",
    "object_classes_funding_this_award",
    "program_activities_funding_this_award",
    "cfda_number",
    "cfda_title",
    "naics_code",
    "naics_description",
    "product_or_service_code",
    "product_or_service_code_description",
    "country_of_product_or_service_origin_code",
    "country_of_product_or_service_origin",
    "dod_claimant_program_code",
    "dod_claimant_program_description",
    "dod_acquisition_program_code",
    "dod_acquisition_program_description",
    "parent_award_agency_id",
    "parent_award_agency_name",
    "recipient_city_code",
    "recipient_country_code",
    "recipient_country_name",
    "recipient_state_code",
    "recipient_state_name",
    "recipient_county_name",
    "recipient_city_name",
    "recipient_zip_code",
    "recipient_zip_4_code",
    "recipient_zip_last_4_code",
    "primary_place_of_performance_country_code",
    "primary_place_of_performance_country_name",
    "primary_place_of_performance_city_name",
    "primary_place_of_performance_county_name",
    "primary_place_of_performance_state_code",
    "primary_place_of_performance_state_name",
    "primary_place_of_performance_zip_4",
    "primary_place_of_performance_code",
    "prime_award_transaction_recipient_state_fips_code",
    "prime_award_transaction_recipient_county_fips_code",
    "prime_award_transaction_place_of_performance_state_fips_code",
    "prime_award_transaction_place_of_performance_county_fips_code",
    "prime_award_transaction_recipient_cd_original",
    "prime_award_transaction_recipient_cd_current",
    "prime_award_transaction_place_of_performance_cd_original",
    "prime_award_transaction_place_of_performance_cd_current",
}

# --------------------------------------------------------------------------
# Per-column notes
# --------------------------------------------------------------------------
OBSERVATIONS = {
    PARTITION: (
        "Coluna de partição. Renomeada de action_date_fiscal_year. O ano fiscal federal "
        "norte-americano começa em 1º de outubro do ano civil anterior, portanto não "
        "coincide com o ano civil e não é ligada ao diretório de tempo"
    ),
    "naics_code": (
        "A vintage do NAICS acompanha a data da ação e varia ao longo da série, de modo "
        "que não existe um único diretório de vintage ao qual a coluna possa ser ligada"
    ),
    "prime_award_transaction_recipient_county_fips_code": (
        "A fonte emite parte dos valores corrompidos por conversão numérica — o código do condado perde o zero à esquerda e ganha o sufixo '.0' (Franklin/OH, 39049, chega como '3949.0') —, corrigidos no modelo dbt. Registros agregados por estado usam o sentinela '<estado>000', que não é um condado. Códigos aposentados (Connecticut antes de 2022, Dade antes de Miami-Dade, Ormsby/NV) não resolvem contra o diretório de vintage corrente, por isso a coluna não tem teste de integridade referencial"
    ),
    "prime_award_transaction_place_of_performance_county_fips_code": (
        "A fonte emite parte dos valores corrompidos por conversão numérica — o código do condado perde o zero à esquerda e ganha o sufixo '.0' (Franklin/OH, 39049, chega como '3949.0') —, corrigidos no modelo dbt. Registros agregados por estado usam o sentinela '<estado>000', que não é um condado. Códigos aposentados (Connecticut antes de 2022, Dade antes de Miami-Dade, Ormsby/NV) não resolvem contra o diretório de vintage corrente, por isso a coluna não tem teste de integridade referencial"
    ),
    "recipient_duns": "Descontinuado em abril de 2022, quando o UEI passou a ser o identificador oficial",
    "recipient_parent_duns": "Descontinuado em abril de 2022, quando o UEI passou a ser o identificador oficial",
    "recipient_state_code": "Sigla postal; a ligação ao diretório é feita pela coluna de código FIPS",
    "primary_place_of_performance_state_code": "Sigla postal; a ligação ao diretório é feita pela coluna de código FIPS",
    "outlayed_amount_from_COVID-19_supplementals_for_overall_award": "Renomeada da coluna de origem, que contém hífen",
    "obligated_amount_from_COVID-19_supplementals_for_overall_award": "Renomeada da coluna de origem, que contém hífen",
    "1862_land_grant_college": "Renomeada da coluna de origem, que começa com dígito",
    "1890_land_grant_college": "Renomeada da coluna de origem, que começa com dígito",
    "1994_land_grant_college": "Renomeada da coluna de origem, que começa com dígito",
    "usaspending_permalink": "Endereço construído pela fonte a partir da chave única do auxílio ou contrato",
    **{
        f"highly_compensated_officer_{i}_{k}": (
            "Divulgação obrigatória sob a FFATA para beneficiários que atendem aos "
            "critérios de receita e financiamento federal; em branco nos demais casos"
        )
        for i in range(1, 6)
        for k in ("name", "amount")
    },
}

FLAG_OBSERVATION = "Valores armazenados como 't' e 'f' na fonte"


def load_headers() -> dict[str, list[str]]:
    return json.loads(HEADERS_FILE.read_text())


def load_data_dictionary() -> dict:
    """Element dictionary keyed by the archive column name."""
    if DICT_CACHE.exists():
        raw = json.loads(DICT_CACHE.read_text())
    else:
        DICT_CACHE.parent.mkdir(parents=True, exist_ok=True)
        with urllib.request.urlopen(DICT_URL, timeout=120) as r:
            raw = json.loads(r.read())
        DICT_CACHE.write_text(json.dumps(raw))
    out: dict[str, dict] = {}
    for row in raw["document"]["rows"]:
        award_element = (row[7] or "").strip()
        if not award_element:
            continue
        for name in (x.strip() for x in award_element.split(",")):
            if name:
                out.setdefault(
                    name,
                    {
                        "element": row[0],
                        "definition": row[1],
                        "domain": row[4] or "",
                    },
                )
    return out


def enumerated_domain(domain: str) -> list[tuple[str, str]]:
    """Parse 'A = Label' lines; return [] when the domain is an external pointer."""
    if not domain or re.match(
        r"\s*(see|refer|according|the authoritative|data)\b", domain, re.I
    ):
        return []
    pairs = []
    for line in domain.replace("_x000D_", "").split("\n"):
        line = line.strip()
        if not line or line.endswith(":"):
            continue
        m = re.match(r"^(.*?)\s*=\s*(.+)$", line)
        if not m:
            continue
        key, val = m.group(1).strip(), m.group(2).strip()
        if key in ("", "N/A", "Blank", "(empty)"):
            continue
        pairs.append((key, val))
    return pairs


def bigquery_type(src: str) -> str:
    if src == PARTITION or src == "action_date_fiscal_year":
        return "INT64"
    if src in MONEY or src in PERCENTS:
        return "FLOAT64"
    if src in COUNTS:
        return "INT64"
    if src in DATES:
        return "DATE"
    if src in TIMESTAMPS:
        return "TIMESTAMP"
    return "STRING"


def measurement_unit(src: str) -> str:
    if src == "action_date_fiscal_year" or src == PARTITION:
        return "year"
    if src in MONEY:
        return "USD"
    if src in PERCENTS:
        return "percent"
    return COUNTS.get(src, "")


def describe(src: str) -> tuple[str, str, str]:
    if src in FLAGS:
        return tuple(
            tpl.format(lbl)
            for tpl, lbl in zip(FLAG_TEMPLATE, FLAGS[src], strict=True)
        )  # type: ignore[return-value]
    if src in DESCRIPTIONS:
        return DESCRIPTIONS[src]
    for code_col, (labels, pt, en, es) in CODE_PAIRS.items():
        if src == code_col:
            return (f"Código de {pt}", f"Code for {en}", f"Código de {es}")
        if src in labels:
            return (
                f"Descrição de {pt}",
                f"Description of {en}",
                f"Descripción de {es}",
            )
    raise KeyError(f"no description authored for column {src!r}")


def covered_by_dictionary(src: str, ddict: dict) -> str:
    if src in DICT_NEVER:
        return "no"
    if src in FLAGS or src in DICT_EXTRA:
        return "yes"
    # label halves of a code/label pair store readable text, not codes
    for code_col, (labels, *_rest) in CODE_PAIRS.items():
        if src in labels:
            return "no"
        if src == code_col:
            return (
                "yes"
                if enumerated_domain(ddict.get(src, {}).get("domain", ""))
                else "no"
            )
    return "no"


def build_table(name: str, header: list[str], ddict: dict) -> list[dict]:
    ordered = [PARTITION] + [
        c for c in header if c != "action_date_fiscal_year"
    ]
    rows = []
    for src in ordered:
        original = "action_date_fiscal_year" if src == PARTITION else src
        key = PARTITION if src == PARTITION else original
        obs = OBSERVATIONS.get(key, "") or OBSERVATIONS.get(original, "")
        if original in FLAGS and not obs:
            obs = FLAG_OBSERVATION
        pt, en, es = describe(key)
        rows.append(
            {
                "name": RENAMES.get(src, src),
                "bigquery_type": bigquery_type(original),
                "description": pt,
                "description_en": en,
                "description_es": es,
                "temporal_coverage": "",
                "covered_by_dictionary": covered_by_dictionary(
                    original, ddict
                ),
                "directory_column": DIRECTORY.get(original, ""),
                "measurement_unit": measurement_unit(original),
                "has_sensitive_data": "no",
                "observations": obs,
                "original_name": original,
            }
        )
    return rows


DICIONARIO_ARCH = [
    {
        "name": "id_tabela",
        "bigquery_type": "STRING",
        "description": "Nome da tabela à qual a chave se aplica",
        "description_en": "Name of the table the key applies to",
        "description_es": "Nombre de la tabla a la que se aplica la clave",
    },
    {
        "name": "nome_coluna",
        "bigquery_type": "STRING",
        "description": "Nome da coluna à qual a chave se aplica",
        "description_en": "Name of the column the key applies to",
        "description_es": "Nombre de la columna a la que se aplica la clave",
    },
    {
        "name": "chave",
        "bigquery_type": "STRING",
        "description": "Valor codificado armazenado na coluna",
        "description_en": "Coded value stored in the column",
        "description_es": "Valor codificado almacenado en la columna",
    },
    {
        "name": "cobertura_temporal",
        "bigquery_type": "STRING",
        "description": "Período em que a chave é válida",
        "description_en": "Period over which the key is valid",
        "description_es": "Período en que la clave es válida",
    },
    {
        "name": "valor",
        "bigquery_type": "STRING",
        "description": "Significado da chave",
        "description_en": "Meaning of the key",
        "description_es": "Significado de la clave",
    },
]

FIELDS = [
    "name",
    "bigquery_type",
    "description",
    "description_en",
    "description_es",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
]


DICIONARIO_DEFAULTS = {
    "covered_by_dictionary": "no",
    "has_sensitive_data": "no",
}
for _row in DICIONARIO_ARCH:
    _row.update(DICIONARIO_DEFAULTS)


def write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in FIELDS})


def build_dicionario_data(
    tables: dict[str, list[dict]], ddict: dict
) -> list[dict]:
    """Rows of the dicionario table, from the source's enumerated domain values."""
    out = []
    seen = set()
    for table, rows in tables.items():
        for row in rows:
            if row["covered_by_dictionary"] != "yes":
                continue
            src = row["original_name"]
            pairs = enumerated_domain(ddict.get(src, {}).get("domain", ""))
            if not pairs and src in FLAGS:
                pairs = [("f", "False"), ("t", "True")]
            for key, val in pairs:
                sig = (table, row["name"], key)
                if sig in seen:
                    continue
                seen.add(sig)
                out.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": row["name"],
                        "chave": key,
                        "cobertura_temporal": "",
                        "valor": val,
                    }
                )
    return out


def main() -> None:
    headers = load_headers()
    ddict = load_data_dictionary()

    tables = {
        "contract_transaction": build_table(
            "contract_transaction", headers["contracts"], ddict
        ),
        "assistance_transaction": build_table(
            "assistance_transaction", headers["assistance"], ddict
        ),
    }
    for name, rows in tables.items():
        write_csv(HERE / f"{name}.csv", rows)
        print(f"{name}.csv: {len(rows)} columns")

    write_csv(HERE / "dicionario.csv", DICIONARIO_ARCH)
    print(f"dicionario.csv: {len(DICIONARIO_ARCH)} columns")

    dic = build_dicionario_data(tables, ddict)
    out = DATA_DIR / "ref" / "dicionario_data.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "id_tabela",
                "nome_coluna",
                "chave",
                "cobertura_temporal",
                "valor",
            ],
        )
        w.writeheader()
        w.writerows(dic)
    print(f"dicionario rows: {len(dic)} -> {out}")

    # sanity summary
    for name, rows in tables.items():
        types: dict[str, int] = {}
        for r in rows:
            types[r["bigquery_type"]] = types.get(r["bigquery_type"], 0) + 1
        n_dict = sum(1 for r in rows if r["covered_by_dictionary"] == "yes")
        n_dir = sum(1 for r in rows if r["directory_column"])
        print(f"  {name}: types={types} dictionary={n_dict} directory={n_dir}")


if __name__ == "__main__":
    main()
