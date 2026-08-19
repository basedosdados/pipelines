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

# Which half of each code/label pair really holds the code, per table, as
# measured by ../code_label_orientation.py. The Contracts archive inverts 24 of
# them — `action_type_code` holds "OTHER ADMINISTRATIVE ACTION" while
# `action_type` holds "M" — so the column names cannot be trusted and this file
# is the authority.
ORIENTATION_FILE = HERE.parent / "code_label_orientation.json"

# --------------------------------------------------------------------------
# Column naming
# --------------------------------------------------------------------------
PARTITION = "fiscal_year"

# Renames applied by the *cleaning* step, so the staging column carries this
# name. Limited to what BigQuery cannot accept (hyphens, leading digits) plus
# the partition column, whose name has to match the hive directory.
RENAMES = {
    "action_date_fiscal_year": PARTITION,
    # BigQuery rejects hyphens and leading digits in column names.
    "outlayed_amount_from_COVID-19_supplementals_for_overall_award": "outlayed_amount_from_covid19_supplementals_for_overall_award",
    "obligated_amount_from_COVID-19_supplementals_for_overall_award": "obligated_amount_from_covid19_supplementals_for_overall_award",
    "1862_land_grant_college": "land_grant_college_1862",
    "1890_land_grant_college": "land_grant_college_1890",
    "1994_land_grant_college": "land_grant_college_1994",
}

# Renames applied by the *dbt model* rather than the cleaning step. Staging
# stays faithful to the source spelling; the published column takes the house
# name. Doing it here rather than in the transform means a rename costs a model
# rebuild instead of re-cleaning a quarter of a billion rows.
MODEL_RENAMES = {
    "contract_transaction_unique_key": "contract_transaction_id",
    # A directory owns these, so they are identifiers rather than codes and take
    # the `_id` suffix. The FIPS scheme is named in each column's description,
    # so dropping "fips_code" from the name loses nothing.
    "prime_award_transaction_recipient_state_fips_code": "prime_award_transaction_recipient_state_id",
    "prime_award_transaction_recipient_county_fips_code": "prime_award_transaction_recipient_county_id",
    "prime_award_transaction_place_of_performance_state_fips_code": "prime_award_transaction_place_of_performance_state_id",
    "prime_award_transaction_place_of_performance_county_fips_code": "prime_award_transaction_place_of_performance_county_id",
    "contract_award_unique_key": "contract_award_id",
    "assistance_transaction_unique_key": "assistance_transaction_id",
    "assistance_award_unique_key": "assistance_award_id",
}


def published_name(src: str) -> str:
    """Column name as published, after both rename layers."""
    return MODEL_RENAMES.get(src, RENAMES.get(src, src))


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
# Stored as 'f'/'t' like the recipient business-type flags, even though the
# published domain writes them as "Y = Hospital Flag" style prose.
BOOLEAN_EXTRA = {
    "hospital_flag",
    "small_disadvantaged_business",
    "small_business_competitiveness_demonstration_program",
}

DICT_EXTRA = {}

# Columns whose "domain" in the source dictionary is a pointer to an external
# reference system rather than an enumerated list — not dictionary-covered.
DICT_NEVER = {
    # Stores readable labels ("AWARD"/"IDV"), not codes.
    "award_or_idv_flag",
    # Stores the label itself ("CORPORATE NOT TAX EXEMPT").
    "organizational_type",
    # Stores self-describing "code: label" strings, concatenated when an award
    # draws on several funds, so no single key resolves a value.
    "disaster_emergency_fund_codes_for_overall_award",
    # Stores readable labels ("SINGLE ZIP CODE"), not codes, so the dictionary
    # has nothing to resolve.
    "primary_place_of_performance_scope",
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


def load_orientation() -> dict[str, dict]:
    if not ORIENTATION_FILE.exists():
        return {}
    return json.loads(ORIENTATION_FILE.read_text())


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


def describe(
    src: str, orientation: dict | None = None
) -> tuple[str, str, str]:
    if src in FLAGS:
        return tuple(
            tpl.format(lbl)
            for tpl, lbl in zip(FLAG_TEMPLATE, FLAGS[src], strict=True)
        )  # type: ignore[return-value]
    if src in DESCRIPTIONS:
        return DESCRIPTIONS[src]
    orientation = orientation or {}
    for code_col, (labels, pt, en, es) in CODE_PAIRS.items():
        if src != code_col and src not in labels:
            continue
        holder = (orientation.get(code_col) or {}).get("code", code_col)
        if src == holder:
            return (f"Código de {pt}", f"Code for {en}", f"Código de {es}")
        return (
            f"Descrição de {pt}",
            f"Description of {en}",
            f"Descripción de {es}",
        )
    raise KeyError(f"no description authored for column {src!r}")


def covered_by_dictionary(
    src: str, ddict: dict, orientation: dict | None = None
) -> str:
    if src in DICT_NEVER:
        return "no"
    if src in FLAGS or src in BOOLEAN_EXTRA or src in DICT_EXTRA:
        return "yes"
    # Only the half that actually holds the code is dictionary-covered, and
    # which half that is depends on the table — see ORIENTATION_FILE.
    orientation = orientation or {}
    for code_col, (labels, *_rest) in CODE_PAIRS.items():
        if src != code_col and src not in labels:
            continue
        holder = (orientation.get(code_col) or {}).get("code", code_col)
        if src != holder:
            return "no"
        return (
            "yes"
            if enumerated_domain(ddict.get(code_col, {}).get("domain", ""))
            else "no"
        )
    return "no"


def build_table(
    name: str, header: list[str], ddict: dict, orientation: dict | None = None
) -> list[dict]:
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
        pt, en, es = describe(key, orientation)
        rows.append(
            {
                "name": published_name(src),
                "bigquery_type": bigquery_type(original),
                "description": pt,
                "description_en": en,
                "description_es": es,
                "temporal_coverage": "",
                "covered_by_dictionary": covered_by_dictionary(
                    original, ddict, orientation
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


def domain_source(src: str, ddict: dict) -> str:
    """Domain text for a column, resolved through its code/label pair.

    An inverted pair means the column holding the code is not the one the
    dictionary indexes by name, so the concept's domain has to be looked up via
    the pair rather than the column.
    """
    for code_col, (labels, *_rest) in CODE_PAIRS.items():
        if src == code_col or src in labels:
            return ddict.get(code_col, {}).get("domain", "")
    return ddict.get(src, {}).get("domain", "")


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
            if src in FLAGS or src in BOOLEAN_EXTRA:
                # The published domain says "F = False", but the archive stores
                # lowercase 'f'/'t'; the dictionary has to match the data.
                pairs = [("f", "False"), ("t", "True")]
            else:
                pairs = enumerated_domain(domain_source(src, ddict))
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

    orientation = load_orientation()
    tables = {
        "contract_transaction": build_table(
            "contract_transaction",
            headers["contracts"],
            ddict,
            orientation.get("contract_transaction"),
        ),
        "assistance_transaction": build_table(
            "assistance_transaction",
            headers["assistance"],
            ddict,
            orientation.get("assistance_transaction"),
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
