"""Download + cleaning transform for us_epa_tri (shared by the recurring pipeline
and the one-shot bootstrap in models/us_epa_tri/code/).

Pure functions (no Prefect) so they are importable and testable. The Prefect
tasks in tasks.py wrap them; the bootstrap CLI imports ``clean_all`` directly.
Column order comes from the architecture CSVs (the single source of truth).

Source: EPA "TRI Basic Data Files", one national CSV per reporting year with
122 columns (August 2024 layout), one row per form (Form R or Form A) filed by
a facility for one chemical. Quantities are pounds, except dioxin and
dioxin-like compounds, reported in grams.

Output: all-STRING hive-partitioned parquet (``<table>/year=YYYY/data.parquet``)
for facility, form and release; unpartitioned parquet for chemical and
dicionario. ``upload_to_gcs`` builds the staging schema from a stringified
header, so typed parquet is rejected; the dbt models ``safe_cast`` every column
back to its architecture type.
"""

import csv
import logging
import re
import time
from collections import Counter
from datetime import date, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_epa_tri.constants import constants

log = logging.getLogger("us_epa_tri")

_ARCH = constants.ARCHITECTURE_DIR.value
_HEADERS = constants.HEADERS.value
_G_PER_LB = constants.GRAMS_PER_POUND.value

# ── release categories ──────────────────────────────────────────────────────
# (source column, code, management group, label EN, label PT, label ES,
#  temporal coverage). The order is the source's column order. Only these
# columns are unpivoted into `release`; EPA's own totals are mutually exclusive
# sums of them, so summing a form's rows reproduces on-site + off-site totals.
RELEASE_CATEGORIES = [
    (
        "5.1 - FUGITIVE AIR",
        "5.1",
        "on_site_release",
        "Fugitive air emissions",
        "Emissões fugitivas para o ar",
        "Emisiones fugitivas al aire",
        "",
    ),
    (
        "5.2 - STACK AIR",
        "5.2",
        "on_site_release",
        "Stack air emissions",
        "Emissões por chaminé",
        "Emisiones por chimenea",
        "",
    ),
    (
        "5.3 - WATER",
        "5.3",
        "on_site_release",
        "Surface water discharges",
        "Lançamentos em águas superficiais",
        "Vertidos a aguas superficiales",
        "",
    ),
    (
        "5.4 - UNDERGROUND",
        "5.4",
        "on_site_release",
        "Underground injection (all wells; replaced by 5.4.1 and 5.4.2 in 1996)",
        "Injeção subterrânea (todos os poços; substituída por 5.4.1 e 5.4.2 em 1996)",
        "Inyección subterránea (todos los pozos; reemplazada por 5.4.1 y 5.4.2 en 1996)",
        "1987(1)1995",
    ),
    (
        "5.4.1 - UNDERGROUND CL I",
        "5.4.1",
        "on_site_release",
        "Underground injection to Class I wells",
        "Injeção subterrânea em poços Classe I",
        "Inyección subterránea en pozos Clase I",
        "1996(1)",
    ),
    (
        "5.4.2 - UNDERGROUND C II-V",
        "5.4.2",
        "on_site_release",
        "Underground injection to Class II-V wells",
        "Injeção subterrânea em poços Classe II-V",
        "Inyección subterránea en pozos Clase II-V",
        "1996(1)",
    ),
    (
        "5.5.1 - LANDFILLS",
        "5.5.1",
        "on_site_release",
        "On-site landfills (all; replaced by 5.5.1A and 5.5.1B in 1996)",
        "Aterros no local (todos; substituído por 5.5.1A e 5.5.1B em 1996)",
        "Rellenos en el sitio (todos; reemplazado por 5.5.1A y 5.5.1B en 1996)",
        "1987(1)1995",
    ),
    (
        "5.5.1A - RCRA C LANDFILL",
        "5.5.1A",
        "on_site_release",
        "On-site RCRA Subtitle C landfills",
        "Aterros RCRA Subtítulo C no local",
        "Rellenos RCRA Subtítulo C en el sitio",
        "1996(1)",
    ),
    (
        "5.5.1B - OTHER LANDFILLS",
        "5.5.1B",
        "on_site_release",
        "Other on-site landfills",
        "Outros aterros no local",
        "Otros rellenos en el sitio",
        "1996(1)",
    ),
    (
        "5.5.2 - LAND TREATMENT",
        "5.5.2",
        "on_site_release",
        "On-site land treatment/application farming",
        "Tratamento no solo/aplicação agrícola no local",
        "Tratamiento en suelo/aplicación agrícola en el sitio",
        "",
    ),
    (
        "5.5.3 - SURFACE IMPNDMNT",
        "5.5.3",
        "on_site_release",
        "On-site surface impoundments (all; replaced by 5.5.3A and 5.5.3B in 2003)",
        "Lagoas de contenção no local (todas; substituído por 5.5.3A e 5.5.3B em 2003)",
        "Lagunas de contención en el sitio (todas; reemplazado por 5.5.3A y 5.5.3B en 2003)",
        "1987(1)2002",
    ),
    (
        "5.5.3A - RCRA SURFACE IM",
        "5.5.3A",
        "on_site_release",
        "On-site RCRA Subtitle C surface impoundments",
        "Lagoas de contenção RCRA Subtítulo C no local",
        "Lagunas de contención RCRA Subtítulo C en el sitio",
        "2003(1)",
    ),
    (
        "5.5.3B - OTHER SURFACE I",
        "5.5.3B",
        "on_site_release",
        "Other on-site surface impoundments",
        "Outras lagoas de contenção no local",
        "Otras lagunas de contención en el sitio",
        "2003(1)",
    ),
    (
        "5.5.4 - OTHER DISPOSAL",
        "5.5.4",
        "on_site_release",
        "Other on-site land disposal",
        "Outras disposições no solo no local",
        "Otras disposiciones en suelo en el sitio",
        "",
    ),
    (
        "6.1 - POTW - TRNS RLSE",
        "6.1_RELEASE",
        "off_site_release",
        "Transfers to POTWs, share counted as released",
        "Transferências para POTW, parcela considerada liberada",
        "Transferencias a POTW, parte considerada liberada",
        "",
    ),
    (
        "6.1 - POTW - TRNS TRT",
        "6.1_TREATMENT",
        "off_site_treatment",
        "Transfers to POTWs, share counted as treated",
        "Transferências para POTW, parcela considerada tratada",
        "Transferencias a POTW, parte considerada tratada",
        "",
    ),
    (
        "6.2 - M10",
        "M10",
        "off_site_release",
        "M10 Storage only",
        "M10 Somente armazenamento",
        "M10 Solo almacenamiento",
        "",
    ),
    (
        "6.2 - M41",
        "M41",
        "off_site_release",
        "M41 Solidification/stabilization (metals and metal compounds only)",
        "M41 Solidificação/estabilização (apenas metais e compostos metálicos)",
        "M41 Solidificación/estabilización (solo metales y compuestos metálicos)",
        "",
    ),
    (
        "6.2 - M62",
        "M62",
        "off_site_release",
        "M62 Wastewater treatment excluding POTWs (metals and metal compounds only)",
        "M62 Tratamento de efluentes exceto POTW (apenas metais e compostos metálicos)",
        "M62 Tratamiento de aguas residuales excepto POTW (solo metales y compuestos metálicos)",
        "",
    ),
    (
        "6.2 - M40 METAL",
        "M40_METAL",
        "off_site_release",
        "M40 Solidification/stabilization of a metal (counted as disposal)",
        "M40 Solidificação/estabilização de metal (contada como disposição)",
        "M40 Solidificación/estabilización de un metal (contada como disposición)",
        "",
    ),
    (
        "6.2 - M61 METAL",
        "M61_METAL",
        "off_site_release",
        "M61 Wastewater treatment excluding POTWs of a metal (counted as disposal)",
        "M61 Tratamento de efluentes exceto POTW de metal (contado como disposição)",
        "M61 Tratamiento de aguas residuales excepto POTW de un metal (contado como disposición)",
        "",
    ),
    (
        "6.2 - M71",
        "M71",
        "off_site_release",
        "M71 Underground injection (replaced by M81 and M82 in 2003)",
        "M71 Injeção subterrânea (substituído por M81 e M82 em 2003)",
        "M71 Inyección subterránea (reemplazado por M81 y M82 en 2003)",
        "1987(1)2002",
    ),
    (
        "6.2 - M81",
        "M81",
        "off_site_release",
        "M81 Underground injection to Class I wells",
        "M81 Injeção subterrânea em poços Classe I",
        "M81 Inyección subterránea en pozos Clase I",
        "2003(1)",
    ),
    (
        "6.2 - M82",
        "M82",
        "off_site_release",
        "M82 Underground injection to Class II-V wells",
        "M82 Injeção subterrânea em poços Classe II-V",
        "M82 Inyección subterránea en pozos Clase II-V",
        "2003(1)",
    ),
    (
        "6.2 - M72",
        "M72",
        "off_site_release",
        "M72 Landfills/disposal surface impoundments (replaced by M63, M64 and M65 in 2002)",
        "M72 Aterros/lagoas de disposição (substituído por M63, M64 e M65 em 2002)",
        "M72 Rellenos/lagunas de disposición (reemplazado por M63, M64 y M65 en 2002)",
        "1987(1)2001",
    ),
    (
        "6.2 - M63",
        "M63",
        "off_site_release",
        "M63 Surface impoundment (replaced by M66 and M67 in 2003)",
        "M63 Lagoa de contenção (substituído por M66 e M67 em 2003)",
        "M63 Laguna de contención (reemplazado por M66 y M67 en 2003)",
        "2002(1)2002",
    ),
    (
        "6.2 - M66",
        "M66",
        "off_site_release",
        "M66 RCRA Subtitle C surface impoundments",
        "M66 Lagoas de contenção RCRA Subtítulo C",
        "M66 Lagunas de contención RCRA Subtítulo C",
        "2003(1)",
    ),
    (
        "6.2 - M67",
        "M67",
        "off_site_release",
        "M67 Other surface impoundments",
        "M67 Outras lagoas de contenção",
        "M67 Otras lagunas de contención",
        "2003(1)",
    ),
    (
        "6.2 - M64",
        "M64",
        "off_site_release",
        "M64 Other landfills",
        "M64 Outros aterros",
        "M64 Otros rellenos",
        "2002(1)",
    ),
    (
        "6.2 - M65",
        "M65",
        "off_site_release",
        "M65 RCRA Subtitle C landfills",
        "M65 Aterros RCRA Subtítulo C",
        "M65 Rellenos RCRA Subtítulo C",
        "2002(1)",
    ),
    (
        "6.2 - M73",
        "M73",
        "off_site_release",
        "M73 Land treatment",
        "M73 Tratamento no solo",
        "M73 Tratamiento en suelo",
        "",
    ),
    (
        "6.2 - M79",
        "M79",
        "off_site_release",
        "M79 Other land disposal",
        "M79 Outras disposições no solo",
        "M79 Otras disposiciones en suelo",
        "",
    ),
    (
        "6.2 - M90",
        "M90",
        "off_site_release",
        "M90 Other off-site management",
        "M90 Outra gestão fora do local",
        "M90 Otra gestión fuera del sitio",
        "",
    ),
    (
        "6.2 - M94",
        "M94",
        "off_site_release",
        "M94 Transfer to waste broker for disposal",
        "M94 Transferência a intermediário de resíduos para disposição",
        "M94 Transferencia a intermediario de residuos para disposición",
        "",
    ),
    (
        "6.2 - M99",
        "M99",
        "off_site_release",
        "M99 Unknown",
        "M99 Desconhecido",
        "M99 Desconocido",
        "",
    ),
    (
        "6.2 - M20",
        "M20",
        "off_site_recycling",
        "M20 Solvents/organics recovery",
        "M20 Recuperação de solventes/orgânicos",
        "M20 Recuperación de solventes/orgánicos",
        "",
    ),
    (
        "6.2 - M24",
        "M24",
        "off_site_recycling",
        "M24 Metals recovery",
        "M24 Recuperação de metais",
        "M24 Recuperación de metales",
        "",
    ),
    (
        "6.2 - M26",
        "M26",
        "off_site_recycling",
        "M26 Other reuse or recovery",
        "M26 Outro reuso ou recuperação",
        "M26 Otro reúso o recuperación",
        "",
    ),
    (
        "6.2 - M28",
        "M28",
        "off_site_recycling",
        "M28 Acid regeneration",
        "M28 Regeneração de ácido",
        "M28 Regeneración de ácido",
        "",
    ),
    (
        "6.2 - M93",
        "M93",
        "off_site_recycling",
        "M93 Transfer to waste broker for recycling",
        "M93 Transferência a intermediário de resíduos para reciclagem",
        "M93 Transferencia a intermediario de residuos para reciclaje",
        "",
    ),
    (
        "6.2 - M56",
        "M56",
        "off_site_energy_recovery",
        "M56 Energy recovery",
        "M56 Recuperação de energia",
        "M56 Recuperación de energía",
        "",
    ),
    (
        "6.2 - M92",
        "M92",
        "off_site_energy_recovery",
        "M92 Transfer to waste broker for energy recovery",
        "M92 Transferência a intermediário de resíduos para recuperação de energia",
        "M92 Transferencia a intermediario de residuos para recuperación de energía",
        "",
    ),
    (
        "6.2 - M40 NON-METAL",
        "M40_NON_METAL",
        "off_site_treatment",
        "M40 Solidification/stabilization of a non-metal (counted as treatment)",
        "M40 Solidificação/estabilização de não metal (contada como tratamento)",
        "M40 Solidificación/estabilización de un no metal (contada como tratamiento)",
        "",
    ),
    (
        "6.2 - M50",
        "M50",
        "off_site_treatment",
        "M50 Incineration/thermal treatment",
        "M50 Incineração/tratamento térmico",
        "M50 Incineración/tratamiento térmico",
        "",
    ),
    (
        "6.2 - M54",
        "M54",
        "off_site_treatment",
        "M54 Incineration/insignificant fuel value",
        "M54 Incineração/valor combustível insignificante",
        "M54 Incineración/valor combustible insignificante",
        "",
    ),
    (
        "6.2 - M61 NON-METAL",
        "M61_NON_METAL",
        "off_site_treatment",
        "M61 Wastewater treatment excluding POTWs of a non-metal (counted as treatment)",
        "M61 Tratamento de efluentes exceto POTW de não metal (contado como tratamento)",
        "M61 Tratamiento de aguas residuales excepto POTW de un no metal (contado como tratamiento)",
        "",
    ),
    (
        "6.2 - M69",
        "M69",
        "off_site_treatment",
        "M69 Other waste treatment",
        "M69 Outro tratamento de resíduos",
        "M69 Otro tratamiento de residuos",
        "",
    ),
    (
        "6.2 - M95",
        "M95",
        "off_site_treatment",
        "M95 Transfer to waste broker for waste treatment",
        "M95 Transferência a intermediário de resíduos para tratamento",
        "M95 Transferencia a intermediario de residuos para tratamiento",
        "",
    ),
    (
        "6.2 - UNCLASSIFIED",
        "6.2_UNCLASSIFIED",
        "off_site_unclassified",
        "Unclassified off-site transfers (code M91 transfers to waste broker and transfers without a specific code)",
        "Transferências fora do local não classificadas (código M91 transferência a intermediário e transferências sem código específico)",
        "Transferencias fuera del sitio no clasificadas (código M91 transferencia a intermediario y transferencias sin código específico)",
        "",
    ),
]

MANAGEMENT_CATEGORIES = [
    (
        "on_site_release",
        "On-site release to air, water and land (Form R section 5)",
        "Liberação no local para ar, água e solo (seção 5 do Form R)",
        "Liberación en el sitio al aire, agua y suelo (sección 5 del Form R)",
    ),
    (
        "off_site_release",
        "Off-site transfer for release or disposal, including the POTW share counted as released",
        "Transferência fora do local para liberação ou disposição, incluindo a parcela de POTW considerada liberada",
        "Transferencia fuera del sitio para liberación o disposición, incluida la parte de POTW considerada liberada",
    ),
    (
        "off_site_recycling",
        "Off-site transfer for recycling",
        "Transferência fora do local para reciclagem",
        "Transferencia fuera del sitio para reciclaje",
    ),
    (
        "off_site_energy_recovery",
        "Off-site transfer for energy recovery",
        "Transferência fora do local para recuperação de energia",
        "Transferencia fuera del sitio para recuperación de energía",
    ),
    (
        "off_site_treatment",
        "Off-site transfer for treatment, including the POTW share counted as treated",
        "Transferência fora do local para tratamento, incluindo a parcela de POTW considerada tratada",
        "Transferencia fuera del sitio para tratamiento, incluida la parte de POTW considerada tratada",
    ),
    (
        "off_site_unclassified",
        "Off-site transfer without a specific management code",
        "Transferência fora do local sem código de gestão específico",
        "Transferencia fuera del sitio sin código de gestión específico",
    ),
]

OTHER_CODES = {
    ("form", "form_type"): [
        (
            "R",
            "Form R, full report of releases and other waste management",
            "Form R, relatório completo de liberações e demais gestões de resíduo",
            "Form R, informe completo de liberaciones y demás gestión de residuos",
        ),
        (
            "A",
            "Form A certification statement, no quantities reported",
            "Form A, declaração de certificação, sem quantidades informadas",
            "Form A, declaración de certificación, sin cantidades informadas",
        ),
    ],
    ("chemical", "classification"): [
        (
            "TRI",
            "General EPCRA section 313 chemical",
            "Substância geral da seção 313 do EPCRA",
            "Sustancia general de la sección 313 del EPCRA",
        ),
        (
            "PBT",
            "Persistent, bioaccumulative and toxic chemical",
            "Substância persistente, bioacumulativa e tóxica",
            "Sustancia persistente, bioacumulativa y tóxica",
        ),
        (
            "Dioxin",
            "Dioxin or dioxin-like compound (reported in grams)",
            "Dioxina ou composto similar (informado em gramas)",
            "Dioxina o compuesto similar (informado en gramos)",
        ),
    ],
}

# Facility attributes, constant within (year, facility): source -> target.
FACILITY_MAP = [
    ("TRIFD", "tri_facility_id"),
    ("FRS ID", "frs_id"),
    ("FACILITY NAME", "facility_name"),
    ("STREET ADDRESS", "street_address"),
    ("CITY", "city"),
    ("COUNTY", "county_name"),
    ("ST", "state"),
    ("ZIP", "zip_code"),
    ("BIA", "bia_code"),
    ("TRIBE", "tribe_name"),
    ("LATITUDE", "latitude"),
    ("LONGITUDE", "longitude"),
    ("PARENT CO NAME", "parent_company_name"),
    ("PARENT CO DB NUM", "parent_company_duns"),
    ("STANDARD PARENT CO NAME", "standardized_parent_company_name"),
    ("FOREIGN PARENT CO NAME", "foreign_parent_company_name"),
    ("FOREIGN PARENT CO DB NUM", "foreign_parent_company_duns"),
    (
        "STANDARD FOREIGN PARENT CO NAME",
        "standardized_foreign_parent_company_name",
    ),
    ("FEDERAL FACILITY", "federal_facility"),
]

CHEMICAL_MAP = [
    ("TRI CHEMICAL/COMPOUND ID", "tri_chemical_id"),
    ("CHEMICAL", "chemical_name"),
    ("CAS#", "cas_number"),
    ("SRS ID", "srs_id"),
    ("CLEAN AIR ACT CHEMICAL", "clean_air_act_chemical"),
    ("CLASSIFICATION", "classification"),
    ("METAL", "metal"),
    ("METAL CATEGORY", "metal_category"),
    ("CARCINOGEN", "carcinogen"),
    ("PBT", "pbt"),
    ("PFAS", "pfas"),
]

# Form-level text/code columns: source -> target.
FORM_TEXT_MAP = [
    ("TRIFD", "tri_facility_id"),
    ("DOC_CTRL_NUM", "document_control_number"),
    ("TRI CHEMICAL/COMPOUND ID", "tri_chemical_id"),
    ("CHEMICAL", "chemical_name"),
    ("FORM TYPE", "form_type"),
    ("ELEMENTAL METAL INCLUDED", "elemental_metal_included"),
    ("UNIT OF MEASURE", "unit_of_measure"),
    ("INDUSTRY SECTOR CODE", "industry_sector_code"),
    ("INDUSTRY SECTOR", "industry_sector"),
    ("PRIMARY SIC", "primary_sic"),
    ("SIC 2", "sic_2"),
    ("SIC 3", "sic_3"),
    ("SIC 4", "sic_4"),
    ("SIC 5", "sic_5"),
    ("SIC 6", "sic_6"),
    ("PRIMARY NAICS", "primary_naics"),
    ("NAICS 2", "naics_2"),
    ("NAICS 3", "naics_3"),
    ("NAICS 4", "naics_4"),
    ("NAICS 5", "naics_5"),
    ("NAICS 6", "naics_6"),
    ("PROD_RATIO_OR_ ACTIVITY", "production_ratio_type"),
]
# Form-level quantities (unit-converted to pounds): source -> target.
FORM_QTY_MAP = [
    ("ON-SITE RELEASE TOTAL", "on_site_release_total"),
    ("POTW - TOTAL TRANSFERS", "potw_transfer_total"),
    ("OFF-SITE RELEASE TOTAL", "off_site_release_total"),
    ("OFF-SITE RECYCLED TOTAL", "off_site_recycling_total"),
    ("OFF-SITE ENERGY RECOVERY T", "off_site_energy_recovery_total"),
    ("OFF-SITE TREATED TOTAL", "off_site_treatment_total"),
    ("6.2 - TOTAL TRANSFER", "total_transfer"),
    ("TOTAL RELEASES", "total_releases"),
    ("8.1 - RELEASES", "waste_released"),
    ("8.1A - ON-SITE CONTAINED", "waste_released_on_site_contained"),
    ("8.1B - ON-SITE OTHER", "waste_released_on_site_other"),
    ("8.1C - OFF-SITE CONTAIN", "waste_released_off_site_contained"),
    ("8.1D - OFF-SITE OTHER R", "waste_released_off_site_other"),
    ("8.2 - ENERGY RECOVER ON", "waste_energy_recovery_on_site"),
    ("8.3 - ENERGY RECOVER OF", "waste_energy_recovery_off_site"),
    ("8.4 - RECYCLING ON SITE", "waste_recycled_on_site"),
    ("8.5 - RECYCLING OFF SIT", "waste_recycled_off_site"),
    ("8.6 - TREATMENT ON SITE", "waste_treated_on_site"),
    ("8.7 - TREATMENT OFF SITE", "waste_treated_off_site"),
    ("PRODUCTION WSTE (8.1-8.7)", "production_related_waste"),
    ("8.8 - ONE-TIME RELEASE", "one_time_release"),
]


# ── schema ──────────────────────────────────────────────────────────────────
def read_arch(table: str) -> list[dict]:
    """Read a table's architecture CSV, the schema source of truth."""
    with open(_ARCH / f"{table}.csv", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def arch_columns(table: str, drop_year: bool = False) -> list[str]:
    cols = [r["name"] for r in read_arch(table)]
    return [c for c in cols if not (drop_year and c == "year")]


def naics_version(year: int) -> str:
    for lo, hi, v in constants.NAICS_VERSION.value:
        if lo <= year <= hi:
            return v
    raise ValueError(year)


# ── source discovery (poll signal) ──────────────────────────────────────────
def fetch_page() -> str:
    r = requests.get(constants.PAGE_URL.value, headers=_HEADERS, timeout=120)
    r.raise_for_status()
    return r.text


def source_years(html: str | None = None) -> list[int]:
    """Reporting years offered by the Basic Data Files page's year dropdown."""
    html = html or fetch_page()
    years = sorted(
        {int(y) for y in re.findall(r'option value="(\d{4})"', html)}
    )
    if not years or years[0] != constants.FIRST_YEAR.value:
        raise RuntimeError(
            f"unexpected year list on the TRI page: {years[:3]}..."
        )
    return years


def source_processed_date(html: str | None = None) -> date:
    """Date the files were last regenerated ("processed as of: Month D, YYYY")."""
    html = html or fetch_page()
    m = re.search(
        r"processed as of:\s*(?:<[^>]+>\s*)*([A-Z][a-z]+ \d{1,2}, \d{4})", html
    )
    if not m:
        raise RuntimeError(
            "could not find the 'processed as of' date on the TRI page"
        )
    return datetime.strptime(m.group(1), "%B %d, %Y").date()


# ── download ────────────────────────────────────────────────────────────────
def _last_row_fields(path: Path, tail_bytes: int = 20_000) -> int:
    with open(path, "rb") as fh:
        fh.seek(max(0, path.stat().st_size - tail_bytes))
        tail = fh.read().decode("utf-8", errors="replace")
    rows = list(csv.reader(tail.splitlines()))
    return len(rows[-1]) if rows else 0


def download_year(year: int, input_dir: Path, attempts: int = 6) -> Path:
    """Fetch one national Basic Data File to ``<input_dir>/<year>_US.csv``.

    Envirofacts generates the CSV on the fly and streams it slowly; a
    connection that drops mid-stream still returns HTTP 200, so the file is
    accepted only when its last row has all 122 fields.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    dest = input_dir / f"{year}_US.csv"
    if dest.exists():
        return dest
    part = input_dir / f"{year}_US.csv.part"
    url = constants.DOWNLOAD_URL.value.format(year=year)
    for attempt in range(1, attempts + 1):
        try:
            with requests.get(
                url, headers=_HEADERS, stream=True, timeout=(60, 600)
            ) as r:
                r.raise_for_status()
                with open(part, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        fh.write(chunk)
            head = part.open("rb").read(20).decode("utf-8", errors="replace")
            nf = _last_row_fields(part)
            if head.startswith("1. YEAR") and nf == constants.N_COLUMNS.value:
                part.rename(dest)
                log.info(f"{year}: {dest.stat().st_size:,} bytes")
                return dest
            log.warning(
                f"{year}: attempt {attempt} incomplete (last row {nf} fields)"
            )
        except requests.RequestException as e:
            log.warning(f"{year}: attempt {attempt} failed: {e}")
        time.sleep(60)
    raise RuntimeError(f"{year}: download failed after {attempts} attempts")


def download_facility_fips(ref_dir: Path) -> Path:
    """Pull TRI_FACILITY from Envirofacts: TRI facility ID -> county FIPS code.

    Writes ``<ref_dir>/tri_facility_fips.csv`` with tri_facility_id,
    county_id, county_name, state.
    """
    ref_dir.mkdir(parents=True, exist_ok=True)
    dest = ref_dir / "tri_facility_fips.csv"
    page = constants.FACILITY_PAGE.value
    rows: list[dict] = []
    start = 0
    while True:
        url = constants.FACILITY_URL.value.format(
            start=start, end=start + page - 1
        )
        r = requests.get(url, headers=_HEADERS, timeout=600)
        r.raise_for_status()
        batch = r.json()
        rows.extend(batch)
        if len(batch) < page:
            break
        start += page
    with open(dest, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["tri_facility_id", "county_id", "county_name", "state"])
        for x in rows:
            w.writerow(
                [
                    x["tri_facility_id"],
                    x.get("state_county_fips_code") or "",
                    x.get("county_name") or "",
                    x.get("state_abbr") or "",
                ]
            )
    log.info(f"tri_facility: {len(rows):,} facilities -> {dest}")
    return dest


def load_facility_fips(path: Path) -> dict[str, str]:
    with open(path, newline="", encoding="utf-8") as fh:
        return {
            r["tri_facility_id"]: r["county_id"]
            for r in csv.DictReader(fh)
            if r["county_id"]
        }


def _row(con: duckdb.DuckDBPyConnection, sql: str) -> tuple:
    """First row of a query, never None (every caller expects one row)."""
    row = con.execute(sql).fetchone()
    if row is None:
        raise RuntimeError(f"query returned no rows: {sql[:80]}")
    return row


# ── transform ───────────────────────────────────────────────────────────────
def _strip_number(col: str) -> str:
    """'51. 5.1 - FUGITIVE AIR' -> '5.1 - FUGITIVE AIR'."""
    return re.sub(r"^\d+\.\s", "", col)


def _ident(name: str) -> str:
    """Quote a SQL identifier, escaping any embedded double quote.

    Column names here come from the header of a downloaded CSV, so they are
    external input: a header carrying a double quote would otherwise close
    the identifier and let the rest of the header run as SQL.
    """
    return '"' + name.replace('"', '""') + '"'


def _to_string_table(tbl: pa.Table, columns: list[str]) -> pa.Table:
    """Reorder to the architecture and cast every column to STRING via arrow.

    Arrow's cast keeps NULL as NULL (``astype(str)`` would write 'nan') and
    serializes integers without a decimal point.
    """
    tbl = tbl.select(columns)
    return tbl.cast(pa.schema([(c, pa.string()) for c in columns]))


def _write(tbl: pa.Table, dest: Path, write_header: bool) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(tbl, dest, compression="snappy")
    if write_header:
        # 0-row all-STRING header that sorts first: the table-approve CI step
        # builds the staging schema from the first parquet it finds and loads
        # it whole, so keep that file tiny. Bootstrap only — the recurring
        # pipeline's dump_header would read 0 rows and infer INT64 everywhere.
        pq.write_table(tbl.slice(0, 0), dest.parent / "00_header.parquet")


def clean_year(
    year: int,
    csv_path: Path,
    facility_fips: dict[str, str],
    output_dir: Path,
    write_header: bool = False,
) -> dict:
    """Transform one national CSV into the facility/form/release partitions.

    Returns counts plus the year's chemical attribute rows (aggregated across
    years by ``build_chemical``).
    """
    con = duckdb.connect()
    con.execute(
        "create table raw as select * from read_csv(?, header=true, all_varchar=true, "
        "sample_size=-1, quote='\"', escape='\"', strict_mode=true)",
        [str(csv_path)],
    )
    cols = [r[0] for r in con.execute("describe raw").fetchall()]
    if len(cols) != constants.N_COLUMNS.value:
        raise RuntimeError(
            f"{csv_path.name}: {len(cols)} columns, expected {constants.N_COLUMNS.value}"
        )
    src = {_strip_number(c): c for c in cols}

    def q(name: str) -> str:
        return _ident(src[name])

    n_rows = _row(con, "select count(*) from raw")[0]
    yrs = con.execute(f"select distinct {q('YEAR')} from raw").fetchall()
    if yrs != [(str(year),)]:
        raise RuntimeError(
            f"{csv_path.name}: YEAR values {yrs}, expected {year}"
        )
    n_doc = _row(con, f"select count(distinct {q('DOC_CTRL_NUM')}) from raw")[
        0
    ]
    if n_doc != n_rows:
        raise RuntimeError(
            f"{csv_path.name}: {n_rows} rows but {n_doc} distinct DOC_CTRL_NUM"
        )

    # nullify empty strings once, so every downstream select is simple
    for cname in cols:
        col = _ident(cname)
        con.execute(f"update raw set {col} = null where trim({col}) = ''")

    # ── facility (year x TRIFID): first form by document control number ──
    fac_sel = ", ".join(f"{q(s)} as {t}" for s, t in FACILITY_MAP)
    fac_attr = [t for _, t in FACILITY_MAP if t != "tri_facility_id"]
    conflicts = _row(
        con,
        "select "
        + ", ".join(
            f"sum(case when n_{t} > 1 then 1 else 0 end)" for t in fac_attr
        )
        + " from (select "
        + ", ".join(
            f"count(distinct {q(s)}) as n_{t}"
            for s, t in FACILITY_MAP
            if t != "tri_facility_id"
        )
        + f" from raw group by {q('TRIFD')})",
    )
    conflict_cols = {
        t: n for t, n in zip(fac_attr, conflicts, strict=True) if n
    }
    if conflict_cols:
        log.warning(
            f"{year}: facility attributes vary within a facility: {conflict_cols}"
        )
    unknown = constants.UNKNOWN_COUNTY.value
    fips_tbl = pa.table(
        {
            "tri_facility_id": list(facility_fips.keys()),
            "county_id": [
                None if v == unknown else v for v in facility_fips.values()
            ],
        }
    )
    con.register("fips", fips_tbl)
    facility = con.execute(
        f"""
        with ranked as (
            select {fac_sel}, {q("DOC_CTRL_NUM")} as doc,
                   row_number() over (partition by {q("TRIFD")} order by {q("DOC_CTRL_NUM")}) as rn
            from raw
        )
        select r.* exclude (doc, rn), f.county_id,
               try_cast(r.latitude as double) as latitude_f,
               try_cast(r.longitude as double) as longitude_f
        from ranked r left join fips f using (tri_facility_id)
        where rn = 1
        order by tri_facility_id
        """
    ).fetch_arrow_table()
    facility = facility.drop_columns(["latitude", "longitude"])
    facility = facility.rename_columns(
        [
            {"latitude_f": "latitude", "longitude_f": "longitude"}.get(c, c)
            for c in facility.column_names
        ]
    )
    n_fac = facility.num_rows
    n_no_fips = (
        facility.num_rows - facility.column("county_id").drop_null().length()
    )
    if n_no_fips:
        log.warning(
            f"{year}: {n_no_fips}/{n_fac} facilities without a county FIPS code"
        )
    _write(
        _to_string_table(facility, arch_columns("facility", drop_year=True)),
        output_dir / "facility" / f"year={year}" / "data.parquet",
        write_header,
    )

    # ── form (year x document control number) ──
    grams = f"({q('UNIT OF MEASURE')} = 'Grams')"

    def lb(s: str) -> str:
        return (
            f"case when {grams} then try_cast({q(s)} as double) / {_G_PER_LB} "
            f"else try_cast({q(s)} as double) end"
        )

    sentinels = ", ".join(f"'{v}'" for v in constants.SIC_SENTINELS.value)

    def txt(s: str, t: str) -> str:
        if t == "primary_sic" or t.startswith("sic_"):
            return (
                f"case when {q(s)} in ({sentinels}) then null else {q(s)} end"
                f" as {t}"
            )
        return f"{q(s)} as {t}"

    text_sel = ", ".join(txt(s, t) for s, t in FORM_TEXT_MAP)
    qty_sel = ", ".join(f"{lb(s)} as {t}" for s, t in FORM_QTY_MAP)
    form = con.execute(
        f"""
        select {text_sel}, '{naics_version(year)}' as naics_version, {qty_sel},
               try_cast({q("8.9 - PRODUCTION RATIO")} as double) as production_ratio
        from raw order by document_control_number
        """
    ).fetch_arrow_table()
    _write(
        _to_string_table(form, arch_columns("form", drop_year=True)),
        output_dir / "form" / f"year={year}" / "data.parquet",
        write_header,
    )

    # ── release (long: form x category), zero rows dropped ──
    parts = []
    for s, code, mgmt, *_ in RELEASE_CATEGORIES:
        parts.append(
            f"select {q('TRIFD')} as tri_facility_id, {q('DOC_CTRL_NUM')} as document_control_number, "
            f"{q('TRI CHEMICAL/COMPOUND ID')} as tri_chemical_id, '{mgmt}' as management_category, "
            f"'{code}' as release_category, try_cast({q(s)} as double) as qty, {grams} as is_grams "
            f"from raw where try_cast({q(s)} as double) <> 0"
        )
    release = con.execute(
        "select tri_facility_id, document_control_number, tri_chemical_id, management_category, "
        f"release_category, case when is_grams then qty / {_G_PER_LB} else qty end as quantity_pounds, "
        "case when is_grams then qty else null end as quantity_grams "
        f"from ({' union all '.join(parts)}) order by document_control_number, release_category"
    ).fetch_arrow_table()
    _write(
        _to_string_table(release, arch_columns("release", drop_year=True)),
        output_dir / "release" / f"year={year}" / "data.parquet",
        write_header,
    )

    # ── chemical attributes of this year (aggregated later) ──
    chem_sel = ", ".join(f"{q(s)} as {t}" for s, t in CHEMICAL_MAP)
    chem = con.execute(
        f"select {chem_sel}, count(*) as n_forms from raw group by all"
    ).fetch_arrow_table()
    con.close()
    return {
        "year": year,
        "counts": {
            "facility": n_fac,
            "form": form.num_rows,
            "release": release.num_rows,
        },
        "facilities_without_fips": n_no_fips,
        "facility_conflicts": conflict_cols,
        "chemical": chem,
    }


def build_chemical(
    chem_years: list[tuple[int, pa.Table]],
    output_dir: Path,
    write_header: bool = False,
) -> int:
    """Write the chemical dimension, one row per reporting year and chemical.

    Partitioned by year like every other data table, so a run that refreshes
    only the newest reporting years rewrites just those partitions. A
    non-partitioned dimension would be replaced wholesale by such a run and
    would lose every chemical last reported in an earlier year.

    Within a year a chemical's attributes are constant in practice; the most
    frequent variant wins if they are not.
    """
    total = 0
    con = duckdb.connect()
    for year, t in chem_years:
        con.register("chem_year", t)
        chem = con.execute(
            """
            select * exclude (n_forms, rn) from (
                select *, row_number() over (
                    partition by tri_chemical_id
                    order by n_forms desc, chemical_name
                ) as rn
                from chem_year
            ) where rn = 1 order by tri_chemical_id
            """
        ).fetch_arrow_table()
        con.unregister("chem_year")
        _write(
            _to_string_table(chem, arch_columns("chemical", drop_year=True)),
            output_dir / "chemical" / f"year={year}" / "data.parquet",
            write_header,
        )
        total += chem.num_rows
    con.close()
    return total


def dicionario_rows() -> list[dict]:
    """Coded values of every dictionary-covered column, from the spec above."""
    rows = []
    for _, code, _, en, pt, es, cov in RELEASE_CATEGORIES:
        rows.append(
            {
                "id_tabela": "release",
                "nome_coluna": "release_category",
                "chave": code,
                "cobertura_temporal": cov,
                "valor": pt,
                "valor_en": en,
                "valor_es": es,
            }
        )
    for code, en, pt, es in MANAGEMENT_CATEGORIES:
        rows.append(
            {
                "id_tabela": "release",
                "nome_coluna": "management_category",
                "chave": code,
                "cobertura_temporal": "",
                "valor": pt,
                "valor_en": en,
                "valor_es": es,
            }
        )
    for (table, col), items in OTHER_CODES.items():
        for code, en, pt, es in items:
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": col,
                    "chave": code,
                    "cobertura_temporal": "",
                    "valor": pt,
                    "valor_en": en,
                    "valor_es": es,
                }
            )
    return rows


def build_dicionario(output_dir: Path, write_header: bool = False) -> int:
    rows = dicionario_rows()
    cols = arch_columns("dicionario")
    tbl = pa.table({c: [r[c] for r in rows] for c in cols})
    _write(
        _to_string_table(tbl, cols),
        output_dir / "dicionario" / "data.parquet",
        write_header,
    )
    return tbl.num_rows


def assert_output_layout(output_dir: Path) -> None:
    """Fail if any table directory holds a parquet the upload should not send.

    The upload ships a whole table directory, so a file left behind by an
    earlier run with a different layout is silently merged into the staging
    table. That is how a stale unpartitioned ``chemical/data.parquet`` once
    added 710 phantom rows and broke the dbt model's schema. Partitioned
    tables must hold parquet only under ``year=YYYY/``; the rest only at the
    top level.
    """
    partitioned = set(constants.YEAR_TABLES.value)
    for table in constants.TABLES.value:
        table_dir = output_dir / table
        if not table_dir.exists():
            continue
        for path in sorted(table_dir.rglob("*.parquet")):
            rel = path.relative_to(table_dir)
            in_partition = len(rel.parts) == 2 and rel.parts[0].startswith(
                "year="
            )
            at_top = len(rel.parts) == 1
            ok = in_partition if table in partitioned else at_top
            if not ok:
                raise RuntimeError(
                    f"unexpected parquet in the {table} output: {path}. "
                    "Delete stale files from an earlier layout before "
                    "uploading — the upload ships the whole directory"
                )


def clean_all(
    input_dir: Path,
    output_dir: Path,
    facility_fips_path: Path,
    years: list[int] | None = None,
    write_header: bool = False,
) -> dict:
    """Clean every ``<year>_US.csv`` in ``input_dir`` (or just ``years``).

    Returns per-table row counts, the max reporting year, and per-year notes.
    """
    fips = load_facility_fips(facility_fips_path)
    files = sorted(input_dir.glob("*_US.csv"))
    if years is not None:
        files = [f for f in files if int(f.name[:4]) in years]
    if not files:
        raise FileNotFoundError(f"no *_US.csv under {input_dir}")
    counts = Counter()
    chem_years = []
    notes = {}
    for f in files:
        year = int(f.name[:4])
        t0 = time.time()
        r = clean_year(year, f, fips, output_dir, write_header)
        counts.update(r["counts"])
        chem_years.append((year, r["chemical"]))
        notes[year] = {
            k: r[k]
            for k in (
                "counts",
                "facilities_without_fips",
                "facility_conflicts",
            )
        }
        log.info(f"{year}: {r['counts']} in {time.time() - t0:.0f}s")
    counts["chemical"] = build_chemical(chem_years, output_dir, write_header)
    counts["dicionario"] = build_dicionario(output_dir, write_header)
    assert_output_layout(output_dir)
    return {
        "counts": dict(counts),
        "max_year": max(int(f.name[:4]) for f in files),
        "notes": notes,
        **{t: output_dir / t for t in constants.TABLES.value},
    }
